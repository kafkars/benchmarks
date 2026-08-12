/* The v2 measured path: four timestamps per offer, on a bounded slab. */
#include "benchmark.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
 * # What this file is for
 *
 * Every record is one immutable offer carrying four timestamps: `intended`
 * (its place in the schedule, or its own call start under closed loop),
 * `call_start` (when the application began the admission attempt),
 * `accepted` (when the client took ownership), and `terminal` (its delivery
 * report). `call_start` is taken once, *before* the application blocks for
 * admission, and no queue-full retry ever rewrites it — a retry that restarts
 * the clock hides exactly the backpressure the measurement exists to show.
 *
 * # Why the memory is bounded
 *
 * Offers live in a slab sized to the outstanding-record ceiling and keyed by
 * the `msg_opaque` librdkafka hands back on the delivery report, so an offer
 * that terminates returns its entry to a free list for the next one. Nothing
 * here grows with run length: the four distributions are fixed-size
 * histograms, and the record buffers are a pool built before the measured
 * interval starts.
 *
 * # Who touches what
 *
 * Submitter threads never poll, so delivery reports are only ever dispatched
 * from the one thread that does — which makes the mutex here the single point
 * of contention rather than a lock that a callback could re-enter. The lock is
 * never held across a backoff sleep: the client frees queue capacity by
 * delivering, delivery runs through the callback, and the callback needs this
 * lock.
 */

/* Sentinel for a monotonic clock that refused to answer. */
#define V2_CLOCK_FAILED UINT64_MAX

/* Lead-in before the fixed-rate schedule starts, matching the legacy path. */
#define V2_SCHEDULE_LEAD_NS 100000000U

typedef struct v2_caller_s {
        rd_kafka_t *producer;
        rd_kafka_topic_t *topic;
        const bench_config_t *config;
        bench_v2_phase_t *phase;
        size_t caller;
        size_t budget;
        char *payload;
        unsigned char *keys;
        rd_kafka_message_t *messages;
        uint64_t *intended;
} v2_caller_t;

static void v2_lock(bench_v2_phase_t *phase) {
        (void)pthread_mutex_lock(&phase->lock);
}

static void v2_unlock(bench_v2_phase_t *phase) {
        (void)pthread_mutex_unlock(&phase->lock);
}

static void v2_broadcast(bench_v2_phase_t *phase) {
        (void)pthread_cond_broadcast(&phase->changed);
}

static void v2_wait(bench_v2_phase_t *phase) {
        if (pthread_cond_wait(&phase->changed, &phase->lock) != 0)
                phase->fatal = 1;
}

static void fail_locked(bench_v2_phase_t *phase) {
        phase->fatal = 1;
        v2_broadcast(phase);
}

static void v2_fail(bench_v2_phase_t *phase) {
        v2_lock(phase);
        fail_locked(phase);
        v2_unlock(phase);
}

static int v2_failed(bench_v2_phase_t *phase) {
        int fatal;

        v2_lock(phase);
        fatal = phase->fatal;
        v2_unlock(phase);
        return fatal;
}

static void submitter_done(bench_v2_phase_t *phase) {
        v2_lock(phase);
        if (phase->active_submitters > 0)
                phase->active_submitters--;
        v2_broadcast(phase);
        v2_unlock(phase);
}

static int submitters_done(bench_v2_phase_t *phase) {
        int done;

        v2_lock(phase);
        done = phase->active_submitters == 0;
        v2_unlock(phase);
        return done;
}

/* Nanoseconds since the measured phase began. */
static uint64_t elapsed_ns(const bench_v2_phase_t *phase) {
        uint64_t now = bench_now_ns();

        if (now == 0)
                return V2_CLOCK_FAILED;
        return now > phase->started_ns ? now - phase->started_ns : 0;
}

/* A duration that can never run backwards past zero. */
static uint64_t span_ns(uint64_t start, uint64_t end) {
        return end > start ? end - start : 0;
}

/*
 * Takes one slab entry. The caller holds the lock and has already waited until
 * its outstanding budget admits the whole offer, which is what guarantees an
 * entry is free.
 */
static bench_offer_t *take_slot(bench_v2_phase_t *phase) {
        bench_offer_t *offer;

        if (phase->free_count == 0)
                return NULL;
        offer            = &phase->slab[phase->free_slots[--phase->free_count]];
        offer->published = 0;
        offer->active    = 1;
        return offer;
}

/* Returns one slab entry to the free list; the caller holds the lock. */
static void release_slot(bench_v2_phase_t *phase, bench_offer_t *offer) {
        offer->active    = 0;
        offer->published = 0;
        phase->free_slots[phase->free_count++] =
            (size_t)(offer - phase->slab);
}

/*
 * Drops the outstanding reservation of every offer in the window the client
 * never took, and fails the phase.
 *
 * "Never took" is exactly "not published": acceptance is published for each
 * record the moment its produce call returns, so an unpublished offer is one
 * the client refused. Releasing an accepted offer here would decide the run
 * had one fewer record in flight than the client does, and the delivery report
 * still on its way would then belong to nobody — which is how a failed run
 * ends up with a result document that cannot be parsed rather than one that
 * says what went wrong.
 */
static void abandon(v2_caller_t *caller, size_t window) {
        bench_v2_phase_t *phase = caller->phase;
        size_t index;

        v2_lock(phase);
        for (index = 0; index < window; ++index) {
                bench_offer_t *offer = caller->messages[index]._private;

                if (!offer || !offer->active || offer->published)
                        continue;
                release_slot(phase, offer);
                if (phase->outstanding > 0)
                        phase->outstanding--;
                if (phase->caller_outstanding[caller->caller] > 0)
                        phase->caller_outstanding[caller->caller]--;
        }
        fail_locked(phase);
        v2_unlock(phase);
}

void bench_v2_delivery(bench_v2_phase_t *phase,
                       const rd_kafka_message_t *message) {
        bench_offer_t *offer = message->_private;
        uint64_t terminal_ns = elapsed_ns(phase);

        if (!offer)
                return;
        v2_lock(phase);
        if (phase->closed || !offer->active) {
                v2_unlock(phase);
                return;
        }
        /* An offer is published before the lock is next released, so this only
           waits if a client delivered faster than the submitter could stamp
           acceptance. */
        while (!offer->published && !phase->fatal)
                v2_wait(phase);
        if (terminal_ns == V2_CLOCK_FAILED) {
                fail_locked(phase);
                terminal_ns = offer->accepted_ns;
        }
        if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                phase->acknowledged++;
        } else if (message->err == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT) {
                phase->timed_out++;
        } else {
                phase->failed++;
                fprintf(stderr, "delivery failed: %s\n",
                        rd_kafka_message_errstr(message));
        }
        bench_histogram_record(&phase->accepted_to_terminal,
                               span_ns(offer->accepted_ns, terminal_ns));
        bench_histogram_record(&phase->intended_to_terminal,
                               span_ns(offer->intended_ns, terminal_ns));
        if (terminal_ns > phase->last_terminal_ns)
                phase->last_terminal_ns = terminal_ns;
        if (phase->outstanding > 0)
                phase->outstanding--;
        if (phase->caller_outstanding[offer->caller] > 0)
                phase->caller_outstanding[offer->caller]--;
        release_slot(phase, offer);
        v2_broadcast(phase);
        v2_unlock(phase);
}

/* Fills the record buffers for one batch, before any of it is measured. */
static void prepare_batch(v2_caller_t *caller, size_t sequence, size_t count) {
        const bench_config_t *config = caller->config;
        size_t index;

        for (index = 0; index < count; ++index) {
                size_t current = sequence + index;
                char *record   = caller->payload + (index * config->payload_bytes);
                unsigned char *key = caller->keys + (index * 8U);

                bench_payload(record, config->payload_bytes, config->run_id,
                              (uint64_t)current);
                bench_encode_key((uint64_t)current, key);
                caller->messages[index].payload   = record;
                caller->messages[index].len       = config->payload_bytes;
                caller->messages[index].key       = key;
                caller->messages[index].key_len   = 8U;
                caller->messages[index].partition =
                    (int32_t)(current % (size_t)config->partitions);
                caller->messages[index]._private = NULL;
                caller->messages[index].err      = RD_KAFKA_RESP_ERR_NO_ERROR;
        }
}

/* Publishes acceptance for every record the last produce call enqueued. */
static void publish_accepted(v2_caller_t *caller,
                             size_t pending,
                             uint64_t accepted_ns) {
        bench_v2_phase_t *phase = caller->phase;
        size_t index;

        v2_lock(phase);
        for (index = 0; index < pending; ++index) {
                bench_offer_t *offer = caller->messages[index]._private;

                if (caller->messages[index].err != RD_KAFKA_RESP_ERR_NO_ERROR)
                        continue;
                offer->accepted_ns = accepted_ns;
                offer->published   = 1;
                phase->accepted++;
                bench_histogram_record(&phase->call_start_to_accepted,
                                       span_ns(offer->call_start_ns,
                                               accepted_ns));
        }
        v2_broadcast(phase);
        v2_unlock(phase);
}

/*
 * Moves the records the client refused to the front of the batch so the next
 * attempt carries only them, and refuses anything that is not backpressure.
 *
 * The check runs to completion before anything moves, so a batch that cannot
 * be retried is handed back exactly as the client left it.
 */
static int compact_queue_full(v2_caller_t *caller,
                              size_t pending,
                              size_t *remaining) {
        size_t index;

        for (index = 0; index < pending; ++index) {
                if (caller->messages[index].err ==
                        RD_KAFKA_RESP_ERR_NO_ERROR ||
                    caller->messages[index].err ==
                        RD_KAFKA_RESP_ERR__QUEUE_FULL)
                        continue;
                fprintf(stderr, "admission refused a record: %s\n",
                        rd_kafka_err2str(caller->messages[index].err));
                return -1;
        }
        *remaining = 0;
        for (index = 0; index < pending; ++index) {
                if (caller->messages[index].err == RD_KAFKA_RESP_ERR_NO_ERROR)
                        continue;
                caller->messages[*remaining] = caller->messages[index];
                caller->messages[*remaining].err = RD_KAFKA_RESP_ERR_NO_ERROR;
                (*remaining)++;
        }
        return 0;
}

static void backoff(void) {
        struct timespec wait;

        wait.tv_sec  = 0;
        wait.tv_nsec = (long)BENCH_V2_QUEUE_FULL_BACKOFF_NS;
        while (nanosleep(&wait, &wait) != 0 && errno == EINTR) {
        }
}

/*
 * The measured admission of one batch of offers. Everything from here to the
 * delivery report is inside `call_start_to_accepted`, including the wait for
 * outstanding budget and every queue-full retry — and `call_start_ns` arrives
 * already taken, so no path in here can restart it.
 */
static int offer_batch(v2_caller_t *caller,
                       size_t batch_index,
                       size_t count,
                       uint64_t call_start_ns,
                       const uint64_t *intended_ns) {
        bench_v2_phase_t *phase = caller->phase;
        uint64_t deadline_ns =
            call_start_ns +
            ((uint64_t)BENCH_DELIVERY_TIMEOUT_MS * UINT64_C(1000000));
        size_t pending = count;
        size_t index;

        v2_lock(phase);
        for (index = 0; index < count; ++index) {
                phase->offered++;
                if (intended_ns)
                        bench_histogram_record(
                            &phase->intended_to_call_start,
                            span_ns(intended_ns[index], call_start_ns));
        }
        while (!phase->fatal &&
               phase->caller_outstanding[caller->caller] + count >
                   caller->budget)
                v2_wait(phase);
        /* Batches enter the client in schedule order, as the legacy path
           admits them, so a slow caller cannot reorder the offered stream. */
        while (!phase->fatal && phase->submission_count < batch_index)
                v2_wait(phase);
        if (phase->fatal || phase->submission_count != batch_index ||
            phase->free_count < count) {
                fail_locked(phase);
                v2_unlock(phase);
                return -1;
        }
        for (index = 0; index < count; ++index) {
                bench_offer_t *offer = take_slot(phase);

                offer->intended_ns =
                    intended_ns ? intended_ns[index] : call_start_ns;
                offer->call_start_ns = call_start_ns;
                offer->accepted_ns   = 0;
                offer->caller        = caller->caller;
                caller->messages[index]._private = offer;
        }
        phase->outstanding += count;
        phase->caller_outstanding[caller->caller] += count;
        if (phase->outstanding > phase->max_outstanding_observed)
                phase->max_outstanding_observed = phase->outstanding;
        v2_unlock(phase);
        for (;;) {
                uint64_t accepted_ns;
                size_t remaining;

                (void)rd_kafka_produce_batch(
                    caller->topic, RD_KAFKA_PARTITION_UA,
                    RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_PARTITION,
                    caller->messages, (int)pending);
                accepted_ns = elapsed_ns(phase);
                if (accepted_ns == V2_CLOCK_FAILED) {
                        /* Acceptance is still published, at the only timestamp
                           left: an offer the client took has to be accounted
                           for even when the run is already lost. */
                        fprintf(stderr, "monotonic clock failed\n");
                        accepted_ns = call_start_ns;
                        v2_fail(phase);
                }
                publish_accepted(caller, pending, accepted_ns);
                if (compact_queue_full(caller, pending, &remaining) != 0) {
                        abandon(caller, pending);
                        return -1;
                }
                pending = remaining;
                if (pending == 0)
                        break;
                if (accepted_ns >= deadline_ns || v2_failed(phase)) {
                        fprintf(stderr,
                                "admission stayed queue-full past the delivery "
                                "bound\n");
                        abandon(caller, pending);
                        return -1;
                }
                backoff();
        }
        v2_lock(phase);
        phase->submission_count++;
        v2_broadcast(phase);
        v2_unlock(phase);
        return 0;
}

/* Sleeps until the schedule says this batch may be offered. */
static int await_due(bench_v2_phase_t *phase, uint64_t due_ns) {
        for (;;) {
                uint64_t now = elapsed_ns(phase);
                uint64_t wait_ns;
                struct timespec wait;

                if (now == V2_CLOCK_FAILED || v2_failed(phase))
                        return -1;
                if (now >= due_ns)
                        return 0;
                wait_ns = due_ns - now;
                if (wait_ns > 1000000U)
                        wait_ns = 1000000U;
                wait.tv_sec  = 0;
                wait.tv_nsec = (long)wait_ns;
                while (nanosleep(&wait, &wait) != 0 && errno == EINTR) {
                }
        }
}

/* The closed-loop submitter: offer as fast as the client will take records. */
static int run_closed_caller(v2_caller_t *caller) {
        const bench_config_t *config = caller->config;
        size_t batch_index           = 0;
        size_t sequence;

        for (sequence = 0; sequence < config->records;) {
                size_t count = config->records - sequence;
                uint64_t call_start_ns;

                if (count > BENCH_BATCH_RECORDS)
                        count = BENCH_BATCH_RECORDS;
                if (count > caller->budget)
                        count = caller->budget;
                prepare_batch(caller, sequence, count);
                call_start_ns = elapsed_ns(caller->phase);
                if (call_start_ns == V2_CLOCK_FAILED ||
                    offer_batch(caller, batch_index, count, call_start_ns,
                                NULL) != 0)
                        return -1;
                batch_index++;
                sequence += count;
        }
        return 0;
}

/* The fixed-rate submitter: offer on the schedule, however late that lands. */
static int run_fixed_caller(v2_caller_t *caller) {
        const bench_config_t *config = caller->config;
        size_t batch_count =
            (config->records + BENCH_BATCH_RECORDS - 1U) / BENCH_BATCH_RECORDS;
        size_t batch_index;

        for (batch_index = caller->caller; batch_index < batch_count;
             batch_index += config->callers) {
                size_t sequence = batch_index * BENCH_BATCH_RECORDS;
                size_t count    = config->records - sequence;
                uint64_t call_start_ns;
                size_t index;

                if (count > BENCH_BATCH_RECORDS)
                        count = BENCH_BATCH_RECORDS;
                for (index = 0; index < count; ++index) {
                        if (bench_schedule_offset_ns(
                                (uint64_t)(sequence + index),
                                config->offered_records_per_second,
                                &caller->intended[index]) != 0)
                                return -1;
                }
                prepare_batch(caller, sequence, count);
                if (await_due(caller->phase, caller->intended[count - 1U]) != 0)
                        return -1;
                call_start_ns = elapsed_ns(caller->phase);
                if (call_start_ns == V2_CLOCK_FAILED ||
                    offer_batch(caller, batch_index, count, call_start_ns,
                                caller->intended) != 0)
                        return -1;
        }
        return 0;
}

static void *caller_main(void *opaque) {
        v2_caller_t *caller = opaque;

        if ((caller->config->fixed_rate ? run_fixed_caller(caller)
                                        : run_closed_caller(caller)) != 0)
                v2_fail(caller->phase);
        submitter_done(caller->phase);
        return NULL;
}

bench_v2_phase_t *bench_v2_create(const bench_config_t *config) {
        bench_v2_phase_t *phase = calloc(1U, sizeof(*phase));
        size_t pool_records     = BENCH_BATCH_RECORDS * config->callers;
        size_t index;

        if (!phase) {
                perror("allocate v2 measured phase");
                return NULL;
        }
        phase->config        = config;
        phase->slab_capacity = config->max_outstanding;
        phase->slab          = calloc(phase->slab_capacity,
                                      sizeof(*phase->slab));
        phase->free_slots =
            calloc(phase->slab_capacity, sizeof(*phase->free_slots));
        phase->caller_outstanding =
            calloc(config->callers, sizeof(*phase->caller_outstanding));
        /* The record pool is built before the measured interval starts, so no
           offer ever waits on an allocator. */
        phase->payload_pool  = malloc(pool_records * config->payload_bytes);
        phase->key_pool      = malloc(pool_records * 8U);
        phase->message_pool =
            calloc(pool_records, sizeof(*phase->message_pool));
        phase->intended_pool =
            calloc(pool_records, sizeof(*phase->intended_pool));
        if (!phase->slab || !phase->free_slots || !phase->caller_outstanding ||
            !phase->payload_pool || !phase->key_pool || !phase->message_pool ||
            !phase->intended_pool) {
                perror("allocate v2 measured phase");
                bench_v2_destroy(phase);
                return NULL;
        }
        for (index = 0; index < phase->slab_capacity; ++index)
                phase->free_slots[index] = index;
        phase->free_count = phase->slab_capacity;
        bench_histogram_reset(&phase->intended_to_terminal);
        bench_histogram_reset(&phase->accepted_to_terminal);
        bench_histogram_reset(&phase->call_start_to_accepted);
        bench_histogram_reset(&phase->intended_to_call_start);
        if (pthread_mutex_init(&phase->lock, NULL) != 0) {
                fprintf(stderr, "initialize v2 synchronization\n");
                bench_v2_destroy(phase);
                return NULL;
        }
        phase->lock_ready = 1;
        if (pthread_cond_init(&phase->changed, NULL) != 0) {
                fprintf(stderr, "initialize v2 synchronization\n");
                bench_v2_destroy(phase);
                return NULL;
        }
        phase->condition_ready    = 1;
        phase->active_submitters = config->callers;
        return phase;
}

void bench_v2_destroy(bench_v2_phase_t *phase) {
        if (!phase)
                return;
        if (phase->condition_ready)
                (void)pthread_cond_destroy(&phase->changed);
        if (phase->lock_ready)
                (void)pthread_mutex_destroy(&phase->lock);
        free(phase->slab);
        free(phase->free_slots);
        free(phase->caller_outstanding);
        free(phase->payload_pool);
        free(phase->key_pool);
        free(phase->message_pool);
        free(phase->intended_pool);
        free(phase);
}

int bench_run_v2_phase(rd_kafka_t *producer,
                       const bench_config_t *config,
                       bench_v2_phase_t *phase) {
        rd_kafka_topic_t *topic;
        pthread_t threads[4];
        v2_caller_t callers[4];
        size_t base_budget  = config->max_outstanding / config->callers;
        size_t extra_budget = config->max_outstanding % config->callers;
        size_t started      = 0;
        size_t index;
        uint64_t now;

        if (config->callers == 0 || config->callers > 4 ||
            config->max_outstanding == 0 ||
            (config->fixed_rate && base_budget < BENCH_BATCH_RECORDS)) {
                fprintf(stderr, "v2 phase cannot run this admission shape\n");
                return -1;
        }
        topic = rd_kafka_topic_new(producer, config->topic, NULL);
        if (!topic) {
                fprintf(stderr, "create librdkafka topic handle: %s\n",
                        rd_kafka_err2str(rd_kafka_last_error()));
                return -1;
        }
        now = bench_now_ns();
        if (now == 0 || now > UINT64_MAX - V2_SCHEDULE_LEAD_NS) {
                fprintf(stderr, "monotonic clock failed\n");
                rd_kafka_topic_destroy(topic);
                return -1;
        }
        phase->started_ns =
            config->fixed_rate ? now + V2_SCHEDULE_LEAD_NS : now;
        for (index = 0; index < config->callers; ++index) {
                callers[index] = (v2_caller_t){
                    .producer = producer,
                    .topic    = topic,
                    .config   = config,
                    .phase    = phase,
                    .caller   = index,
                    .budget = base_budget + (index < extra_budget ? 1U : 0U),
                    .payload = phase->payload_pool +
                               (index * BENCH_BATCH_RECORDS *
                                config->payload_bytes),
                    .keys = phase->key_pool +
                            (index * BENCH_BATCH_RECORDS * 8U),
                    .messages = phase->message_pool +
                                (index * BENCH_BATCH_RECORDS),
                    .intended = phase->intended_pool +
                                (index * BENCH_BATCH_RECORDS),
                };
                if (pthread_create(&threads[index], NULL, caller_main,
                                   &callers[index]) != 0) {
                        size_t absent;

                        v2_fail(phase);
                        for (absent = index; absent < config->callers; ++absent)
                                submitter_done(phase);
                        break;
                }
                started++;
        }
        /* The polling thread is the only one that dispatches delivery reports,
           which is what keeps the callback off every submitter's back. */
        while (!submitters_done(phase))
                (void)rd_kafka_poll(producer, 10);
        while (started > 0) {
                started--;
                if (pthread_join(threads[started], NULL) != 0)
                        v2_fail(phase);
        }
        if (rd_kafka_flush(producer, BENCH_DELIVERY_TIMEOUT_MS) !=
            RD_KAFKA_RESP_ERR_NO_ERROR)
                fprintf(stderr,
                        "librdkafka did not drain every accepted record inside "
                        "the delivery bound\n");
        /* Past the drain deadline every offer still outstanding is unknown,
           and no later callback may change that account. */
        v2_lock(phase);
        phase->closed  = 1;
        phase->unknown = phase->outstanding;
        v2_unlock(phase);
        rd_kafka_topic_destroy(topic);
        return v2_failed(phase) ? -1 : 0;
}
