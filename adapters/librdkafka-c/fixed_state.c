/* Bounded fixed-load phase storage, time, and synchronization ownership. */
#include "benchmark.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct bench_fixed_state_s {
        pthread_mutex_t lock;
        pthread_cond_t changed;
        int condition_initialized;
        int fatal;
        size_t active_submitters;
};

void bench_fixed_lock(bench_fixed_state_t *state) {
        (void)pthread_mutex_lock(&state->lock);
}

void bench_fixed_unlock(bench_fixed_state_t *state) {
        (void)pthread_mutex_unlock(&state->lock);
}

void bench_fixed_changed(bench_fixed_state_t *state) {
        (void)pthread_cond_broadcast(&state->changed);
}

void bench_fixed_destroy(bench_fixed_state_t *state) {
        if (!state)
                return;
        if (state->condition_initialized)
                (void)pthread_cond_destroy(&state->changed);
        (void)pthread_mutex_destroy(&state->lock);
        free(state);
}

int bench_fixed_failed(bench_phase_t *phase) {
        int fatal;

        bench_fixed_lock(phase->fixed);
        fatal = phase->fixed->fatal;
        bench_fixed_unlock(phase->fixed);
        return fatal;
}

void bench_fixed_fail(bench_phase_t *phase) {
        bench_fixed_lock(phase->fixed);
        phase->fixed->fatal = 1;
        bench_fixed_changed(phase->fixed);
        bench_fixed_unlock(phase->fixed);
}

int bench_allocate_fixed_phase(bench_phase_t *phase,
                               const bench_config_t *config) {
        size_t records = config->records;

        memset(phase, 0, sizeof(*phase));
        phase->offered = records;
        phase->callers = config->callers;
        phase->offered_records_per_second =
            config->offered_records_per_second;
        phase->admitted_ns = calloc(records, sizeof(*phase->admitted_ns));
        phase->completed_ns = calloc(records, sizeof(*phase->completed_ns));
        phase->latency_ns = calloc(records, sizeof(*phase->latency_ns));
        phase->intended_ns = calloc(records, sizeof(*phase->intended_ns));
        phase->corrected_latency_ns =
            calloc(records, sizeof(*phase->corrected_latency_ns));
        phase->schedule_delay_ns =
            calloc(records, sizeof(*phase->schedule_delay_ns));
        phase->deliveries = calloc(records, sizeof(*phase->deliveries));
        phase->submissions = calloc(records, sizeof(*phase->submissions));
        phase->caller_outstanding =
            calloc(config->callers, sizeof(*phase->caller_outstanding));
        phase->fixed = calloc(1U, sizeof(*phase->fixed));
        if (!phase->admitted_ns || !phase->completed_ns ||
            !phase->latency_ns || !phase->intended_ns ||
            !phase->corrected_latency_ns || !phase->schedule_delay_ns ||
            !phase->deliveries || !phase->submissions ||
            !phase->caller_outstanding || !phase->fixed) {
                perror("allocate fixed-load evidence");
                return -1;
        }
        if (pthread_mutex_init(&phase->fixed->lock, NULL) != 0 ||
            pthread_cond_init(&phase->fixed->changed, NULL) != 0) {
                fprintf(stderr, "initialize fixed-load synchronization\n");
                return -1;
        }
        phase->fixed->condition_initialized = 1;
        phase->fixed->active_submitters = config->callers;
        return 0;
}

int bench_fixed_await_due(bench_phase_t *phase, uint64_t due_ns) {
        for (;;) {
                uint64_t now = bench_now_ns();
                uint64_t target_ns;
                uint64_t wait_ns;
                struct timespec wait;

                if (now == 0 ||
                    phase->started_ns > UINT64_MAX - due_ns ||
                    bench_fixed_failed(phase))
                        return -1;
                target_ns = phase->started_ns + due_ns;
                if (now >= target_ns)
                        return 0;
                wait_ns = target_ns - now;
                if (wait_ns > 1000000U)
                        wait_ns = 1000000U;
                wait.tv_sec = 0;
                wait.tv_nsec = (long)wait_ns;
                while (nanosleep(&wait, &wait) != 0 && errno == EINTR) {
                }
        }
}

int bench_fixed_await_budget(bench_phase_t *phase,
                             size_t caller,
                             size_t budget,
                             size_t count) {
        int failed;

        if (pthread_mutex_lock(&phase->fixed->lock) != 0)
                return -1;
        while (!phase->fixed->fatal &&
               phase->caller_outstanding[caller] + count > budget) {
                if (pthread_cond_wait(&phase->fixed->changed,
                                      &phase->fixed->lock) != 0) {
                        phase->fixed->fatal = 1;
                        break;
                }
        }
        failed = phase->fixed->fatal;
        if (pthread_mutex_unlock(&phase->fixed->lock) != 0)
                return -1;
        return failed ? -1 : 0;
}

void bench_fixed_submitter_done(bench_phase_t *phase) {
        bench_fixed_lock(phase->fixed);
        if (phase->fixed->active_submitters > 0)
                phase->fixed->active_submitters--;
        bench_fixed_changed(phase->fixed);
        bench_fixed_unlock(phase->fixed);
}

int bench_fixed_submitters_done(bench_phase_t *phase) {
        int done;

        bench_fixed_lock(phase->fixed);
        done = phase->fixed->active_submitters == 0;
        bench_fixed_unlock(phase->fixed);
        return done;
}

int bench_fixed_begin_submission(bench_phase_t *phase, size_t batch_index) {
        if (pthread_mutex_lock(&phase->fixed->lock) != 0)
                return -1;
        while (!phase->fixed->fatal &&
               phase->submission_count < batch_index) {
                if (pthread_cond_wait(&phase->fixed->changed,
                                      &phase->fixed->lock) != 0) {
                        phase->fixed->fatal = 1;
                        break;
                }
        }
        if (phase->fixed->fatal || phase->submission_count != batch_index) {
                (void)pthread_mutex_unlock(&phase->fixed->lock);
                return -1;
        }
        return 0;
}

void bench_fixed_finish_submission(bench_phase_t *phase) {
        phase->submission_count++;
        bench_fixed_changed(phase->fixed);
        bench_fixed_unlock(phase->fixed);
}

void bench_fixed_abort_submission(bench_phase_t *phase) {
        phase->fixed->fatal = 1;
        bench_fixed_changed(phase->fixed);
        bench_fixed_unlock(phase->fixed);
}
