/* Shared destruction of closed-loop and fixed-load phase ownership. */
#include "benchmark.h"

#include <stdlib.h>
#include <string.h>

void bench_destroy_phase(bench_phase_t *phase) {
        free(phase->admitted_ns);
        free(phase->completed_ns);
        free(phase->latency_ns);
        free(phase->intended_ns);
        free(phase->corrected_latency_ns);
        free(phase->schedule_delay_ns);
        free(phase->deliveries);
        free(phase->submissions);
        free(phase->caller_outstanding);
        bench_fixed_destroy(phase->fixed);
        memset(phase, 0, sizeof(*phase));
}
