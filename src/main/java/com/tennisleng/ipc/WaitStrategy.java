package com.tennisleng.ipc;

/**
 * Strategy for what the consumer/producer does when there's nothing to do.
 *
 * The trade-off is always: latency vs CPU usage.
 *
 * SPIN     — lowest latency, burns a full CPU core (100% usage)
 * YIELD    — slightly higher latency, lets other threads run
 * SLEEP    — highest latency, lowest CPU usage (good for demos)
 *
 * Real trading systems use SPIN because they dedicate entire cores
 * to the hot thread (via taskset/isolcpus on Linux). For this project,
 * YIELD is a good default.
 */
public enum WaitStrategy {

    /**
     * Busy-spin with Thread.onSpinWait().
     * Best latency (~100ns). Uses 100% of one CPU core.
     * Use when: you have a dedicated core and care about every nanosecond.
     */
    SPIN {
        @Override
        public void idle() {
            Thread.onSpinWait();
        }
    },

    /**
     * Thread.yield() — let the OS schedule other threads.
     * Decent latency (~1-10µs). CPU usage varies.
     * Use when: you want low latency but can't burn a whole core.
     */
    YIELD {
        @Override
        public void idle() {
            Thread.yield();
        }
    },

    /**
     * Thread.sleep(0, 1) — sleep for ~1 nanosecond (really ~50-100µs on most OS).
     * Highest latency. Lowest CPU usage.
     * Use when: you're running demos or don't need sub-millisecond response.
     */
    SLEEP {
        @Override
        public void idle() {
            try {
                Thread.sleep(0, 1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    },

    /**
     * Progressive back-off: spin → yield → sleep.
     * Adapts based on how long we've been idle.
     * Good general-purpose choice.
     */
    PROGRESSIVE {
        private static final int SPIN_TRIES = 100;
        private static final int YIELD_TRIES = 200;

        // Thread-local counter to track idle iterations
        // (can't use instance state on an enum, so we use ThreadLocal)
        private final ThreadLocal<int[]> counter = ThreadLocal.withInitial(() -> new int[]{0});

        @Override
        public void idle() {
            int[] c = counter.get();
            if (c[0] < SPIN_TRIES) {
                Thread.onSpinWait();
            } else if (c[0] < SPIN_TRIES + YIELD_TRIES) {
                Thread.yield();
            } else {
                try {
                    Thread.sleep(0, 1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            c[0]++;
        }

        @Override
        public void reset() {
            counter.get()[0] = 0;
        }
    };

    /**
     * Called when there's nothing to do (buffer empty for consumer, full for producer).
     */
    public abstract void idle();

    /**
     * Called when work was found (resets back-off state for PROGRESSIVE).
     * Default is a no-op for strategies that don't track state.
     */
    public void reset() {
        // no-op for most strategies
    }
}
