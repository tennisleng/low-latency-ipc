package com.tennisleng.ipc;

/**
 * What to do when there's nothing to read/write.
 * Trade-off is always latency vs CPU burn.
 */
public enum WaitStrategy {

    /** Busy-spin. Best latency, pegs a core at 100%. */
    SPIN {
        @Override public void idle() { Thread.onSpinWait(); }
    },

    /** Thread.yield(). Decent latency, plays nicer with the scheduler. */
    YIELD {
        @Override public void idle() { Thread.yield(); }
    },

    /** Sleep. Worst latency (~50-100us real), but barely uses CPU. Good for demos. */
    SLEEP {
        @Override public void idle() {
            try { Thread.sleep(0, 1); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    },

    /** Spin for a bit, then yield, then sleep. Good general-purpose default. */
    PROGRESSIVE {
        private static final int SPIN_LIMIT = 100;
        private static final int YIELD_LIMIT = 300;
        private final ThreadLocal<int[]> ctr = ThreadLocal.withInitial(() -> new int[]{0});

        @Override public void idle() {
            int[] c = ctr.get();
            if (c[0] < SPIN_LIMIT)       Thread.onSpinWait();
            else if (c[0] < YIELD_LIMIT) Thread.yield();
            else {
                try { Thread.sleep(0, 1); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            c[0]++;
        }

        @Override public void reset() { ctr.get()[0] = 0; }
    };

    public abstract void idle();

    /** Reset back-off state (only matters for PROGRESSIVE). */
    public void reset() {}
}
