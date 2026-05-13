package com.tennisleng.ipc;

/**
 * A dead-simple latency histogram that records measurements in nanoseconds.
 * Zero allocations after construction — safe for the hot path.
 *
 * Instead of storing every data point (which would need a growable array = GC),
 * we bucket measurements into logarithmic ranges:
 *
 *   Bucket 0:   0 -   99 ns
 *   Bucket 1: 100 -  199 ns
 *   Bucket 2: 200 -  499 ns
 *   Bucket 3: 500 -  999 ns
 *   Bucket 4:   1 -  1.9 µs
 *   Bucket 5:   2 -  4.9 µs
 *   Bucket 6:   5 -  9.9 µs
 *   Bucket 7:  10 - 49.9 µs
 *   Bucket 8:  50 - 99.9 µs
 *   Bucket 9: 100 - 999  µs
 *   Bucket 10:  1 -  9.9 ms
 *   Bucket 11: 10+       ms   (GC pause territory)
 *
 * This lets us see the distribution shape — especially the long tail
 * that GC pauses create — without any allocation in the recording path.
 *
 * For real production use, look at HdrHistogram by Gil Tene.
 * This is a learning tool that shows the same concept.
 */
public class LatencyHistogram {

    private static final String[] BUCKET_LABELS = {
            "    0-99ns ",
            "  100-199ns",
            "  200-499ns",
            "  500-999ns",
            "    1-1.9µs",
            "    2-4.9µs",
            "    5-9.9µs",
            "   10-49µs ",
            "   50-99µs ",
            "  100-999µs",
            "    1-9.9ms",
            "   10ms+   "
    };

    private static final int NUM_BUCKETS = BUCKET_LABELS.length;

    // pre-allocated — no GC pressure
    private final long[] counts = new long[NUM_BUCKETS];
    private long totalCount;
    private long minNanos = Long.MAX_VALUE;
    private long maxNanos = Long.MIN_VALUE;
    private long sumNanos;

    /**
     * Record a single latency measurement in nanoseconds.
     * This method does ZERO allocations. Safe for the hot path.
     */
    public void record(long nanos) {
        int bucket = bucketFor(nanos);
        counts[bucket]++;
        totalCount++;
        sumNanos += nanos;

        if (nanos < minNanos) minNanos = nanos;
        if (nanos > maxNanos) maxNanos = nanos;
    }

    /** Record a measurement by computing the delta from a start nanoTime. */
    public void recordSince(long startNanos) {
        record(System.nanoTime() - startNanos);
    }

    /**
     * Map a nanosecond value to a bucket index.
     * Branchy but simple — the JIT will likely convert this to a jump table.
     */
    private static int bucketFor(long nanos) {
        if (nanos <       100) return 0;
        if (nanos <       200) return 1;
        if (nanos <       500) return 2;
        if (nanos <      1000) return 3;   //  1 µs
        if (nanos <      2000) return 4;   //  2 µs
        if (nanos <      5000) return 5;   //  5 µs
        if (nanos <     10000) return 6;   // 10 µs
        if (nanos <     50000) return 7;   // 50 µs
        if (nanos <    100000) return 8;   //100 µs
        if (nanos <   1000000) return 9;   //  1 ms
        if (nanos <  10000000) return 10;  // 10 ms
        return 11;                         // 10+ ms (GC land)
    }

    /** Reset all counters. */
    public void reset() {
        for (int i = 0; i < NUM_BUCKETS; i++) counts[i] = 0;
        totalCount = 0;
        minNanos = Long.MAX_VALUE;
        maxNanos = Long.MIN_VALUE;
        sumNanos = 0;
    }

    public long getTotalCount() { return totalCount; }
    public long getMinNanos()   { return totalCount > 0 ? minNanos : 0; }
    public long getMaxNanos()   { return totalCount > 0 ? maxNanos : 0; }
    public double getMeanNanos() { return totalCount > 0 ? (double) sumNanos / totalCount : 0; }

    /**
     * Compute an approximate percentile (e.g., 0.99 for p99).
     * Returns the lower bound of the bucket that contains the percentile.
     */
    public String getPercentileBucket(double percentile) {
        long target = (long) (totalCount * percentile);
        long running = 0;
        for (int i = 0; i < NUM_BUCKETS; i++) {
            running += counts[i];
            if (running >= target) {
                return BUCKET_LABELS[i].trim();
            }
        }
        return BUCKET_LABELS[NUM_BUCKETS - 1].trim();
    }

    /**
     * Print a horizontal bar chart of the latency distribution.
     * Allocates strings — call this for reporting, not in the hot path.
     */
    public String prettyPrint() {
        if (totalCount == 0) return "[no data recorded]";

        // Find max count for scaling the bars
        long maxCount = 0;
        for (long c : counts) {
            if (c > maxCount) maxCount = c;
        }

        int barWidth = 40;
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Latency Distribution (%,d samples)%n", totalCount));
        sb.append(String.format("  min=%.1fµs  mean=%.1fµs  max=%.1fµs%n",
                getMinNanos() / 1000.0,
                getMeanNanos() / 1000.0,
                getMaxNanos() / 1000.0));
        sb.append(String.format("  p50=%s  p99=%s  p999=%s%n",
                getPercentileBucket(0.50),
                getPercentileBucket(0.99),
                getPercentileBucket(0.999)));
        sb.append("  ──────────────────────────────────────────────────────\n");

        for (int i = 0; i < NUM_BUCKETS; i++) {
            long c = counts[i];
            if (c == 0 && i > 0 && i < NUM_BUCKETS - 1) {
                // skip empty middle buckets for cleaner output
                continue;
            }
            int barLen = maxCount > 0 ? (int) (c * barWidth / maxCount) : 0;
            double pct = totalCount > 0 ? (c * 100.0 / totalCount) : 0;

            sb.append(String.format("  %s │%s %,d (%.1f%%)%n",
                    BUCKET_LABELS[i],
                    "█".repeat(barLen),
                    c,
                    pct));
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return prettyPrint();
    }
}
