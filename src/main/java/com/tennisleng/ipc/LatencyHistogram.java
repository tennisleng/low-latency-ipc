package com.tennisleng.ipc;

/**
 * Quick and dirty latency histogram. Pre-allocates everything so
 * recording is safe in the hot path (no GC pressure).
 *
 * Buckets are logarithmic — the last few are basically "GC pause detected."
 * For production use you'd want HdrHistogram, but this gets the point across
 * and keeps the dependency list clean.
 */
public class LatencyHistogram {

    private static final String[] LABELS = {
            "    0-99ns ",
            "  100-199ns",
            "  200-499ns",
            "  500-999ns",
            "    1-1.9us",
            "    2-4.9us",
            "    5-9.9us",
            "   10-49us ",
            "   50-99us ",
            "  100-999us",
            "    1-9.9ms",
            "   10ms+   "
    };

    private static final int BUCKETS = LABELS.length;

    private final long[] counts = new long[BUCKETS];
    private long total;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private long sum;

    /** Record one measurement in nanos. No allocation. */
    public void record(long nanos) {
        counts[bucket(nanos)]++;
        total++;
        sum += nanos;
        if (nanos < min) min = nanos;
        if (nanos > max) max = nanos;
    }

    /** Shorthand: record(System.nanoTime() - startNanos) */
    public void recordSince(long startNanos) {
        record(System.nanoTime() - startNanos);
    }

    private static int bucket(long ns) {
        if (ns <       100) return 0;
        if (ns <       200) return 1;
        if (ns <       500) return 2;
        if (ns <      1000) return 3;
        if (ns <      2000) return 4;
        if (ns <      5000) return 5;
        if (ns <     10000) return 6;
        if (ns <     50000) return 7;
        if (ns <    100000) return 8;
        if (ns <   1000000) return 9;
        if (ns <  10000000) return 10;
        return 11;
    }

    public void reset() {
        for (int i = 0; i < BUCKETS; i++) counts[i] = 0;
        total = 0;
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
        sum = 0;
    }

    public long getTotalCount() { return total; }
    public long getMinNanos()   { return total > 0 ? min : 0; }
    public long getMaxNanos()   { return total > 0 ? max : 0; }
    public double getMeanNanos() { return total > 0 ? (double) sum / total : 0; }

    /** Returns the bucket label that contains the given percentile (e.g. 0.99). */
    public String getPercentileBucket(double pct) {
        long target = (long) (total * pct);
        long running = 0;
        for (int i = 0; i < BUCKETS; i++) {
            running += counts[i];
            if (running >= target) return LABELS[i].trim();
        }
        return LABELS[BUCKETS - 1].trim();
    }

    /** ASCII bar chart. Allocates strings — call for reporting, not in the loop. */
    public String prettyPrint() {
        if (total == 0) return "[no data recorded]";

        long maxC = 0;
        for (long c : counts) if (c > maxC) maxC = c;

        int barW = 40;
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Latency Distribution (%,d samples)%n", total));
        sb.append(String.format("  min=%.1fus  mean=%.1fus  max=%.1fus%n",
                min / 1000.0, getMeanNanos() / 1000.0, max / 1000.0));
        sb.append(String.format("  p50=%s  p99=%s  p999=%s%n",
                getPercentileBucket(0.50), getPercentileBucket(0.99), getPercentileBucket(0.999)));
        sb.append("  ---\n");

        for (int i = 0; i < BUCKETS; i++) {
            long c = counts[i];
            if (c == 0) continue;

            int bar = maxC > 0 ? (int) (c * barW / maxC) : 0;
            double pctOf = (c * 100.0 / total);
            sb.append(String.format("  %s |%s %,d (%.1f%%)%n",
                    LABELS[i], "#".repeat(bar), c, pctOf));
        }
        return sb.toString();
    }

    @Override
    public String toString() { return prettyPrint(); }
}
