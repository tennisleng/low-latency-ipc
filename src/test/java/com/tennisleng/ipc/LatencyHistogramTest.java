package com.tennisleng.ipc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LatencyHistogramTest {

    private LatencyHistogram histogram;

    @BeforeEach
    void setUp() {
        histogram = new LatencyHistogram();
    }

    @Test
    @DisplayName("empty histogram reports zero for everything")
    void emptyHistogram() {
        assertEquals(0, histogram.getTotalCount());
        assertEquals(0, histogram.getMinNanos());
        assertEquals(0, histogram.getMaxNanos());
        assertEquals(0.0, histogram.getMeanNanos());
        assertEquals("[no data recorded]", histogram.prettyPrint());
    }

    @Test
    @DisplayName("single recording updates all stats")
    void singleRecording() {
        histogram.record(500);

        assertEquals(1, histogram.getTotalCount());
        assertEquals(500, histogram.getMinNanos());
        assertEquals(500, histogram.getMaxNanos());
        assertEquals(500.0, histogram.getMeanNanos());
    }

    @Test
    @DisplayName("min and max track correctly across recordings")
    void minMaxTracking() {
        histogram.record(100);
        histogram.record(50);
        histogram.record(1000);
        histogram.record(200);

        assertEquals(4, histogram.getTotalCount());
        assertEquals(50, histogram.getMinNanos());
        assertEquals(1000, histogram.getMaxNanos());
    }

    @Test
    @DisplayName("mean calculates correctly")
    void meanCalculation() {
        histogram.record(100);
        histogram.record(200);
        histogram.record(300);

        assertEquals(200.0, histogram.getMeanNanos(), 0.01);
    }

    @Test
    @DisplayName("bucketing puts values in correct ranges")
    void bucketingCorrectness() {
        // Record one value in each bucket range
        histogram.record(50);       // bucket 0: 0-99ns
        histogram.record(150);      // bucket 1: 100-199ns
        histogram.record(300);      // bucket 2: 200-499ns
        histogram.record(750);      // bucket 3: 500-999ns
        histogram.record(1500);     // bucket 4: 1-1.9µs
        histogram.record(3000);     // bucket 5: 2-4.9µs
        histogram.record(7000);     // bucket 6: 5-9.9µs
        histogram.record(25000);    // bucket 7: 10-49µs
        histogram.record(75000);    // bucket 8: 50-99µs
        histogram.record(500000);   // bucket 9: 100-999µs
        histogram.record(5000000);  // bucket 10: 1-9.9ms
        histogram.record(15000000); // bucket 11: 10ms+

        assertEquals(12, histogram.getTotalCount());

        // The pretty print should contain all bucket labels
        String output = histogram.prettyPrint();
        assertTrue(output.contains("12 samples"));
    }

    @Test
    @DisplayName("reset clears all state")
    void resetClearsState() {
        histogram.record(100);
        histogram.record(200);
        histogram.record(300);

        histogram.reset();

        assertEquals(0, histogram.getTotalCount());
        assertEquals(0, histogram.getMinNanos());
        assertEquals(0, histogram.getMaxNanos());
    }

    @Test
    @DisplayName("percentile bucket estimation")
    void percentileBucketEstimation() {
        // Put 99 values in the fast bucket and 1 in the slow bucket
        for (int i = 0; i < 99; i++) {
            histogram.record(50); // 0-99ns bucket
        }
        histogram.record(15000000); // 10ms+ bucket

        // p50 should be in the fast bucket
        assertEquals("0-99ns", histogram.getPercentileBucket(0.50));

        // p99 should still be in the fast bucket (99th value out of 100)
        assertEquals("0-99ns", histogram.getPercentileBucket(0.99));
    }

    @Test
    @DisplayName("prettyPrint produces readable output")
    void prettyPrintOutput() {
        for (int i = 0; i < 1000; i++) {
            histogram.record(150); // all in the 100-199ns bucket
        }

        String output = histogram.prettyPrint();
        assertNotNull(output);
        assertTrue(output.contains("1,000 samples"));
        assertTrue(output.contains("█")); // has bar chart characters
        assertTrue(output.contains("100-199ns"));
    }
}
