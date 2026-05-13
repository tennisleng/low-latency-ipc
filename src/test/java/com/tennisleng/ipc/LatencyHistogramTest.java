package com.tennisleng.ipc;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class LatencyHistogramTest {

    private LatencyHistogram h;

    @BeforeEach void setUp() { h = new LatencyHistogram(); }

    @Test void emptyState() {
        assertEquals(0, h.getTotalCount());
        assertEquals(0, h.getMinNanos());
        assertEquals(0, h.getMaxNanos());
        assertEquals(0.0, h.getMeanNanos());
        assertEquals("[no data recorded]", h.prettyPrint());
    }

    @Test void singleRecord() {
        h.record(500);
        assertEquals(1, h.getTotalCount());
        assertEquals(500, h.getMinNanos());
        assertEquals(500, h.getMaxNanos());
        assertEquals(500.0, h.getMeanNanos());
    }

    @Test void minMax() {
        h.record(100); h.record(50); h.record(1000); h.record(200);
        assertEquals(50, h.getMinNanos());
        assertEquals(1000, h.getMaxNanos());
    }

    @Test void mean() {
        h.record(100); h.record(200); h.record(300);
        assertEquals(200.0, h.getMeanNanos(), 0.01);
    }

    @Test void allBuckets() {
        h.record(50);       // 0-99ns
        h.record(150);      // 100-199ns
        h.record(300);      // 200-499ns
        h.record(750);      // 500-999ns
        h.record(1500);     // 1-1.9us
        h.record(3000);     // 2-4.9us
        h.record(7000);     // 5-9.9us
        h.record(25000);    // 10-49us
        h.record(75000);    // 50-99us
        h.record(500000);   // 100-999us
        h.record(5000000);  // 1-9.9ms
        h.record(15000000); // 10ms+
        assertEquals(12, h.getTotalCount());
    }

    @Test void reset() {
        h.record(100); h.record(200);
        h.reset();
        assertEquals(0, h.getTotalCount());
        assertEquals(0, h.getMinNanos());
    }

    @Test void percentile() {
        for (int i = 0; i < 99; i++) h.record(50);  // fast bucket
        h.record(15000000);                           // 10ms+ bucket
        assertEquals("0-99ns", h.getPercentileBucket(0.50));
        assertEquals("0-99ns", h.getPercentileBucket(0.99));
    }

    @Test void prettyPrintWorks() {
        for (int i = 0; i < 1000; i++) h.record(150);
        String out = h.prettyPrint();
        assertTrue(out.contains("1,000 samples"));
        assertTrue(out.contains("#"));
        assertTrue(out.contains("100-199ns"));
    }
}
