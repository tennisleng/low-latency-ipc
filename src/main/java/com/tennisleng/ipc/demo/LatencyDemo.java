package com.tennisleng.ipc.demo;

import com.tennisleng.ipc.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * End-to-end latency measurement demo.
 *
 * Runs producer and consumer on separate threads (same JVM for simplicity)
 * and measures true IPC latency using embedded timestamps.
 *
 * This is the "proof" that the system works: you should see flat, consistent
 * latency with no GC spikes.
 *
 * Run: ./gradlew runLatencyDemo
 */
public class LatencyDemo {

    private static final Path SHARED_FILE = Path.of("/tmp/ipc-latency-demo.dat");
    private static final int CAPACITY = 4096;
    private static final int MAX_PAYLOAD = 64;
    private static final int MESSAGE_COUNT = 500_000;
    private static final int WARMUP_COUNT = 50_000;

    public static void main(String[] args) throws Exception {
        System.out.println("┌──────────────────────────────────────────┐");
        System.out.println("│     End-to-End Latency Measurement      │");
        System.out.println("├──────────────────────────────────────────┤");
        System.out.printf("│  Messages:  %,d (+ %,d warmup)%n", MESSAGE_COUNT, WARMUP_COUNT);
        System.out.printf("│  Capacity:  %d slots%n", CAPACITY);
        System.out.println("│  Mode:      timestamp-in-payload        │");
        System.out.println("└──────────────────────────────────────────┘");
        System.out.println();

        SPSCRingBuffer ring = new SPSCRingBuffer(SHARED_FILE, CAPACITY, MAX_PAYLOAD);
        ring.reset();

        LatencyHistogram histogram = new LatencyHistogram();
        int totalMessages = WARMUP_COUNT + MESSAGE_COUNT;

        // ── Consumer thread ─────────────────────────────────────────────
        Thread consumer = new Thread(() -> {
            MessageView view = new MessageView();
            int received = 0;

            while (received < totalMessages) {
                if (ring.read(view)) {
                    long latency = TimestampedMessage.decodeLatency(view);

                    // Only record after warmup
                    if (received >= WARMUP_COUNT) {
                        histogram.record(latency);
                    }

                    received++;
                } else {
                    Thread.onSpinWait();
                }
            }
        }, "consumer");

        // ── Producer thread ─────────────────────────────────────────────
        Thread producer = new Thread(() -> {
            ByteBuffer scratch = ByteBuffer.allocate(MAX_PAYLOAD);

            for (int i = 0; i < totalMessages; i++) {
                TimestampedMessage.encodeTimestampOnly(scratch);

                while (!ring.write(scratch)) {
                    Thread.onSpinWait();
                }
            }
        }, "producer");

        // ── Run ─────────────────────────────────────────────────────────
        long startTime = System.nanoTime();

        consumer.start();
        producer.start();

        producer.join();
        consumer.join();

        long elapsed = System.nanoTime() - startTime;

        // ── Results ─────────────────────────────────────────────────────
        System.out.println("=== Results ===");
        System.out.println();
        System.out.println(histogram.prettyPrint());
        System.out.printf("Total time: %.2f seconds%n", elapsed / 1_000_000_000.0);
        System.out.printf("Throughput: %,.0f msgs/sec%n",
                (double) MESSAGE_COUNT / (elapsed / 1_000_000_000.0));
        System.out.println();

        ring.close();

        // ── Now run the same test with LinkedBlockingQueue for comparison ─
        System.out.println("=== Baseline: LinkedBlockingQueue ===");
        System.out.println();
        runLBQBaseline();
    }

    private static void runLBQBaseline() throws InterruptedException {
        java.util.concurrent.LinkedBlockingQueue<long[]> lbq =
                new java.util.concurrent.LinkedBlockingQueue<>(4096);

        LatencyHistogram lbqHistogram = new LatencyHistogram();
        int total = WARMUP_COUNT + MESSAGE_COUNT;

        Thread consumer = new Thread(() -> {
            int received = 0;
            try {
                while (received < total) {
                    long[] msg = lbq.take();
                    long latency = System.nanoTime() - msg[0];
                    if (received >= WARMUP_COUNT) {
                        lbqHistogram.record(latency);
                    }
                    received++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "lbq-consumer");

        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) {
                    lbq.put(new long[]{System.nanoTime()}); // allocates every time!
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "lbq-producer");

        long startTime = System.nanoTime();
        consumer.start();
        producer.start();
        producer.join();
        consumer.join();
        long elapsed = System.nanoTime() - startTime;

        System.out.println(lbqHistogram.prettyPrint());
        System.out.printf("Total time: %.2f seconds%n", elapsed / 1_000_000_000.0);
        System.out.printf("Throughput: %,.0f msgs/sec%n",
                (double) MESSAGE_COUNT / (elapsed / 1_000_000_000.0));
        System.out.println();
        System.out.println("Compare the p99/p999 between the two — the LBQ will show");
        System.out.println("GC-induced spikes that the SPSC ring buffer avoids.");
    }
}
