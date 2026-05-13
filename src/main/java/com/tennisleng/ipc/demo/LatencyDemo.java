package com.tennisleng.ipc.demo;

import com.tennisleng.ipc.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * Automated latency test: spins up producer + consumer threads, measures
 * real end-to-end latency using timestamps baked into the messages, then
 * runs the same thing with LinkedBlockingQueue to show the difference.
 *
 *   ./gradlew runLatencyDemo
 */
public class LatencyDemo {

    private static final Path FILE = Path.of("/tmp/ipc-latency-demo.dat");
    private static final int CAPACITY = 4096;
    private static final int MAX_PAYLOAD = 64;
    private static final int MSG_COUNT = 500_000;
    private static final int WARMUP = 50_000;

    public static void main(String[] args) throws Exception {
        System.out.printf("[LatencyDemo] %,d messages (%,d warmup), capacity=%d%n%n",
                MSG_COUNT, WARMUP, CAPACITY);

        SPSCRingBuffer ring = new SPSCRingBuffer(FILE, CAPACITY, MAX_PAYLOAD);
        ring.reset();

        LatencyHistogram hist = new LatencyHistogram();
        int total = WARMUP + MSG_COUNT;

        Thread consumer = new Thread(() -> {
            MessageView view = new MessageView();
            int rx = 0;
            while (rx < total) {
                if (ring.read(view)) {
                    long lat = TimestampedMessage.decodeLatency(view);
                    if (rx >= WARMUP) hist.record(lat);
                    rx++;
                } else {
                    Thread.onSpinWait();
                }
            }
        }, "consumer");

        Thread producer = new Thread(() -> {
            ByteBuffer scratch = ByteBuffer.allocate(MAX_PAYLOAD);
            for (int i = 0; i < total; i++) {
                TimestampedMessage.encodeTimestampOnly(scratch);
                while (!ring.write(scratch)) Thread.onSpinWait();
            }
        }, "producer");

        long t0 = System.nanoTime();
        consumer.start();
        producer.start();
        producer.join();
        consumer.join();
        long elapsed = System.nanoTime() - t0;

        System.out.println("=== SPSC Ring Buffer ===");
        System.out.println(hist.prettyPrint());
        System.out.printf("  time: %.2fs   throughput: %,.0f msg/s%n%n",
                elapsed / 1e9, MSG_COUNT / (elapsed / 1e9));

        ring.close();

        // baseline comparison
        System.out.println("=== LinkedBlockingQueue (baseline) ===");
        runLBQ();
    }

    private static void runLBQ() throws InterruptedException {
        var lbq = new java.util.concurrent.LinkedBlockingQueue<long[]>(4096);
        LatencyHistogram hist = new LatencyHistogram();
        int total = WARMUP + MSG_COUNT;

        Thread consumer = new Thread(() -> {
            int rx = 0;
            try {
                while (rx < total) {
                    long[] msg = lbq.take();
                    long lat = System.nanoTime() - msg[0];
                    if (rx >= WARMUP) hist.record(lat);
                    rx++;
                }
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }, "lbq-consumer");

        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < total; i++) {
                    lbq.put(new long[]{System.nanoTime()});
                }
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }, "lbq-producer");

        long t0 = System.nanoTime();
        consumer.start();
        producer.start();
        producer.join();
        consumer.join();
        long elapsed = System.nanoTime() - t0;

        System.out.println(hist.prettyPrint());
        System.out.printf("  time: %.2fs   throughput: %,.0f msg/s%n%n",
                elapsed / 1e9, MSG_COUNT / (elapsed / 1e9));
        System.out.println("look at the p99/p999 — LBQ will have GC spikes that SPSC doesn't.");
    }
}
