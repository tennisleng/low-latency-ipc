package com.tennisleng.ipc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Writes messages into the ring buffer. Run the Consumer in a separate terminal.
 *
 *   ./gradlew runProducer
 *   ./gradlew runProducer --args="--fast"           # no throttle
 *   ./gradlew runProducer --args="--count 100000"   # stop after N
 */
public class Producer {

    private static final Path SHARED_FILE = Path.of("/tmp/ipc-ring-buffer.dat");
    private static final int CAPACITY = 1024;
    private static final int MAX_PAYLOAD = 256;

    public static void main(String[] args) throws IOException, InterruptedException {
        boolean fast = false;
        long maxMsgs = Long.MAX_VALUE;
        WaitStrategy ws = WaitStrategy.YIELD;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--fast"  -> fast = true;
                case "--count" -> maxMsgs = Long.parseLong(args[++i]);
                case "--spin"  -> ws = WaitStrategy.SPIN;
                case "--sleep" -> ws = WaitStrategy.SLEEP;
            }
        }

        System.out.printf("[Producer] file=%s  capacity=%d  mode=%s%n",
                SHARED_FILE, CAPACITY, fast ? "fast" : "throttled");

        LatencyHistogram hist = new LatencyHistogram();
        final WaitStrategy wait = ws;

        // print stats on ctrl+c
        final boolean[] running = {true};
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running[0] = false;
            System.out.println("\n" + hist.prettyPrint());
        }));

        try (SPSCRingBuffer ring = new SPSCRingBuffer(SHARED_FILE, CAPACITY, MAX_PAYLOAD)) {
            ring.reset();

            long count = 0;
            long lastReport = System.nanoTime();

            while (running[0] && count < maxMsgs) {
                byte[] payload = ("msg-" + count).getBytes(StandardCharsets.UTF_8);

                long t0 = System.nanoTime();
                while (!ring.write(payload)) wait.idle();
                wait.reset();
                hist.recordSince(t0);

                count++;

                // progress every ~2 sec
                long now = System.nanoTime();
                if (now - lastReport > 2_000_000_000L) {
                    System.out.printf("[Producer] %,d sent  buffer~%d/%d%n",
                            count, ring.size(), ring.getCapacity());
                    lastReport = now;
                }

                if (!fast) Thread.sleep(0, 100_000);
            }

            System.out.printf("[Producer] done. %,d messages.%n", count);
            System.out.println(hist.prettyPrint());
        }
    }
}
