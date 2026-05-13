package com.tennisleng.ipc;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Reads messages from the ring buffer. Run the Producer in a separate terminal.
 *
 *   ./gradlew runConsumer
 *   ./gradlew runConsumer --args="--verbose"   # print every message
 *   ./gradlew runConsumer --args="--spin"      # busy-wait (lowest latency)
 */
public class Consumer {

    private static final Path SHARED_FILE = Path.of("/tmp/ipc-ring-buffer.dat");
    private static final int CAPACITY = 1024;
    private static final int MAX_PAYLOAD = 256;

    public static void main(String[] args) throws IOException {
        boolean verbose = false;
        WaitStrategy ws = WaitStrategy.YIELD;

        for (String arg : args) {
            switch (arg) {
                case "--verbose"     -> verbose = true;
                case "--spin"        -> ws = WaitStrategy.SPIN;
                case "--sleep"       -> ws = WaitStrategy.SLEEP;
                case "--progressive" -> ws = WaitStrategy.PROGRESSIVE;
            }
        }

        System.out.printf("[Consumer] waiting for messages (wait=%s)...%n", ws);

        LatencyHistogram hist = new LatencyHistogram();
        final WaitStrategy wait = ws;
        final boolean verb = verbose;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n" + hist.prettyPrint());
        }));

        try (SPSCRingBuffer ring = new SPSCRingBuffer(SHARED_FILE, CAPACITY, MAX_PAYLOAD)) {
            // allocated once — reused on every read. this is the whole point.
            MessageView view = new MessageView();

            long count = 0;
            long lastReport = System.nanoTime();

            while (true) {
                long t0 = System.nanoTime();

                if (ring.read(view)) {
                    hist.recordSince(t0);
                    wait.reset();

                    if (verb) System.out.println("  " + view.getPayloadAsString());
                    count++;

                    long now = System.nanoTime();
                    if (now - lastReport > 2_000_000_000L) {
                        System.out.printf("[Consumer] %,d received  buffer~%d/%d%n",
                                count, ring.size(), ring.getCapacity());
                        lastReport = now;
                    }
                } else {
                    wait.idle();
                }
            }
        }
    }
}
