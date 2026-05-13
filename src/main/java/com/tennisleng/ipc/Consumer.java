package com.tennisleng.ipc;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Consumer process — reads messages from the shared ring buffer.
 *
 * Run: ./gradlew runConsumer
 * Then in another terminal: ./gradlew runProducer
 *
 * NOTICE: Zero `new` keywords inside the while(true) loop.
 * The MessageView is allocated ONCE — that's the Flyweight pattern.
 */
public class Consumer {

    private static final Path SHARED_FILE = Path.of("/tmp/ipc-ring-buffer.dat");
    private static final int CAPACITY = 1024;
    private static final int MAX_PAYLOAD = 256;

    public static void main(String[] args) throws IOException {
        System.out.println("[Consumer] Waiting for messages...");

        try (SPSCRingBuffer ring = new SPSCRingBuffer(SHARED_FILE, CAPACITY, MAX_PAYLOAD)) {
            MessageView view = new MessageView();   // ONE allocation
            long messageCount = 0;

            while (true) {                           // ZERO allocations inside
                if (ring.read(view)) {
                    if (messageCount % 10_000 == 0) {
                        System.out.println("[Consumer] Received: " + view.getPayloadAsString());
                    }
                    messageCount++;
                } else {
                    Thread.onSpinWait();
                }
            }
        }
    }
}
