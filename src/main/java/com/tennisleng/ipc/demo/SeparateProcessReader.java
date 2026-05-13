package com.tennisleng.ipc.demo;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Standalone reader process for true 2-process IPC demo.
 *
 * Run SeparateProcessWriter in terminal 1: ./gradlew runWriter
 * Run this in terminal 2: ./gradlew runReader
 *
 * Polls the shared file using ACQUIRE semantics. Prints whenever
 * the value changes. This proves that two separate JVM processes
 * can communicate through shared memory with zero sockets.
 */
public class SeparateProcessReader {

    private static final Path FILE = Path.of("/tmp/ipc-simple-counter.dat");
    private static final VarHandle LONG_VIEW = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.nativeOrder());

    public static void main(String[] args) throws IOException {
        System.out.println("[Reader] Opening shared file: " + FILE);
        System.out.println("[Reader] Waiting for Writer to start...");
        System.out.println();

        try (FileChannel ch = FileChannel.open(FILE,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, 64);
            buf.order(ByteOrder.nativeOrder());

            long lastSeen = -1;
            long messagesReceived = 0;

            while (true) {
                // ACQUIRE load — guarantees we see everything the writer
                // stored before the RELEASE that published this value.
                long current = (long) LONG_VIEW.getAcquire(buf, 0);

                if (current != lastSeen) {
                    messagesReceived++;
                    if (messagesReceived % 1000 == 0 || messagesReceived <= 5) {
                        System.out.printf("[Reader] counter = %,d  (received %,d updates)%n",
                                current, messagesReceived);
                    }
                    lastSeen = current;
                } else {
                    Thread.onSpinWait();
                }
            }
        }
    }
}
