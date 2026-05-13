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
 * Standalone writer process for true 2-process IPC demo.
 *
 * Run this in terminal 1:  ./gradlew runWriter
 * Run SeparateProcessReader in terminal 2: ./gradlew runReader
 *
 * This is the simplest possible cross-process communication:
 * Writer increments a counter and stores it with RELEASE semantics.
 * Reader polls with ACQUIRE semantics and prints when it changes.
 *
 * No ring buffer, no message framing — just a single shared long.
 * Good for verifying that mmap IPC actually works between two JVMs
 * before building the full SPSC queue on top of it.
 */
public class SeparateProcessWriter {

    private static final Path FILE = Path.of("/tmp/ipc-simple-counter.dat");
    private static final VarHandle LONG_VIEW = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.nativeOrder());

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("[Writer] Opening shared file: " + FILE);
        System.out.println("[Writer] Start the Reader in another terminal to see the values.");
        System.out.println("[Writer] Press Ctrl+C to stop.");
        System.out.println();

        try (FileChannel ch = FileChannel.open(FILE,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, 64);
            buf.order(ByteOrder.nativeOrder());

            long counter = 0;
            while (true) {
                // Store with RELEASE — guarantees the reader (via ACQUIRE)
                // will see this value only after all previous writes complete.
                LONG_VIEW.setRelease(buf, 0, counter);

                if (counter % 1000 == 0) {
                    System.out.printf("[Writer] counter = %,d%n", counter);
                }

                counter++;
                Thread.sleep(1); // ~1000 writes/sec for demo
            }
        }
    }
}
