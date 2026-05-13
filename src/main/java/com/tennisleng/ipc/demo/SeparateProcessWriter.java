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
 * The simplest possible cross-process demo: one long, shared via mmap.
 * Run this, then run SeparateProcessReader in another terminal.
 *
 *   ./gradlew runWriter
 */
public class SeparateProcessWriter {

    private static final Path FILE = Path.of("/tmp/ipc-simple-counter.dat");
    private static final VarHandle LONG_VH = MethodHandles.byteBufferViewVarHandle(
            long[].class, ByteOrder.nativeOrder());

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("[Writer] file: " + FILE);
        System.out.println("[Writer] start the Reader in another terminal. ctrl+c to stop.");

        try (FileChannel ch = FileChannel.open(FILE,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            MappedByteBuffer buf = ch.map(FileChannel.MapMode.READ_WRITE, 0, 64);
            buf.order(ByteOrder.nativeOrder());

            long counter = 0;
            while (true) {
                LONG_VH.setRelease(buf, 0, counter);
                if (counter % 1000 == 0) System.out.printf("[Writer] %,d%n", counter);
                counter++;
                Thread.sleep(1);
            }
        }
    }
}
