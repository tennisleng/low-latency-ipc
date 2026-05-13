package com.tennisleng.ipc;

import org.openjdk.jmh.annotations.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmark: SPSC Ring Buffer vs LinkedBlockingQueue
 *
 * Run:  ./gradlew jmh
 *
 * What to look for in the results:
 *   - spscWrite:  should show FLAT, consistent latency (no GC spikes)
 *   - lbqWrite:   will show periodic SPIKES from GC pauses
 *
 * The "proof" that your design works is in the p99/p999 percentiles.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Thread)
public class SPSCBenchmark {

    private SPSCRingBuffer ringBuffer;
    private MessageView view;
    private byte[] payload;
    private Path tempFile;

    // Baseline comparison
    private LinkedBlockingQueue<byte[]> lbq;

    @Setup
    public void setup() throws IOException {
        tempFile = Files.createTempFile("spsc-bench-", ".dat");
        ringBuffer = new SPSCRingBuffer(tempFile, 1024, 64);
        ringBuffer.reset();
        view = new MessageView();
        payload = "benchmark-payload".getBytes(StandardCharsets.UTF_8);
        lbq = new LinkedBlockingQueue<>(1024);
    }

    @TearDown
    public void teardown() throws IOException {
        ringBuffer.close();
        Files.deleteIfExists(tempFile);
    }

    @Benchmark
    public boolean spscWriteRead() {
        ringBuffer.write(payload);
        return ringBuffer.read(view);
    }

    @Benchmark
    public boolean lbqWriteRead() throws InterruptedException {
        lbq.put(payload.clone()); // clone simulates real-world allocation
        byte[] msg = lbq.take();
        return msg != null;
    }
}
