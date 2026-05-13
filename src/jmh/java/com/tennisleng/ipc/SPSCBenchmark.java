package com.tennisleng.ipc;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmark: SPSC Ring Buffer vs standard Java queues.
 *
 * Run:  ./gradlew jmh
 *
 * What to look for in the results:
 *
 *   1. AVERAGE latency: spsc should beat both queue implementations
 *   2. SAMPLE mode (percentiles): spsc should have flat p99/p999
 *      while the queues show "spikes" from GC pauses
 *   3. THROUGHPUT: spsc should sustain higher ops/sec
 *
 * The "proof" that your design works is in the tail latencies.
 * A system that averages 200ns but spikes to 10ms at p999 is
 * WORSE for trading than one that averages 500ns but never exceeds 2µs.
 */
public class SPSCBenchmark {

    // ═══════════════════════════════════════════════════════════════════
    //  SHARED STATE
    // ═══════════════════════════════════════════════════════════════════

    @State(Scope.Thread)
    public static class SPSCState {
        SPSCRingBuffer ringBuffer;
        MessageView view;
        byte[] payload;
        Path tempFile;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            tempFile = Files.createTempFile("spsc-bench-", ".dat");
            ringBuffer = new SPSCRingBuffer(tempFile, 1024, 64);
            ringBuffer.reset();
            view = new MessageView();
            payload = "benchmark-payload-32b-xxxxx".getBytes(StandardCharsets.UTF_8);
        }

        @TearDown(Level.Trial)
        public void teardown() throws IOException {
            ringBuffer.close();
            Files.deleteIfExists(tempFile);
        }
    }

    @State(Scope.Thread)
    public static class LBQState {
        LinkedBlockingQueue<byte[]> queue;
        byte[] payload;

        @Setup(Level.Trial)
        public void setup() {
            queue = new LinkedBlockingQueue<>(1024);
            payload = "benchmark-payload-32b-xxxxx".getBytes(StandardCharsets.UTF_8);
        }
    }

    @State(Scope.Thread)
    public static class ABQState {
        ArrayBlockingQueue<byte[]> queue;
        byte[] payload;

        @Setup(Level.Trial)
        public void setup() {
            queue = new ArrayBlockingQueue<>(1024);
            payload = "benchmark-payload-32b-xxxxx".getBytes(StandardCharsets.UTF_8);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  LATENCY BENCHMARKS (write + read = one round trip)
    // ═══════════════════════════════════════════════════════════════════

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean spsc_roundtrip_avg(SPSCState state) {
        state.ringBuffer.write(state.payload);
        return state.ringBuffer.read(state.view);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean lbq_roundtrip_avg(LBQState state) throws InterruptedException {
        state.queue.put(state.payload.clone()); // clone = real-world alloc
        byte[] msg = state.queue.take();
        return msg != null;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean abq_roundtrip_avg(ABQState state) throws InterruptedException {
        state.queue.put(state.payload.clone());
        byte[] msg = state.queue.take();
        return msg != null;
    }

    // ═══════════════════════════════════════════════════════════════════
    //  THROUGHPUT BENCHMARKS
    // ═══════════════════════════════════════════════════════════════════

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean spsc_roundtrip_throughput(SPSCState state) {
        state.ringBuffer.write(state.payload);
        return state.ringBuffer.read(state.view);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean lbq_roundtrip_throughput(LBQState state) throws InterruptedException {
        state.queue.put(state.payload.clone());
        byte[] msg = state.queue.take();
        return msg != null;
    }

    // ═══════════════════════════════════════════════════════════════════
    //  SAMPLE MODE (shows percentile distribution — GC spikes visible)
    // ═══════════════════════════════════════════════════════════════════

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 3)
    @Fork(1)
    public boolean spsc_roundtrip_percentiles(SPSCState state) {
        state.ringBuffer.write(state.payload);
        return state.ringBuffer.read(state.view);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 3)
    @Fork(1)
    public boolean lbq_roundtrip_percentiles(LBQState state) throws InterruptedException {
        state.queue.put(state.payload.clone());
        byte[] msg = state.queue.take();
        return msg != null;
    }
}
