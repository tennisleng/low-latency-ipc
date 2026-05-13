package com.tennisleng.ipc;

import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * JMH: SPSC ring buffer vs standard java queues.
 *
 * What matters here isn't the average — it's the tail. Look at the SampleTime
 * results: SPSC should be flat at p99/p999 while LBQ gets destroyed by GC pauses.
 *
 *   ./gradlew jmh
 */
public class SPSCBenchmark {

    @State(Scope.Thread)
    public static class SPSCState {
        SPSCRingBuffer ring;
        MessageView view;
        byte[] payload;
        Path file;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            file = Files.createTempFile("spsc-bench-", ".dat");
            ring = new SPSCRingBuffer(file, 1024, 64);
            ring.reset();
            view = new MessageView();
            payload = "benchmark-payload-32bytes-xx".getBytes(StandardCharsets.UTF_8);
        }

        @TearDown(Level.Trial)
        public void teardown() throws IOException {
            ring.close();
            Files.deleteIfExists(file);
        }
    }

    @State(Scope.Thread)
    public static class LBQState {
        LinkedBlockingQueue<byte[]> q;
        byte[] payload;

        @Setup(Level.Trial)
        public void setup() {
            q = new LinkedBlockingQueue<>(1024);
            payload = "benchmark-payload-32bytes-xx".getBytes(StandardCharsets.UTF_8);
        }
    }

    @State(Scope.Thread)
    public static class ABQState {
        ArrayBlockingQueue<byte[]> q;
        byte[] payload;

        @Setup(Level.Trial)
        public void setup() {
            q = new ArrayBlockingQueue<>(1024);
            payload = "benchmark-payload-32bytes-xx".getBytes(StandardCharsets.UTF_8);
        }
    }

    // --- latency (avg) ---

    @Benchmark
    @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1) @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean spsc_avg(SPSCState s) {
        s.ring.write(s.payload);
        return s.ring.read(s.view);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1) @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean lbq_avg(LBQState s) throws InterruptedException {
        s.q.put(s.payload.clone());
        return s.q.take() != null;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1) @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean abq_avg(ABQState s) throws InterruptedException {
        s.q.put(s.payload.clone());
        return s.q.take() != null;
    }

    // --- throughput ---

    @Benchmark
    @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1) @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean spsc_throughput(SPSCState s) {
        s.ring.write(s.payload);
        return s.ring.read(s.view);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1) @Measurement(iterations = 5, time = 2)
    @Fork(1)
    public boolean lbq_throughput(LBQState s) throws InterruptedException {
        s.q.put(s.payload.clone());
        return s.q.take() != null;
    }

    // --- percentiles (this is where you see GC spikes) ---

    @Benchmark
    @BenchmarkMode(Mode.SampleTime) @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1) @Measurement(iterations = 5, time = 3)
    @Fork(1)
    public boolean spsc_pct(SPSCState s) {
        s.ring.write(s.payload);
        return s.ring.read(s.view);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime) @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1) @Measurement(iterations = 5, time = 3)
    @Fork(1)
    public boolean lbq_pct(LBQState s) throws InterruptedException {
        s.q.put(s.payload.clone());
        return s.q.take() != null;
    }
}
