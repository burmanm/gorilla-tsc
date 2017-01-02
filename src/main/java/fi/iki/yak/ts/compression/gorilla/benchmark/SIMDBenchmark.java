/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fi.iki.yak.ts.compression.gorilla.benchmark;

import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * @author michael
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5) // Reduce the amount of iterations if you start to see GC interference
public class SIMDBenchmark {

    @State(Scope.Benchmark)
    public static class DataGenerator {
        public int[] deltas;
        public int[] timestamps;
        public long[] ts;
        public int[] dods;

        public int blockStart = 1476792700;
        public long blockTimestamp = System.currentTimeMillis();

        @Setup(Level.Trial)
        public void setup() {
            timestamps = new int[32];

            deltas = new int[32];
            dods = new int[32];
            ts = new long[32];
            for(int i = 0; i < deltas.length; i++) {
                deltas[i] = ThreadLocalRandom.current().nextInt(960, 1200);
            }
            timestamps[0] = blockStart + deltas[0];
            ts[0] = blockStart + deltas[0];
            for(int j = 1; j < timestamps.length; j++) {
                timestamps[j] = timestamps[j - 1] + deltas[j];
                ts[j] = ts[j - 1] + deltas[j];
            }

        }
    }

    public static int fastinverseDelta(int[] data, int start, int length,
                                       int init) {
        data[start] += init;
        int sz0 = length / 4 * 4;
        int i = 1;
        if (sz0 >= 4) {
            int a = data[start];
            for (; i < sz0 - 4; i += 4) {
                a = data[start + i] += a;
                a = data[start + i + 1] += a;
                a = data[start + i + 2] += a;
                a = data[start + i + 3] += a;
            }
        }

        for (; i != length; ++i) {
            data[start + i] += data[start + i - 1];
        }
        return data[start + length - 1];
    }

    public static void inverseDelta(int[] data) {
        for (int i = 1; i < data.length; ++i) {
            data[i] += data[i - 1];
        }
    }

    public static int inverseDoD(int[] dods, int firstDelta, int blockStart) {

        int storedDelta = firstDelta;
        int storedTimestamp = blockStart;

        for(int i = 0; i < dods.length; i++) {
            storedDelta += dods[i];
            storedTimestamp += storedDelta;
            dods[i] = storedTimestamp;
        }

        return dods[31];

    }

//    public static void simdDod(int[] dods, int firstDelta, int blockStart) {
//        int storedDelta = firstDelta;
//        int storedTimestamp = blockStart;
//
//        int sz0 = length / 4 * 4;
//        int a = blockStart + firstDelta;
//        for (int i = 1; i < sz0 - 4; i+= 4) {
//            dods[i + 1] += a;
//        }
//
//    }

    public static void deltaTwice(int[] timestamps, int firstDelta, long firstTimestamp) {
        inverseDelta(timestamps);
        inverseDelta(timestamps);
    }

    public static void naiveCalculateDoDInplace(int[] timestamps, int firstDelta, int firstTimestamp) {
        int storedTimestamp = firstTimestamp;
        int storedDelta = firstDelta;

        for(int i = 0; i < timestamps.length; i++) {
            int newDelta = timestamps[i] - storedTimestamp;
            storedTimestamp = timestamps[i];
            timestamps[i] = newDelta - storedDelta;
        }
    }

//    @Benchmark
    public void singleXor(DataGenerator dg, Blackhole bh) { // 108M xors / s
        int[] zeros = new int[2];
        long[] xors = new long[1];
        xors[0] = dg.ts[0] ^ dg.ts[1];
        zeros[0] = Long.numberOfLeadingZeros(xors[0]);
        zeros[1] = Long.numberOfTrailingZeros(xors[0]);
        bh.consume(xors);
        bh.consume(zeros);
    }

//    @Benchmark
    public void fourXors(DataGenerator dg, Blackhole bh) { // 68M/s -> 272M xors / s
        int[] zeros = new int[8];
        long[] xors = new long[4];
        int z = 0;
        for(int i = 0; i < 4; i++) {
            xors[i] = dg.ts[i] ^ dg.ts[i+1];
            zeros[z++] = Long.numberOfLeadingZeros(xors[i]);
            zeros[z++] = Long.numberOfTrailingZeros(xors[i]);
        }
        bh.consume(zeros);
        bh.consume(xors);
    }

//    @Benchmark
//    public void fourTwoLoopXors(DataGenerator dg, Blackhole bh) {
//        int[] zeros = new int[8];
//        long[] xors = new long[4];
//        int z = 0;
//        for(int i = 0; i < 4; i++) {
//            xors[i] = dg.ts[i] ^ dg.ts[i+1];
//        }
//        for(int i = 0; i < 4; i++) {
//            zeros[z++] = Long.numberOfLeadingZeros(xors[i]);
//            zeros[z++] = Long.numberOfTrailingZeros(xors[i]);
//        }
//        bh.consume(zeros);
//        bh.consume(xors);
//    }


//    @Benchmark
    public void fastInverseDelta(DataGenerator dg, Blackhole bh) {
        int[] values = new int[32];
        System.arraycopy(dg.deltas, 0, values, 0, 32);
        fastinverseDelta(values, 0, 32, 1476792775);
        bh.consume(values);
    }

//    @Benchmark
    public void inverseDelta(DataGenerator dg, Blackhole bh) {
        int[] values = new int[32];
        System.arraycopy(dg.deltas, 0, values, 0, 32);
        inverseDelta(values);
        bh.consume(values);
    }

//    @Benchmark
    public void naiveDoD(DataGenerator dg, Blackhole bh) {
        int[] values = new int[32];
        System.arraycopy(dg.timestamps, 0, values, 0, 32);
        naiveCalculateDoDInplace(values, dg.deltas[0], dg.blockStart);
        bh.consume(values);
    }

//    @Benchmark
    public void naiveExtractDods(DataGenerator dg, Blackhole bh) {
        int[] values = new int[32];
        System.arraycopy(dg.dods, 0, values, 0, 32);
        inverseDoD(values, dg.deltas[0], dg.blockStart);
        bh.consume(values);
    }
}
