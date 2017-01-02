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
public class MicroBenchmark {

    public final static long[] BIT_MASKS;

    static {
        BIT_MASKS = new long[64];
        for(int i = 0; i < BIT_MASKS.length; i++) {
            BIT_MASKS[i] = (1L << i);
        }
    }

    @State(Scope.Benchmark)
    public static class DataGenerator {
        public long[] masks;
        public boolean[] bits;
        public byte[][] byteBits;
        public int[] intBits;
        public long[] longBits;
        public byte[] bytes;
        public byte[] BITMASKS;
        public long[] values;
        public int[] necessaryBits;
        public int[] bitsLeft;

        @Setup(Level.Trial)
        public void setup() {
            BITMASKS = new byte[8];
            masks = new long[8];
            bits = new boolean[8];
            byteBits = new byte[2][8];
            intBits = new int[8];
            bytes = new byte[8];
            longBits = new long[8];
            values = new long[8];
            bitsLeft = new int[8];
            necessaryBits = new int[8];
            for(int i = 0; i < bits.length; i++) {
                masks[i] = (1 << (i - 1));
                bits[i] = (i % 2 == 0);
                intBits[i] = (i % 2);
                bitsLeft[i] = 5;
                necessaryBits[i] = (int) Math.pow(i, i);
                byteBits[1][i] = (byte) (1 << (i - 1));
                byteBits[0][i] = 0;
                longBits[i] = Long.MAX_VALUE;
                bytes[i] = (byte) 0xFF;
                BITMASKS[i] = (byte) ((1 << i) - 1);
                values[i] = ThreadLocalRandom.current().nextInt(1, 268435456);
            }
        }
    }

//    @Benchmark
//    public void tableLookup(DataGenerator dg, Blackhole bh) {
//        long[] a = new long[8];
//        for(int i = 0; i < 8; i++) {
//            a[i] = dg.masks[i];
//        }
//        bh.consume(a);
//    }
//
//    @Benchmark
//    public void reCalculate(Blackhole bh) {
//        long[] a = new long[8];
//        for(int i = 0; i < 8; i++) {
//            a[i] = 1 << (i - 1);
//        }
//        bh.consume(a);
//    }

//    @Benchmark
    public void writeBit(DataGenerator dg, Blackhole bh) {
        long b = 0;

        for(int i = 0; i < dg.bits.length; i++) {
//            if(dg.bits[i]) b |= (1 << (i - 1));
//            (1L << (bitsLeft - 1));
            b |= (1L << i);

        }
        bh.consume(b);
    }

//    @Benchmark
    public void writeLookupBit(DataGenerator dg, Blackhole bh) {
        long b = 0;

        for(int i = 0; i < dg.byteBits.length; i++) {
            b |= BIT_MASKS[i];
//            b |= dg.byteBits[dg.intBits[i]][i];
        }
        bh.consume(b);
    }
//
//    @Benchmark
//    public void writeAlwaysIntBit(DataGenerator dg, Blackhole bh) {
//        byte b = 0;
//
//        for(int i = 0; i < dg.bits.length; i++) {
//            b |= (dg.intBits[i] << (i - 1));
//        }
//        bh.consume(b);
//    }


    // These were optimized away .. meh.

//    @Benchmark
//    public void readControlBits(DataGenerator dg, Blackhole bh) {
//        byte b = 0x02;
//        int val = 0x00;
//
//        for(int i = 0; i < 2; i++) {
//            val <<= 1;
//            boolean bit = ((b >> (i - 1)) & 1) == 1;
//            if(bit) {
//                val |= 0x01;
//            } else {
//                break;
//            }
//        }
//        bh.consume(val);
//    }
//
//    @Benchmark
//    public void readControlIntBits(DataGenerator dg, Blackhole bh) {
//        byte b = 0x0F;
//        int val = 0x00;
//
//        for(int i = 0; i < 4; i++) {
//            val <<= 1;
//            int value = ((b >> (i - 1)) & 1); // this is slow
//            if((0 - value) >>> (Integer.SIZE - 1) == 1) {
//                val |= 0x01;
//            } else {
//                break;
//            }
//        }
//        bh.consume(val);
//    }

//    @Benchmark
    public void writeBitsRealityProto2(DataGenerator dg, Blackhole bh) {
        int bits = 29;
        long value;
        byte[] bytes = new byte[8*8];

        for(int vals = 0; vals < dg.values.length; vals++) {
            value = dg.values[vals];
            int bitsLeft = 3;
            int i = 0;
            byte b = 0;
            if(bits > bitsLeft) {
                b |= (byte) ((value >> 26) & dg.BITMASKS[3]);
                bytes[i++] = b;
                bytes[i++] = (byte) ((value) >> 18);
                bytes[i++] = (byte) ((value) >> 10);
                bytes[i++] = (byte) ((value) >> 2);
            }
            b |= (byte) (value << 6);
            bytes[i++] = b;
            bh.consume(bitsLeft);
        }

        bh.consume(bytes);
    }

    @Benchmark
    public void neededBitsInteger(DataGenerator dg, Blackhole bh) {
        for(int vals = 0; vals < dg.necessaryBits.length; vals++) {
            int bitsRequired = Integer.highestOneBit(vals);
            bh.consume(bitsRequired);
        }
    }

    @Benchmark
    public void neededBitsInteger2(DataGenerator dg, Blackhole bh) {
        for(int vals = 0; vals < dg.necessaryBits.length; vals++) {
            int bitsRequired = 32 - Integer.numberOfLeadingZeros(vals);
            bh.consume(bitsRequired);
        }
    }
}
