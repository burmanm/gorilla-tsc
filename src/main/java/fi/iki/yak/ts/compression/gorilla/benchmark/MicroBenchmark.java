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

    @State(Scope.Benchmark)
    public static class DataGenerator {
        public long[] masks;
        public boolean[] bits;
        public byte[][] byteBits;
        public int[] intBits;
        public long[] longBits;
        public byte[] bytes;

        @Setup(Level.Trial)
        public void setup() {
            masks = new long[8];
            bits = new boolean[8];
            byteBits = new byte[2][8];
            intBits = new int[8];
            bytes = new byte[8];
            longBits = new long[8];
            for(int i = 0; i < bits.length; i++) {
                masks[i] = (1 << (i - 1));
                bits[i] = (i % 2 == 0);
                intBits[i] = (i % 2);
                byteBits[1][i] = (byte) (1 << (i - 1));
                byteBits[0][i] = 0;
                longBits[i] = Long.MAX_VALUE;
                bytes[i] = (byte) 0xFF;
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

    @Benchmark
    public void writeBit(DataGenerator dg, Blackhole bh) {
        byte b = 0;

        for(int i = 0; i < dg.bits.length; i++) {
            if(dg.bits[i]) b |= (1 << (i - 1));
        }
        bh.consume(b);
    }

    @Benchmark
    public void writeTargetBit(DataGenerator dg, Blackhole bh) {
        byte b = 0;

        for(int i = 0; i < dg.byteBits.length; i++) {
            b |= dg.byteBits[dg.intBits[i]][i];
        }
        bh.consume(b);
    }

    @Benchmark
    public void writeBranchlessBit(DataGenerator dg, Blackhole bh) {
        byte b = 0;
        int trueish = 1;

        for(int i = 0; i < dg.bits.length; i++) {
            b = (byte) ((b & ~(1 << i)) | (trueish << i));
//            b = (b & ~m) | (-f & m);
        }
        bh.consume(b);
    }



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

    @Benchmark
    public void getLong(DataGenerator dg, Blackhole bh) {
        long value = 0;
        int bits = 29;
        int i = 0;
        byte b = dg.bytes[i];
        int bitsLeft = 3;
        while(bits > 0) {
            if(bits > bitsLeft || bits == Byte.SIZE) {
                // Take only the bitsLeft "least significant" bits
                byte d = (byte) (b & ((1<<bitsLeft) - 1));
                value = (value << bitsLeft) + (d & 0xFF);
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                // Shift to correct position and take only least significant bits
                byte d = (byte) ((b >>> (bitsLeft - bits)) & ((1<<bits) - 1));
                value = (value << bits) + (d & 0xFF);
                bitsLeft -= bits;
                bits = 0;
            }
            if(bitsLeft == 0) {
                b = dg.bytes[++i];
                bitsLeft = 8;
            }
        }
        bh.consume(value);
    }

    @Benchmark
    public void targetGetLong(DataGenerator dg, Blackhole bh) {
        long value = 0;
        int i = 0;
        byte b = dg.bytes[i];
        int bitsLeft = 3;
        byte d = (byte) (b & ((1<<bitsLeft) - 1));
        value = (value << bitsLeft) + (d & 0xFF);
        b = dg.bytes[++i];
        for(int j = 0; j < 3; j++) {
            value = (value << 8) + b;
            b = dg.bytes[++i];
        }
        d = (byte) ((b >>> 6) & ((1<<2) - 1));
        value = (value << 2) + (d & 0xFF);
        bh.consume(value);
    }

    @Benchmark
    public void targetGetLongFromLong(DataGenerator dg, Blackhole bh) {
        long value = 0;
        int i = 0;
        byte b = dg.bytes[i];
        int bitsLeft = 3;
        byte d = (byte) (b & ((1<<bitsLeft) - 1));
        value = (value << bitsLeft) + (d & 0xFF);
        b = dg.bytes[++i];
        for(int j = 0; j < 3; j++) {
            value = (value << 8) + b;
            b = dg.bytes[++i];
        }
        d = (byte) ((b >>> 6) & ((1<<2) - 1));
        value = (value << 2) + (d & 0xFF);
        bh.consume(value);
    }

//
//    @Benchmark
//    public void writeBits(DataGenerator dg, Blackhole bh) {
//        int bits = 29;
//        long value = 3457457472L;
//        byte[] bytes = new byte[8];
//        int bitsLeft = 3;
//        int i = 0;
//        byte b = 0;
//        while(bits > 0) {
//            int bitsToWrite = (bits > bitsLeft) ? bitsLeft : bits;
//            if(bits > bitsLeft) {
//                int shift = bits - bitsLeft;
//                b |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
//            } else {
//                int shift = bitsLeft - bits;
//                b |= (byte) (value << shift);
//            }
//            bits -= bitsToWrite;
//            bitsLeft -= bitsToWrite;
//            if(bitsLeft == 0) {
//                bytes[++i] = b;
//                b = 0;
//                bitsLeft = 8;
//            }
//        }
//        bh.consume(bytes);
//    }
}
