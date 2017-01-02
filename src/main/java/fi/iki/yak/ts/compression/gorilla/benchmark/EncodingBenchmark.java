package fi.iki.yak.ts.compression.gorilla.benchmark;

import fi.iki.yak.ts.compression.gorilla.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Michael Burman
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5) // Reduce the amount of iterations if you start to see GC interference
public class EncodingBenchmark {

    @State(Scope.Benchmark)
    public static class DataGenerator {
        public List<Pair> insertList;

        @Param({"100000"})
        public int amountOfPoints;

        public long blockStart;

        public ByteBuffer uncompressedBuffer;
        public ByteBuffer compressedBuffer;

        public long[] uncompressedLongs;
        public double[] uncompressedDoubles;

        public IntOutputProto output;
        public LongOutputProto longOutput;
        public ByteOutputProto byteOutput;

        @Setup(Level.Trial)
        public void setup() {
            blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                    .toInstant(ZoneOffset.UTC).toEpochMilli();

            output = new IntOutputProto();
            longOutput = new LongOutputProto();
            byteOutput = new ByteOutputProto();

            long now = blockStart + 60;

            insertList = new ArrayList<>(amountOfPoints);

            ByteBuffer bb = ByteBuffer.allocate(amountOfPoints * 2*Long.BYTES);

            uncompressedLongs = new long[amountOfPoints*2];
            uncompressedDoubles = new double[amountOfPoints];

            int j = 0;
            for(int i = 0; i < amountOfPoints; i++) {
//                now += 1000 - ((i % 5) * 10);
                now += 60;

                bb.putLong(now);
                bb.putDouble(i);

                uncompressedLongs[j++] = now;
//                uncompressedLongs[j++] = i + ThreadLocalRandom.current().nextInt(0, 3); // Not very stable benchmark..
                uncompressedLongs[j++] = i; // Not very stable benchmark..
                uncompressedDoubles[i] = i;

//                bb.putLong(i);
            }

            if (bb.hasArray()) {
                uncompressedBuffer = bb.duplicate();
                uncompressedBuffer.flip();
            }
            ByteBufferBitOutput output = new ByteBufferBitOutput();

            Compressor c = new Compressor(blockStart, output);

            bb.flip();

            for(int k = 0; k < amountOfPoints; k++) {
//                c.addValue(bb.getLong(), bb.getLong());
                c.addValue(bb.getLong(), bb.getDouble());
            }

            c.close();

            ByteBuffer byteBuffer = output.getByteBuffer();
            byteBuffer.flip();
            compressedBuffer = byteBuffer;
        }
    }

//    @Benchmark
//    @OperationsPerInvocation(100000)
//    public void encodingBenchmarkByteBufferBitOutput(DataGenerator dg) {
//        ByteBufferBitOutput output = new ByteBufferBitOutput();
//        Compressor c = new Compressor(dg.blockStart, output);
//
//        for(int j = 0; j < dg.amountOfPoints; j++) {
//            c.addValue(dg.uncompressedBuffer.getLong(), dg.uncompressedBuffer.getDouble());
//        }
//        c.close();
//        dg.uncompressedBuffer.rewind();
//    }

//    @Benchmark
//    @OperationsPerInvocation(100000)
//    public void encodingBenchmarkByteBufferBitOutputLong(DataGenerator dg) {
//        ByteBufferBitOutput output = new ByteBufferBitOutput();
//        Compressor c = new Compressor(dg.blockStart, output);
//
//        int i = 0;
//        for(int j = 0; j < dg.amountOfPoints; j++) {
//            c.addValue(dg.uncompressedBuffer.getLong(), dg.uncompressedBuffer.getLong());
//        }
//        c.close();
//    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingBenchmarkByteBufferBitOutputProto(DataGenerator dg, Blackhole bh) {
//        for(;;) {
            ByteBufferBitOutputProto output = new ByteBufferBitOutputProto();
            Compressor c = new Compressor(dg.blockStart, output);

            for(int j = 0; j < dg.amountOfPoints; j++) {
                c.addValue(dg.uncompressedBuffer.getLong(), dg.uncompressedBuffer.getDouble());
            }
            c.close();
            dg.uncompressedBuffer.rewind();
        bh.consume(output);
//        }
    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingBenchmarkByteOutputProto(DataGenerator dg, Blackhole bh) {
        ByteOutputProto output = dg.byteOutput;
        output.reset();

        Compressor c = new Compressor(dg.blockStart, output);

        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedBuffer.getLong(), dg.uncompressedBuffer.getDouble());
        }
        c.close();
        dg.uncompressedBuffer.rewind();
        bh.consume(output);
    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingBenchmarkByteOutputProtoArray(DataGenerator dg, Blackhole bh) {
        ByteOutputProto output = new ByteOutputProto();
        Compressor c = new Compressor(dg.blockStart, output);

        int i = 0;
        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedLongs[i++], dg.uncompressedDoubles[j]);
            i++;
        }
        c.close();
        bh.consume(output);
    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingBenchmarkIntOutputProtoArray(DataGenerator dg, Blackhole bh) {
        IntOutputProto output = new IntOutputProto();
        Compressor c = new Compressor(dg.blockStart, output);

        int i = 0;
        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedLongs[i++], dg.uncompressedDoubles[j]);
            i++;
        }
        c.close();
        bh.consume(output);
    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingBenchmarkLongOutputProtoArray(DataGenerator dg, Blackhole bh) {
        ByteOutputProto output = dg.byteOutput;
        output.reset();
        Compressor c = new Compressor(dg.blockStart, output);

        int i = 0;
        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedLongs[i++], dg.uncompressedDoubles[j]);
            i++;
        }
        c.close();
        bh.consume(output);
    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodeDecodeBenchmark(DataGenerator dg, Blackhole bh) {
        LongOutputProto output = dg.longOutput;
        output.reset();
        Compressor2 c = new Compressor2(dg.blockStart, output);

        int i = 0;
        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedLongs[i++], dg.uncompressedDoubles[j]);
            i++;
        }
        c.close();


//        Decompressor2 d = new Decompressor2()

        bh.consume(output);
    }

    //    @Benchmark
//    @Fork(jvmArgsAppend =
//            {"-XX:+UnlockDiagnosticVMOptions",
//                    "-XX:PrintAssemblyOptions=intel",
//                    "-XX:CompileCommand=print,*ValueCompressor.*"
//            })
    @OperationsPerInvocation(45000)
    public void valueCompress(DataGenerator dg, Blackhole bh) {
        BitOutput output = new BitOutput() {
            @Override public void writeBit(boolean bit) {

            }

            @Override public void writeBits(long value, int bits) {

            }

            @Override public void flush() {

            }
        };
//        IntOutputProto output = dg.output;
//        output.reset();
//        IntOutputProto output = new IntOutputProto();
        ValueCompressor vc = new ValueCompressor(output);
        for(int i = 0; i < 45000; i++) {
            vc.compressValue(dg.uncompressedLongs[i++]);
        }
        bh.consume(vc);
    }

//    @Benchmark
//    @OperationsPerInvocation(45000)
//    public void valueCompressLong(DataGenerator dg, Blackhole bh) {
//        LongOutputProto output = dg.longOutput;
//        output.reset();
//        ValueCompressor vc = new ValueCompressor(output);
//        for(int i = 0; i < 45000; i++) {
//            vc.compressValue(dg.uncompressedLongs[i++]);
//        }
//        bh.consume(vc);
//    }


//    @Benchmark
    @OperationsPerInvocation(45000)
    public void valueCompressProto(DataGenerator dg, Blackhole bh) {
//        IntOutputProto output = dg.output;
//        output.reset();

        BitOutput output = new BitOutput() {
            @Override public void writeBit(boolean bit) {

            }

            @Override public void writeBits(long value, int bits) {

            }

            @Override public void flush() {

            }
        };

        ValueCompressorProto vc = new ValueCompressorProto(output);
        for(int i = 0; i < 45000; i++) {
            vc.compressValue(dg.uncompressedLongs[i++]);
        }
        bh.consume(vc);
    }

    //
//    @Benchmark
    @OperationsPerInvocation(100000)
    public void decodingDoubleBenchmarkByteBufferBitInput(DataGenerator dg, Blackhole bh) throws Exception {
        ByteBuffer duplicate = dg.compressedBuffer.duplicate();
        ByteBufferBitInput input = new ByteBufferBitInput(duplicate);
        Decompressor d = new Decompressor(input);
        Pair pair;
        while((pair = d.readPair()) != null) {
            bh.consume(pair);
        }
    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void decodingDoubleBenchmarkByteBufferBitInput2(DataGenerator dg, Blackhole bh) throws Exception {
        ByteBuffer duplicate = dg.compressedBuffer.duplicate();
        ByteBufferBitInput input = new ByteBufferBitInput(duplicate);
        Decompressor2 d = new Decompressor2(input);
        Pair pair;
        for(int i = 0; i < 100000; i++) {
            pair = d.readPair();
            bh.consume(pair);
        }
    }

//    @Benchmark
    @OperationsPerInvocation(22500)
    public void timestampCompress(DataGenerator dg, Blackhole bh) {
        LongOutputProto output = dg.longOutput;
        output.reset();
        TimestampCompressor vc = new TimestampCompressor(dg.blockStart, dg.uncompressedLongs[0], output);
        for(int i = 2; i < 45000; i++) {
            vc.compressTimestamp(dg.uncompressedLongs[i++]);
        }
        bh.consume(vc);
    }

//    @Benchmark
    @OperationsPerInvocation(22500)
    public void timestampCompressProto(DataGenerator dg, Blackhole bh) {
        LongOutputProto output = dg.longOutput;
        output.reset();
        TimestampCompressorProto vc = new TimestampCompressorProto(dg.blockStart, dg.uncompressedLongs[0], output);
        for(int i = 2; i < 45000; i++) {
            vc.compressTimestamp(dg.uncompressedLongs[i++]);
        }
        bh.consume(vc);
    }

//    @Benchmark
//    @OperationsPerInvocation(22500)
//    public void timestampCompressProto2(DataGenerator dg, Blackhole bh) {
//        TimestampCompressorProto vc = new TimestampCompressorProto(dg.blockStart, dg.uncompressedLongs[0]);
//        for(int i = 2; i < 45000; i++) {
//            vc.compressTimestamp2(dg.uncompressedLongs[i++]);
//        }
//        bh.consume(vc);
//    }
}
