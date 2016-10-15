package fi.iki.yak.ts.compression.gorilla.benchmark;

import fi.iki.yak.ts.compression.gorilla.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Michael Burman
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10) // Reduce the amount of iterations if you start to see GC interference
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

        @Setup(Level.Trial)
        public void setup() {
            blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                    .toInstant(ZoneOffset.UTC).toEpochMilli();

            long now = blockStart + 60;

            insertList = new ArrayList<>(amountOfPoints);

            ByteBuffer bb = ByteBuffer.allocate(amountOfPoints * 2*Long.BYTES);

            uncompressedLongs = new long[amountOfPoints*2];
            uncompressedDoubles = new double[amountOfPoints];

            int j = 0;
            for(int i = 0; i < amountOfPoints; i++) {
                now += 60;
                bb.putLong(now);
                bb.putDouble(i);

                uncompressedLongs[j++] = now;
                uncompressedLongs[j++] = i;
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
    public void encodingBenchmarkBitOutputBufferInputProto(DataGenerator dg, Blackhole bh) {
        ByteOutputProto output = new ByteOutputProto();
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

    @Benchmark
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
        LongOutputProto output = new LongOutputProto();
        Compressor c = new Compressor(dg.blockStart, output);

        int i = 0;
        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedLongs[i++], dg.uncompressedDoubles[j]);
            i++;
        }
        c.close();
        bh.consume(output);
    }

    //
//    @Benchmark
//    @OperationsPerInvocation(100000)
//    public void decodingDoubleBenchmarkByteBufferBitInput(DataGenerator dg, Blackhole bh) throws Exception {
//        ByteBuffer duplicate = dg.compressedBuffer.duplicate();
//        ByteBufferBitInput input = new ByteBufferBitInput(duplicate);
//        Decompressor d = new Decompressor(input);
//        Pair pair;
//        while((pair = d.readPair()) != null) {
//            bh.consume(pair);
//        }
//    }
}
