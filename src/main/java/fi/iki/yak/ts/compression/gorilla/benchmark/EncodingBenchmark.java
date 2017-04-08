package fi.iki.yak.ts.compression.gorilla.benchmark;

import fi.iki.yak.ts.compression.gorilla.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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

        public long[] uncompressedTimestamps;
        public long[] uncompressedValues;
        public double[] uncompressedDoubles;
        public long[] compressedArray;

        public ByteBuffer uncompressedBuffer;
        public ByteBuffer compressedBuffer;

        public List<Pair> pairs;

        @Setup(Level.Trial)
        public void setup() {
            blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                    .toInstant(ZoneOffset.UTC).toEpochMilli();

            long now = blockStart + 60;
            uncompressedTimestamps = new long[amountOfPoints];
            uncompressedDoubles = new double[amountOfPoints];
            uncompressedValues = new long[amountOfPoints];

            insertList = new ArrayList<>(amountOfPoints);

            ByteBuffer bb = ByteBuffer.allocate(amountOfPoints * 2*Long.BYTES);

            pairs = new ArrayList<>(amountOfPoints);

            for(int i = 0; i < amountOfPoints; i++) {
                now += 60;
                bb.putLong(now);
                bb.putDouble(i);
                uncompressedTimestamps[i] = now;
                uncompressedDoubles[i] = i;
                uncompressedValues[i] = i;
                pairs.add(new Pair(now, i));
//                bb.putLong(i);
            }

            if (bb.hasArray()) {
                uncompressedBuffer = bb.duplicate();
                uncompressedBuffer.flip();
            }
            ByteBufferBitOutput output = new ByteBufferBitOutput();
            LongArrayOutput arrayOutput = new LongArrayOutput(amountOfPoints);

            Compressor c = new Compressor(blockStart, output);
            GorillaCompressor gc = new GorillaCompressor(blockStart, arrayOutput);

            bb.flip();

            for(int j = 0; j < amountOfPoints; j++) {
//                c.addValue(bb.getLong(), bb.getLong());
                c.addValue(bb.getLong(), bb.getDouble());
                gc.addValue(uncompressedTimestamps[j], uncompressedDoubles[j]);
            }

            gc.close();
            c.close();

            ByteBuffer byteBuffer = output.getByteBuffer();
            byteBuffer.flip();
            compressedBuffer = byteBuffer;

            compressedArray = arrayOutput.getLongArray();
        }
    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingBenchmark(DataGenerator dg) {
        ByteBufferBitOutput output = new ByteBufferBitOutput();
        Compressor c = new Compressor(dg.blockStart, output);

        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedBuffer.getLong(), dg.uncompressedBuffer.getDouble());
        }
        c.close();
        dg.uncompressedBuffer.rewind();
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void decodingBenchmark(DataGenerator dg, Blackhole bh) throws Exception {
        ByteBuffer duplicate = dg.compressedBuffer.duplicate();
        ByteBufferBitInput input = new ByteBufferBitInput(duplicate);
        Decompressor d = new Decompressor(input);
        Pair pair;
        while((pair = d.readPair()) != null) {
            bh.consume(pair);
        }
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingGorillaBenchmark(DataGenerator dg) {
        LongArrayOutput output = new LongArrayOutput();
        GorillaCompressor c = new GorillaCompressor(dg.blockStart, output);

        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedTimestamps[j], dg.uncompressedDoubles[j]);
        }
        c.close();
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingGorillaBenchmarkLong(DataGenerator dg) {
        LongArrayOutput output = new LongArrayOutput();
        GorillaCompressor c = new GorillaCompressor(dg.blockStart, output);

        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedTimestamps[j], dg.uncompressedValues[j]);
        }
        c.close();
    }

//    @Benchmark
//    @OperationsPerInvocation(100000)
//    public void encodingGorillaStreamBenchmark(DataGenerator dg) {
//        LongArrayOutput output = new LongArrayOutput();
//        GorillaCompressor c = new GorillaCompressor(dg.blockStart, output);
//
//        c.compressLongStream(dg.pairs.stream());
//        c.close();
//    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void decodingGorillaBenchmark(DataGenerator dg, Blackhole bh) throws Exception {
        LongArrayInput input = new LongArrayInput(dg.compressedArray);
        GorillaDecompressor d = new GorillaDecompressor(input);
        Pair pair;
        while((pair = d.readPair()) != null) {
            bh.consume(pair);
        }
    }
}
