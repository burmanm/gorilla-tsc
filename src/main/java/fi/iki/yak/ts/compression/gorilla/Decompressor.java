package fi.iki.yak.ts.compression.gorilla;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Michael Burman
 */
public class Decompressor {

    private ByteBuffer bb;
    private byte b;
    private int bitsLeft = 0;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private double storedVal = 0;
    private long storedTimestamp = 0;
    private long storedDelta = 0;

    private long blockTimestamp = 0;

    // This could be in the same class as Compressor.. but .. meeh

    public static void main(String[] args) {
        try {
            // Small silly test.. will remove after I write JUnit tests
            long now = 1470424826100L;
            Compressor compressor = new Compressor(now);
            compressor.addValue(now + 10, 1.0);
            compressor.addValue(now + 20, 1.0);
            compressor.addValue(now + 31, 4.0);
            compressor.addValue(now + 42, 124.0);
            compressor.addValue(now + 50, 126.0);
            compressor.Close();

            Decompressor decompressor = new Decompressor(compressor.getByteBuffer().array());
            Pair pair = decompressor.readPair();
            System.out.println(pair.getTimestamp() + ";" + pair.getValue());
            pair = decompressor.readPair();
            System.out.println(pair.getTimestamp() + ";" + pair.getValue());
            pair = decompressor.readPair();
            System.out.println(pair.getTimestamp() + ";" + pair.getValue());
            pair = decompressor.readPair();
            System.out.println(pair.getTimestamp() + ";" + pair.getValue());
            pair = decompressor.readPair();
            System.out.println(pair.getTimestamp() + ";" + pair.getValue());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public Decompressor(byte[] data) throws IOException {
        bb = ByteBuffer.wrap(data);
        flipByte();
        readHeader();

        /*
        TODO: Iterator or io.Reader type of stuff? Same for Compressor, could it use writer or input function..? Or should those be external.
        TODO Allow pumping custom BitStream process, for example for external ByteBuffer / ByteArrayOutputStream handling
         */
    }

    private void readHeader() throws IOException {
        blockTimestamp = getLong(64);
    }

    private void flipByte() {
        if (bitsLeft == 0) {
            b = bb.get();
            bitsLeft = Byte.SIZE;
        }
    }

    public Pair readPair() throws IOException {

        if (storedTimestamp == 0) {
            // First item to read
            storedDelta = getLong(27);
            storedVal = Double.longBitsToDouble(getLong(64));
            storedTimestamp = blockTimestamp + storedDelta;

//            System.out.println("First value: timestamp->" + storedTimestamp + ", val->" + storedVal + ", delta->" + storedDelta + ", long->" + storedValLong);

        } else {
            // Next, read timestamp
            // TODO Read 4 bits and then use the extra bits for the next values
            long deltaDelta = 0;
            byte toRead = 0;
            if (readBit()) {
                if (!readBit()) {
                    toRead = 7; // '10'
                } else {
                    if (!readBit()) {
                        toRead = 9; // '110'
                    } else {
                        if (!readBit()) {
                            // 1110
                            toRead = 12;
                        } else {
                            // 1111
                            toRead = 32;
                        }
                    }
                }
            }
            if (toRead > 0) {
                deltaDelta = getLong(toRead);

                // Does not solve the issue either.. maybe I have something wrong in the compressor..
//                if(deltaDelta > (1 << (toRead - 1))) {
//                    deltaDelta = deltaDelta - (1 << toRead);
//                }
            }

            if (deltaDelta == 0xFFFFFFFF) {
                // End of stream
                return null;
            }

            // Negative values of deltaDelta are not handled correctly

            storedDelta = storedDelta + deltaDelta;
            System.out.printf("StoredDelta->%d, deltaDelta->%d\n", storedDelta, deltaDelta);
            storedTimestamp = storedDelta + storedTimestamp;

            // Read value
            if (readBit()) {
                // else -> same value as before
                if (readBit()) {
                    // New leading and trailing zeros
                    storedLeadingZeros = (int) getLong(5);

                    byte significantBits = (byte) getLong(6);
                    storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
                }
                long value = getLong(64 - storedLeadingZeros - storedTrailingZeros);
                value <<= storedTrailingZeros;
                value = Double.doubleToRawLongBits(storedVal) ^ value;
                storedVal = Double.longBitsToDouble(value);
            }
        }

        return new Pair(storedTimestamp, storedVal);
    }

    private boolean readBit() {
//        System.out.printf("readBit, bitsLeft->%d, b-> %8s\n", bitsLeft, Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
        byte bit = (byte) ((b >> (bitsLeft - 1)) & 1);

//        System.out.printf("readBit, bit-> %8s\n", Integer.toBinaryString((bit & 0xFF) + 0x100).substring(1));

        bitsLeft--;
        flipByte();
        return bit == 1;
    }

    private long getLong(int bits) {
        long value = 0;
        while(bits > 0) {
            if(bits > bitsLeft || bits == Byte.SIZE) {
                // Take only the bitsLeft "least significant" bits
                byte d = (byte) (b & ((1<<bitsLeft) - 1));
                value = (value << bitsLeft) + (d & 0xFF);
//                System.out.printf("getLong primary, b-> %8s\n", Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
//                System.out.printf("getLong primary, d-> %8s\n", Integer.toBinaryString((d & 0xFF) + 0x100).substring(1));
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                // TODO This isn't optimal - we need a range of bits
                for(; bits > 0; bits--) {
                    byte bit = (byte) ((b >> (bitsLeft - 1)) & 1);
                    bitsLeft--;
                    value = (value << 1) + (bit & 0xFF);
                }
            }
            flipByte();
        }

        return value;
    }
}