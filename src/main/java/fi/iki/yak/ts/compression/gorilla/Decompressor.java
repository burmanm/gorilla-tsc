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

    public Decompressor(byte[] data) throws IOException {
        bb = ByteBuffer.wrap(data);
        flipByte();
        readHeader();

        /*
        TODO: Implement SplitIterator / create RxJava bindings?
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
            storedDelta = getLong(Compressor.FIRST_DELTA_BITS);
            storedVal = Double.longBitsToDouble(getLong(64));
            storedTimestamp = blockTimestamp + storedDelta;
        } else {
            // Next, read timestamp
            // TODO Read 4 bits and then return the unused extra bits for the next values.. or something
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

                if(toRead == 32) {
                    if ((int) deltaDelta == 0xFFFFFFFF) {
                        // End of stream
                        return null;
                    }
                } else {
                    // Turn "unsigned" long value back to signed one
                    if(deltaDelta > (1 << (toRead - 1))) {
                        deltaDelta -= (1 << toRead);
                    }
                }

                deltaDelta = (int) deltaDelta;
            }

            // Negative values of deltaDelta are not handled correctly. actually nothing negative is.. ugh

            storedDelta = storedDelta + deltaDelta;
            storedTimestamp = storedDelta + storedTimestamp;

            // Read value
            if (readBit()) {
                // else -> same value as before
                if (readBit()) {
                    // New leading and trailing zeros
                    storedLeadingZeros = (int) getLong(5);

                    byte significantBits = (byte) getLong(6);
                    if(significantBits == 0) {
                        significantBits = 64;
                    }
                    storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
                }
                long value = getLong(64 - storedLeadingZeros - storedTrailingZeros);
                value <<= storedTrailingZeros;
                value = Double.doubleToRawLongBits(storedVal) ^ value; // Would it make more sense to keep the rawLongBits in the memory than redo it?
                storedVal = Double.longBitsToDouble(value);
            }
        }

        return new Pair(storedTimestamp, storedVal);
    }

    private boolean readBit() {
        byte bit = (byte) ((b >> (bitsLeft - 1)) & 1);
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