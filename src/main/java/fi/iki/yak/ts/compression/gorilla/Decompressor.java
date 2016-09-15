package fi.iki.yak.ts.compression.gorilla;

/**
 * Decompresses a compressed stream done created by the Compressor. Returns pairs of timestamp and flaoting point value.
 *
 * @author Michael Burman
 */
public class Decompressor {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private double storedVal = 0;
    private long storedTimestamp = 0;
    private long storedDelta = 0;

    private long blockTimestamp = 0;

    private BitInput in;

    public Decompressor(BitInput input) {
        in = input;
        readHeader();
    }

    private void readHeader() {
        blockTimestamp = in.getLong(64);
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    public Pair readPair() {

        if (storedTimestamp == 0) {
            // First item to read
            storedDelta = in.getLong(Compressor.FIRST_DELTA_BITS);

            if(storedDelta == (1<<27) - 1) {
                // Empty - no timestamp space left
                return null;
            }
            storedVal = Double.longBitsToDouble(in.getLong(64));
            storedTimestamp = blockTimestamp + storedDelta;
        } else {
            if(nextTimestamp() == Long.MAX_VALUE) {
                return null;
            }
            nextValue();
        }

        return new Pair(storedTimestamp, storedVal);
    }

    private int bitsToRead() {
        // TODO Read 4 bits and then return the unused extra bits for the next values.. or something
        int toRead = 0;
        if (in.readBit()) {
            if (!in.readBit()) {
                toRead = 7; // '10'
            } else {
                if (!in.readBit()) {
                    toRead = 9; // '110'
                } else {
                    if (!in.readBit()) {
                        // 1110
                        toRead = 12;
                    } else {
                        // 1111
                        toRead = 32;
                    }
                }
            }
        }
        return toRead;
    }

    private long nextTimestamp() {
        // Next, read timestamp
        long deltaDelta = 0;
        int toRead = bitsToRead();
        if (toRead > 0) {
            deltaDelta = in.getLong(toRead);

            if(toRead == 32) {
                if ((int) deltaDelta == 0xFFFFFFFF) {
                    // End of stream
                    return Long.MAX_VALUE;
                }
            } else {
                // Turn "unsigned" long value back to signed one
                if(deltaDelta > (1 << (toRead - 1))) {
                    deltaDelta -= (1 << toRead);
                }
            }

            deltaDelta = (int) deltaDelta;
        }

        storedDelta = storedDelta + deltaDelta;
        storedTimestamp = storedDelta + storedTimestamp;

        return storedTimestamp;
    }

    private double nextValue() {
        // Read value
        if (in.readBit()) {
            // else -> same value as before
            if (in.readBit()) {
                // New leading and trailing zeros
                storedLeadingZeros = (int) in.getLong(5);

                byte significantBits = (byte) in.getLong(6);
                if(significantBits == 0) {
                    significantBits = 64;
                }
                storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
            }
            long value = in.getLong(64 - storedLeadingZeros - storedTrailingZeros);
            value <<= storedTrailingZeros;
            value = Double.doubleToRawLongBits(storedVal) ^ value; // Would it make more sense to keep the rawLongBits in the memory than redo it?
            storedVal = Double.longBitsToDouble(value);
        }
        return storedVal;
    }

}