package fi.iki.yak.ts.compression.gorilla;

/**
 * Implements the time series compression as described in the Facebook's Gorilla Paper. Value compression
 * is for floating points only.
 *
 * @author Michael Burman
 */
public class Compressor {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private long storedVal = 0;
    private long storedTimestamp = 0;
    private long storedDelta = 0;

    private long blockTimestamp = 0;

    public final static short FIRST_DELTA_BITS = 27;

    private BitOutput out;

    // We should have access to the series?
    public Compressor(long timestamp, BitOutput output) {
        blockTimestamp = timestamp;
        out = output;
        addHeader(timestamp);
    }

    private void addHeader(long timestamp) {
        // One byte: length of the first delta
        // One byte: precision of timestamps
        out.writeBits(timestamp, 64);
    }

    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param timestamp Timestamp which is inside the allowed time block (default 24 hours with millisecond precision)
     * @param value next floating point value in the series
     */
    public void addValue(long timestamp, long value) {
        if(storedTimestamp == 0) {
            writeFirst(timestamp, value);
        } else {
            compressTimestamp(timestamp);
            compressValue(value);
        }
    }

    /**
     * Adds a new double value to the series. Note, values must be inserted in order.
     *
     * @param timestamp Timestamp which is inside the allowed time block (default 24 hours with millisecond precision)
     * @param value next floating point value in the series
     */
    public void addValue(long timestamp, double value) {
        if(storedTimestamp == 0) {
            writeFirst(timestamp, Double.doubleToRawLongBits(value));
        } else {
            compressTimestamp(timestamp);
            compressValue(Double.doubleToRawLongBits(value));
        }
    }

    private void writeFirst(long timestamp, long value) {
        storedDelta = timestamp - blockTimestamp;
        storedTimestamp = timestamp;
        storedVal = value;

        out.writeBits(storedDelta, FIRST_DELTA_BITS);
        out.writeBits(storedVal, 64);
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    public void close() {
        // These are selected to test interoperability and correctness of the solution, this can be read with go-tsz
        out.writeBits(0x0F, 4);
        out.writeBits(0xFFFFFFFF, 32);
        out.skipBit();
        out.flush();
    }

    /**
     * Difference to the original Facebook paper, we store the first delta as 27 bits to allow
     * millisecond accuracy for a one day block.
     *
     * Also, the timestamp delta-delta is not good for millisecond compressions..
     *
     * @param timestamp epoch
     */
    private void compressTimestamp(long timestamp) {
        // a) Calculate the delta of delta
        long newDelta = (timestamp - storedTimestamp);
        long deltaD = newDelta - storedDelta;

        // If delta is zero, write single 0 bit
        if(deltaD == 0) {
            out.skipBit();
        } else if(deltaD >= -63 && deltaD <= 64) {
            out.writeBits(0x02, 2); // store '10'
            out.writeBits(deltaD, 7); // Using 7 bits, store the value..
        } else if(deltaD >= -255 && deltaD <= 256) {
            out.writeBits(0x06, 3); // store '110'
            out.writeBits(deltaD, 9); // Use 9 bits
        } else if(deltaD >= -2047 && deltaD <= 2048) {
            out.writeBits(0x0E, 4); // store '1110'
            out.writeBits(deltaD, 12); // Use 12 bits
        } else {
            out.writeBits(0x0F, 4); // Store '1111'
            out.writeBits(deltaD, 32); // Store delta using 32 bits
        }

        storedDelta = newDelta;
        storedTimestamp = timestamp;
    }

    private void compressValue(long value) {
        // TODO Fix already compiled into a big method
       long xor = storedVal ^ value;

        if(xor == 0) {
            // Write 0
            out.skipBit();
        } else {
            int leadingZeros = Long.numberOfLeadingZeros(xor);
            int trailingZeros = Long.numberOfTrailingZeros(xor);

            // Check overflow of leading? Can't be 32!
            if(leadingZeros >= 32) {
                leadingZeros = 31;
            }

            // Store bit '1'
            out.writeBit();

            if(leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
                writeExistingLeading(xor);
            } else {
                writeNewLeading(xor, leadingZeros, trailingZeros);
            }
        }

        storedVal = value;
    }

    /**
     * If there at least as many leading zeros and as many trailing zeros as previous value, control bit = 0 (type a)
     * store the meaningful XORed value
     *
     * @param xor XOR between previous value and current
     */
    private void writeExistingLeading(long xor) {
        out.skipBit();
        int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
        out.writeBits(xor >>> storedTrailingZeros, significantBits);
    }

    /**
     * store the length of the number of leading zeros in the next 5 bits
     * store length of the meaningful XORed value in the next 6 bits,
     * store the meaningful bits of the XORed value
     * (type b)
     *
     * @param xor XOR between previous value and current
     * @param leadingZeros New leading zeros
     * @param trailingZeros New trailing zeros
     */
    private void writeNewLeading(long xor, int leadingZeros, int trailingZeros) {
        out.writeBit();
        out.writeBits(leadingZeros, 5); // Number of leading zeros in the next 5 bits

        int significantBits = 64 - leadingZeros - trailingZeros;
        out.writeBits(significantBits, 6); // Length of meaningful bits in the next 6 bits
        out.writeBits(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR

        storedLeadingZeros = leadingZeros;
        storedTrailingZeros = trailingZeros;
    }
}
