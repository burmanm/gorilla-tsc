package fi.iki.yak.ts.compression.gorilla;

import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;

/**
 * Implements a slightly modified version of the time series compression as described in the Facebook's Gorilla
 * Paper.
 *
 * @author Michael Burman
 */
public class GorillaCompressor {

    private long storedTimestamp = 0;
    private int storedDelta = 0;

    private long blockTimestamp = 0;

    public final static int FIRST_DELTA_BITS = 27;

    private static int DELTAD_7_MASK = 0x02 << 7;
    private static int DELTAD_9_MASK = 0x06 << 9;
    private static int DELTAD_12_MASK = 0x0E << 12;

    private BitOutput out;

    private ValueCompressor valueCompressor;

    public GorillaCompressor(long timestamp, BitOutput output) {
        this(timestamp, output, new LastValuePredictor());
    }

    public GorillaCompressor(long timestamp, BitOutput output, Predictor predictor) {
        blockTimestamp = timestamp;
        out = output;
        addHeader(timestamp);
        this.valueCompressor = new ValueCompressor(output, predictor);
    }

    private void addHeader(long timestamp) {
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
            valueCompressor.compressValue(value);
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
            return;
        }
        compressTimestamp(timestamp);
        valueCompressor.compressValue(Double.doubleToRawLongBits(value));
    }

    private void writeFirst(long timestamp, long value) {
        storedDelta = (int) (timestamp - blockTimestamp);
        storedTimestamp = timestamp;

        out.writeBits(storedDelta, FIRST_DELTA_BITS);
        valueCompressor.writeFirst(value);
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    public void close() {
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
        int newDelta = (int) (timestamp - storedTimestamp);
        int deltaD = newDelta - storedDelta;

        if(deltaD == 0) {
            out.skipBit();
        } else {
            deltaD = encodeZigZag32(deltaD);
            deltaD--; // Increase by one in the decompressing phase as we have one free bit
            int bitsRequired = 32 - Integer.numberOfLeadingZeros(deltaD); // Faster than highestSetBit

            // Turns to inlineable tableswitch
            switch(bitsRequired) {
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    deltaD |= DELTAD_7_MASK;
                    out.writeBits(deltaD, 9);
                    break;
                case 8:
                case 9:
                    deltaD |= DELTAD_9_MASK;
                    out.writeBits(deltaD, 12);
                    break;
                case 10:
                case 11:
                case 12:
                    out.writeBits(deltaD | DELTAD_12_MASK, 16);
                    break;
                default:
                    out.writeBits(0x0F, 4); // Store '1111'
                    out.writeBits(deltaD, 32); // Store delta using 32 bits
                    break;
            }
            storedDelta = newDelta;
        }

        storedTimestamp = timestamp;
    }

    // START: From protobuf

    /**
     * Encode a ZigZag-encoded 32-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n A signed 32-bit integer.
     * @return An unsigned 32-bit integer, stored in a signed int because
     *         Java has no explicit unsigned support.
     */
    public static int encodeZigZag32(final int n) {
        // Note:  the right-shift must be arithmetic
        return (n << 1) ^ (n >> 31);
    }

    // END: From protobuf
}
