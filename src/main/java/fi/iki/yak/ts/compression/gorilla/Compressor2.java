package fi.iki.yak.ts.compression.gorilla;

import java.util.stream.Stream;

/**
 * Implements the time series compression as described in the Facebook's Gorilla Paper. Value compression
 * is for floating points only.
 *
 * @author Michael Burman
 */
public class Compressor2 {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private long storedVal = 0;
    private long storedTimestamp = 0;
    private int storedDelta = 0;

    private long blockTimestamp = 0;

    public final static int FIRST_DELTA_BITS = 27;
//    private static int NEW_LEADING_MASK = (0x03 << 11);

    private static int DELTAD_7_MASK = 0x02 << 7;
    private static int DELTAD_9_MASK = 0x06 << 9;
    private static int DELTAD_12_MASK = 0x0E << 12;
//    private static long DELTAD_32_MASK = 0x0F << 32;

    private BitOutput2 out;

    // We should have access to the series?
    public Compressor2(long timestamp, BitOutput2 output) {
        blockTimestamp = timestamp;
        out = output;
        addHeader(timestamp);
    }

    public void compressLongStream(Stream<Pair> stream) {
        stream.peek(p -> writeFirst(p.getTimestamp(), Double.doubleToRawLongBits(p.getDoubleValue()))).skip(1)
                .forEach(p -> {
                    compressTimestamp(p.getTimestamp());
                    compressValue(p.getLongValue());
                });
    }

    private void addHeader(long timestamp) {
        // One byte: length of the first delta
        // One byte: precision of timestamps
//        System.out.printf("addHeader, timestamp->%d\n", timestamp);
        out.writeBits(timestamp, 64);
    }

    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param timestamp Timestamp which is inside the allowed time block (default 24 hours with millisecond precision)
     * @param value next floating point value in the series
     */
    public void addValue(long timestamp, long value) {
        // TODO Try to get rid of this branch .. compressLongStream does do that
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
            return;
        }
        compressTimestamp(timestamp);
        compressValue(Double.doubleToRawLongBits(value));
    }

    private void writeFirst(long timestamp, long value) {
        storedDelta = (int) (timestamp - blockTimestamp);
        storedTimestamp = timestamp;
        storedVal = value;

//        System.out.printf("writeFirst: storedDelta->%d, storedTimestamp->%d, blockTimestamp->%d\n", storedDelta,
//                storedTimestamp, blockTimestamp);

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
        int newDelta = (int) (timestamp - storedTimestamp);
        int deltaD = newDelta - storedDelta;

        // TODO First, transform with zigZag to avoid negative numbers
        // TODO Then, take highestOneBit and a switch clause?

        // TODO Also, storing these as Long is insane.. we can't have larger than 27 bit delta in this block.
        // TODO And we only support storing them as max 32 bits..

        // TODO Remember to write to thesis..

        // TODO Measure if vs. switch also.. or could we have some branchless idea (how on earth?)
        // TODO Needs a better test, timestamps only
        // TODO Also, check the assembly code of this switch clause.. does it turn to hashmap or table lookup?

        // TODO Could I otherwise use the knowledge of limited range somehow? Huffman? Calculate deltas first and
        // then find the maximum range..

        // TODO To thesis: fluctuating values will in default mode cause always 64 bits write (-2, +2 for example)
        // zigzag should reduce this to fewer bits (writeExisting probably)

        if(deltaD == 0) {
            out.skipBit();
        } else {
            deltaD = encodeZigZag32(deltaD);
            deltaD--; // Increase by one in the decompressing phase as we have one free bit
            int bitsRequired = 32 - Integer.numberOfLeadingZeros(deltaD); // Faster than highestSetBit

            // TODO Verify that this is lookupswitch in the bytecode and inlineable
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
        }

//        // TODO Could this be a table lookup after all? No switch needed..
//        // If delta is zero, write single 0 bit
//        if(deltaD == 0) {
//            out.skipBit();
//        } else if(deltaD >= -63 && deltaD <= 64) {
//            // TODO There can't be a zero.. so stored values should be between 0-63 (+1 on the read path)
//
//            // TODO Could use ZigZagging with -1 always on the write path. That would allow values 0-127 (1-128) to
//            // be stored as 0 can't be a value. No branches.
//
//            // We could store -64 to 64 that way? (ignore the 0)
//            out.writeBits(0x02, 2); // store '10'
//            out.writeBits(deltaD, 7);
//            // Using 7 bits, store the value..
//        } else if(deltaD >= -255 && deltaD <= 256) {
//            out.writeBits(0x06, 3); // store '110'
//            out.writeBits(deltaD, 9);
//            // Use 9 bits
//        } else if(deltaD >= -2047 && deltaD <= 2048) {
//            out.writeBits(0x0E, 4); // store '1110'
//            out.writeBits(deltaD, 12); // Use 12 bits
//        } else {
//            out.writeBits(0x0F, 4); // Store '1111'
//            out.writeBits(deltaD, 32); // Store delta using 32 bits
//        }

        storedDelta = newDelta;
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

    /**
     * Encode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n A signed 64-bit integer.
     * @return An unsigned 64-bit integer, stored in a signed int because
     *         Java has no explicit unsigned support.
     */
    public static long encodeZigZag64(final long n) {
        // Note:  the right-shift must be arithmetic
        return (n << 1) ^ (n >> 63);
    }

    // END: From protobuf

    private void compressValue(long value) {
       long xor = storedVal ^ value;

        if(xor == 0) {
            // Write 0
            out.skipBit();
        } else {
            int leadingZeros = Long.numberOfLeadingZeros(xor);
            int trailingZeros = Long.numberOfTrailingZeros(xor);

            // Check overflow of leading? Can't be 32!

            // To thesis - 6 bits to avoid a branch & to store longs with 63 leading zeros - as we store that a lot

//            if(leadingZeros >= 31) {
//                leadingZeros = 31;
//            }

//            leadingZeros = Math.min(31, leadingZeros);
//            --leadingZeros;

            out.writeBit(); // Optimize to writeNewLeading / writeExistingLeading?

            // This branch is something I can't avoid..
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
//        out.writeBit(true);
//        out.writeBit(false);

        int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
        xor >>>= storedTrailingZeros;
//        xor |= (0x02 << significantBits);
//        out.writeBits(0x02, 2);
        out.writeBits(xor, significantBits);
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
//        out.writeBits(0x03, 2);
        out.writeBit();

        // To thesis - use (significantBits - 1) in storage - avoids a branch
        int significantBits = 64 - leadingZeros - trailingZeros;
//        leadingZeros = ((leadingZeros << 6) | significantBits);
//        leadingZeros |= NEW_LEADING_MASK;


//        out.writeBit(true);
        // Different from original, bits 5 -> 6, avoids a branch, allows storing small longs
        out.writeBits(leadingZeros, 6); // Number of leading zeros in the next 6 bits
//
        out.writeBits(significantBits - 1, 6); // Length of meaningful bits in the next 6 bits
        // set bits 12 & 13 in leadingZeros..
//        out.writeBits(leadingZeros, 13);
//        xor >>>= trailingZeros;
        out.writeBits(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR

        storedLeadingZeros = leadingZeros;
        storedTrailingZeros = trailingZeros;
    }
}
