package fi.iki.yak.ts.compression.gorilla;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * An implementation of BitOutput interface that uses off-heap storage.
 *
 * @author Michael Burman
 */
public class LongOutputProto implements BitOutput {
    public static final int DEFAULT_ALLOCATION =  4096*32;

    private long[] longArray;
    private long lB;
    private int position = 0;
    private int bitsLeft = Long.SIZE;

    /**
     * Creates a new ByteBufferBitOutput with a default allocated size of 4096 bytes.
     */
    public LongOutputProto() {
        this(DEFAULT_ALLOCATION);
    }

    /**
     * Give an initialSize different than DEFAULT_ALLOCATIONS. Recommended to use values which are dividable by 4096.
     *
     * @param initialSize New initialsize to use
     */
    public LongOutputProto(int initialSize) {
        longArray = new long[initialSize];
        lB = longArray[position];
    }

    private void expandAllocation() {
        long[] largerArray = new long[longArray.length*2];
        System.arraycopy(longArray, 0, largerArray, 0, longArray.length);
        longArray = largerArray;
    }

    private void checkAndFlipByte() {
        if(bitsLeft == 0) {
            flipByte();
        }
    }

    private void flipByte() {
        longArray[position] = lB;
        ++position;
        if(position >= (longArray.length - 2)) { // We want to have always at least 2 longs available
            expandAllocation();
        }
        lB = 0; // Do I need even this?
        bitsLeft = Long.SIZE;
    }

    private void flipByteWithoutExpandCheck() {
        longArray[position] = lB;
        ++position;
        lB = 0; // Do I need even this?
        bitsLeft = Long.SIZE;
    }

    /**
     * Sets the next bit (or not) and moves the bit pointer.
     *
     * @param bit true == 1 or false == 0
     */
    public void writeBit(boolean bit) {
        if(bit) {
            lB |= (1 << (bitsLeft - 1));
        }
        bitsLeft--;
        checkAndFlipByte();
    }

    /**
     * Writes the given long to the stream using bits amount of meaningful bits. This command does not
     * check input values, so if they're larger than what can fit the bits (you should check this before writing),
     * expect some weird results.
     *
     * @param value Value to be written to the stream
     * @param bits How many bits are stored to the stream
     */
    public void writeBits(long value, int bits) {
        if(bits <= bitsLeft) {
            int bitsToWrite = bitsLeft - bits;
            lB |= (value << bitsToWrite) & (1 << bitsToWrite - 1);
            bitsLeft -= bits;
            checkAndFlipByte(); // We could be at 0 bits left because of the <= condition
        } else {
            value &= (1 << bits - 1); // turn to unsigned first
            int bitsToWrite = bits - bitsLeft;
            lB |= value >> bitsToWrite; // Fill the current word
            flipByteWithoutExpandCheck();
            bits -= bitsToWrite;
            lB |= value << (64 - bits);
            bitsLeft -= bits;
        }
    }

    /**
     * Causes the currently handled byte to be written to the stream
     */
    @Override
    public void flush() {
        flipByte(); // Causes write to the ByteBuffer
    }

    public void reset() {
        position = 0;
        bitsLeft = Long.SIZE;
        lB = 0;
    }

    /**
     * Returns the underlying DirectByteBuffer
     *
     * @return ByteBuffer of type DirectByteBuffer
     */
    public ByteBuffer getByteBuffer() {
//        LongBuffer wrap = LongBuffer.wrap(longArray, 0, position);
        ByteBuffer bb = ByteBuffer.allocate(position * 8);
        bb.asLongBuffer().put(longArray, 0, position);
        bb.position(position * 8);
        return bb;
    }
//    public ByteBuffer getByteBuffer() {
//        return this.bb;
//    }
}
