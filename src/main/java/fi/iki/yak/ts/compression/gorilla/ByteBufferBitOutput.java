package fi.iki.yak.ts.compression.gorilla;

import java.nio.ByteBuffer;

/**
 * An implementation of BitOutput interface that uses off-heap storage.
 *
 * @author Michael Burman
 */
public class ByteBufferBitOutput implements BitOutput {
    public static final int DEFAULT_ALLOCATION = 4096;

    private ByteBuffer bb;
    private byte b;
    private int bitsLeft = Byte.SIZE;

    /**
     * Creates a new ByteBufferBitOutput with a default allocated size of 4096 bytes.
     */
    public ByteBufferBitOutput() {
        this(DEFAULT_ALLOCATION);
    }

    /**
     * Give an initialSize different than DEFAULT_ALLOCATIONS. Recommended to use values which are dividable by 4096.
     *
     * @param initialSize
     */
    public ByteBufferBitOutput(int initialSize) {
        bb = ByteBuffer.allocateDirect(initialSize);
        b = bb.get(bb.position());
    }

    private void expandAllocation() {
        ByteBuffer largerBB = ByteBuffer.allocateDirect(bb.capacity()*2);
        bb.flip();
        largerBB.put(bb);
        largerBB.position(bb.capacity());
        bb = largerBB;
    }

    private void flipByte() {
        if(bitsLeft == 0) {
            bb.put(b);
            if(!bb.hasRemaining()) {
                expandAllocation();
            }
            b = bb.get(bb.position());
            bitsLeft = Byte.SIZE;
        }
    }

    /**
     * Sets the next bit (or not) and moves the bit pointer.
     *
     * @param bit
     */
    public void writeBit(boolean bit) {
        if(bit) {
            b |= (1 << (bitsLeft - 1));
        }
        bitsLeft--;
        flipByte();
    }


    /**
     * Writes the given long to the stream using bits amount of bits.
     *
     * @param value
     * @param bits How many bits are stored to the stream
     */
    public void writeBits(long value, int bits) {
        // TODO Fix already compiled into a medium method
        while(bits > 0) {
            int shift = bits - bitsLeft;
            // TODO Should I optimize the 0 shift case?
            if(shift > 0) {
                b |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
            } else {
                int shiftAmount = Math.abs(shift);
                b |= (byte) (value << shiftAmount);
            }
            if(bits > bitsLeft) {
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                bitsLeft -= bits;
                bits = 0;
            }
            flipByte();
        }
    }

    /**
     * Causes the currently handled byte to be written to the stream
     */
    @Override
    public void flush() {
        bitsLeft = 0;
        flipByte(); // Causes write to the ByteBuffer
    }

    /**
     * Returns the underlying DirectByteBuffer
     *
     * @return ByteBuffer of type DirectByteBuffer
     */
    public ByteBuffer getByteBuffer() {
        return this.bb;
    }
}
