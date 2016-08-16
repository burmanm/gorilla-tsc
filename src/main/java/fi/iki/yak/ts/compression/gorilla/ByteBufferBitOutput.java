package fi.iki.yak.ts.compression.gorilla;

import java.nio.ByteBuffer;

/**
 * @author Michael Burman
 */
public class ByteBufferBitOutput implements BitOutput {
    public static final int DEFAULT_ALLOCATION = 4096;

    private ByteBuffer bb;
    private byte b;
    private int bitsLeft = Byte.SIZE;

    public ByteBufferBitOutput() {
        this(DEFAULT_ALLOCATION);
    }

    public ByteBufferBitOutput(int initialSize) {
        bb = ByteBuffer.allocateDirect(initialSize);
        b = bb.get(0);
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

    public void writeBit(boolean bit) {
        if(bit) {
            b |= (1 << (bitsLeft - 1));
        }
        bitsLeft--;
        flipByte();
    }


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

    @Override
    public void flush() {
        bitsLeft = 0;
        flipByte(); // Causes write to the ByteBuffer
    }

    public ByteBuffer getByteBuffer() {
        return this.bb;
    }
}
