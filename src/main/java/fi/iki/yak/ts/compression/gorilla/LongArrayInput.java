/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fi.iki.yak.ts.compression.gorilla;

/**
 * Implements on-heap long array input stream
 *
 * @author Michael Burman
 */
public class LongArrayInput implements BitInput {
    private final long[] longArray; // TODO Investigate also the ByteBuffer performance here.. or Unsafe
    private long lB;
    private int position = 0;
    private int bitsLeft = 0;

    public LongArrayInput(long[] array) {
        this.longArray = array;
        flipByte();
    }

    @Override
    public boolean readBit() {
        boolean bit = (lB & LongArrayOutput.BIT_SET_MASK[bitsLeft - 1]) != 0;
        bitsLeft--;
        checkAndFlipByte();
        return bit;
    }

    private void flipByte() {
        lB = longArray[position++];
        bitsLeft = Long.SIZE;
    }

    private void checkAndFlipByte() {
        if(bitsLeft == 0) {
            flipByte();
        }
    }

    @Override
    public long getLong(int bits) {
        long value;
        if(bits <= bitsLeft) {
            // We can read from this word only
            // Shift to correct position and take only n least significant bits
            value = (lB >>> (bitsLeft - bits)) & LongArrayOutput.MASK_ARRAY[bits - 1];
            bitsLeft -= bits; // We ate n bits from it
            checkAndFlipByte();
        } else {
            // This word and next one, no more (max bits is 64)
            value = lB & LongArrayOutput.MASK_ARRAY[bitsLeft - 1]; // Read what's left first
            bits -= bitsLeft;
            flipByte(); // We need the next one
            value <<= bits; // Give n bits of space to value
            value |= (lB >>> (bitsLeft - bits));
            bitsLeft -= bits;
        }
        return value;
    }

    @Override
    public int nextClearBit(int maxBits) {
        int val = 0x00;

        for(int i = 0; i < maxBits; i++) {
            val <<= 1;
            // TODO This loop has too many branches and unnecessary boolean casts
            boolean bit = readBit();

            if(bit) {
                val |= 0x01;
            } else {
                break;
            }
        }
        return val;
    }
}
