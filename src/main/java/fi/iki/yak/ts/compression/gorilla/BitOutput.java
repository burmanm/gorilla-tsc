package fi.iki.yak.ts.compression.gorilla;

/**
 * This interface is used to write a compressed timeseries.
 *
 * @author Michael Burman
 */
public interface BitOutput {

    /**
     * Stores a single bit, set if true, false otherwise.
     * @param bit
     */
    void writeBit(boolean bit);

    /**
     * Write the given long value using the defined amount of least significant bits.
     *
     * @param value
     * @param bits How many bits are stored to the stream
     */
    void writeBits(long value, int bits);

    /**
     * Flushes the current byte to the underlying stream
     */
    void flush();
}
