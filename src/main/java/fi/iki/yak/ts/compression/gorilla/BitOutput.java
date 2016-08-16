package fi.iki.yak.ts.compression.gorilla;

/**
 * @author Michael Burman
 */
public interface BitOutput {
    void writeBit(boolean bit);
    void writeBits(long value, int bits);

    /**
     * Flushes the current byte to the underlying stream
     */
    void flush();
}
