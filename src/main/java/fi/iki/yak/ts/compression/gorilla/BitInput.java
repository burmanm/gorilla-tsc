package fi.iki.yak.ts.compression.gorilla;

/**
 * This interface is used for reading a compressed time series.
 *
 * @author Michael Burman
 */
public interface BitInput {

    /**
     * Reads the next bit and returns true if bit is set and false if not.
     *
     * @return
     */
    boolean readBit();

    /**
     * Returns a long that was stored in the next X bits in the stream.
     *
     * @param bits Amount of least significant bits to read from the stream.
     * @return
     */
    long getLong(int bits);
}
