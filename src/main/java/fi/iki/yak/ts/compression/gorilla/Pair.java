package fi.iki.yak.ts.compression.gorilla;

/**
 * Created by michael on 8/5/16.
 */
public class Pair {
    private long timestamp;
    private double value;

    public Pair(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }
}
