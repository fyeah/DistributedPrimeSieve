package main.java.models;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by yorin on 16-8-17.
 */
public class SieveJob implements Serializable {
    private long firstNumer;
    private long lastNumber;
    private ArrayList<Integer> primeFactor;

    public SieveJob(long firstNumer, long lastNumber, ArrayList<Integer> primeFactor) {
        this.firstNumer = firstNumer;
        this.lastNumber = lastNumber;
        this.primeFactor = primeFactor;
    }

    public long getFirstNumer() {
        return firstNumer;
    }

    public long getLastNumber() {
        return lastNumber;
    }

    public ArrayList<Integer> getPrimeFactor() {
        return primeFactor;
    }
}
