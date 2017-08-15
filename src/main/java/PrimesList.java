package main.java;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Created by yorin on 11-8-17.
 */
public class PrimesList {
    private BitSet primes;
    private long indexOffset;

    private List primeRemainders = Arrays.asList(new Integer[] {1, 7, 11, 13, 17, 19, 23, 29});


    public PrimesList(BitSet primes, long indexOffset) {
        this.primes = primes;
        this.indexOffset = indexOffset;
    }

    public void printPrimes() {
        for (int i = 0; i < primes.length(); i++) {
            if(primes.get(i)) {
                long index = i + indexOffset;
                long byteOffset = (long) Math.floor((double) index / 8);
                int remainderIndex = (int) (index % 8);
                int remainder = (int) primeRemainders.get(remainderIndex);

                long prime = byteOffset * 30 + remainder;
                System.out.println(prime);
             }
        }
    }

    public BitSet getBitSet() {
        return primes;
    }
}
