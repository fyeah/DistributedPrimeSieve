package main.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

/**
 * Created by yorin on 20-7-17.
 */
public class SieveSerial {


    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        ArrayList<Integer> result = findPrimes(100000000);
        long end = System.currentTimeMillis();
//        for (Integer prime : result) {
//            System.out.println(prime);
//        }
        System.out.println("\n Size:");
        System.out.println(result.size());
        System.out.println("\n Miliseconds:");
        System.out.println(end-start);

    }

    public static ArrayList<Integer> findPrimes(int n) {

        // index offset: index 0: 2. index >0 = 2*i + 1.
        // only store odd indexed.
        int size = ((int) Math.ceil((double) n/2)-1);
        BitSet isPrime = new BitSet(size);
        for (int i = 0; i <= size; i++) {
            isPrime.set(i);
        }

        final int MAX = (int) Math.sqrt(n);

        for (int i = 1; (i*2+1) <= MAX; i++) {
            if (isPrime.get(i)) {
                // start at i because every composites of j < i have already been found.
                for (int j = i*2+1; j*(i*2+1) <= n; j+=2) {
                    isPrime.clear(((j*(i*2+1)-1)/2));
                }

            }
        }

        ArrayList<Integer> result = new ArrayList<Integer>();
        result.add(2); // algorithm doesn't find prime number 2.
        for (int i = 1; i < isPrime.length(); i++) {
            if (isPrime.get(i)) {
                result.add(i*2+1);
            }
        }
        return result;
    }

    // notes
    // clear out array when done with certain part. Then move on to the next part. No need to have everything in memory all the time right?
    // Then I do have to save the prime numbers that I found..


    // thread safety..? Will the parent thread always get the correctly updated bitset?
    // that is only in shared memory situation

    // try parallel first. Then if that goes okay try distributed with message queue.


    // dit stukje kan in parallel:
    // add job to queue. other threads will take job off queue
    // only need to return the leftover numbers? or return the whole array? or return the crossed off numbers?
    // or return nothing? Let the main thread ask the thread (in that number's range) if that number isPrime or not.

    //             doesn't seem to be any faster.
//            if (i >= 3*3 && i % 3 == 0) {
//                continue;
//            }
//            // skip multiples of five
//            if (i >= 5*5 && i % 5 == 0) {
//                continue;
//            }
//            // skip multiples of seven
//            if (i >= 7*7 && i % 7 == 0) {
//                continue;
//            }
//            // skip multiples of eleven
//            if (i >= 11*11 && i % 11 == 0) {
//                continue;
//            }
//            // skip multiples of thirteen
//            if (i >= 13*13 && i % 13 == 0) {
//                continue;
//            }





    public static ArrayList<Integer> originalFindPrimes(int n) {
        BitSet isPrime = new BitSet(n);
        for (int i = 3; i <= n; i+=2) {
            isPrime.set(i);
        }
        isPrime.set(2);

        for (int i = 3; i*i <= n; i+=2) {
            if (isPrime.get(i)) {
                for (int j = 3; j*i <= n; j+=2) {
                    isPrime.clear(j*i);
                }

            }
        }

        ArrayList<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < isPrime.length(); i++) {
            if (isPrime.get(i)) {
                result.add(i);
            }
        }
        return result;
    }

    public static ArrayList<Integer> basicFindPrimes(int n) {
        // initially assume all integers are prime
        boolean[] isPrime = new boolean[n+1];
        for (int i = 2; i <= n; i++) {
            isPrime[i] = true;
        }

        // mark non-primes <= n using Sieve of Eratosthenes
        for (int factor = 2; factor*factor <= n; factor++) {

            // if factor is prime, then mark multiples of factor as nonprime
            // suffices to consider mutiples factor, factor+1, ...,  n/factor
            if (isPrime[factor]) {
                for (int j = factor; factor*j <= n; j++) {
                    isPrime[factor*j] = false;
                }
            }
        }

        ArrayList<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < isPrime.length; i++) {
            if (isPrime[i]) {
                result.add(i);
            }
        }
        return result;
    }
}
