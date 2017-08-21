package main.java;

import main.java.models.PrimesList;
import main.java.models.SieveJob;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by yorin on 20-7-17.
 */

class SegmentedSieve implements Runnable {
    private long firstNumber;
    private long lastNumber;
    private ArrayList<Integer> primeFactors;
    private CountDownLatch countDownLatch;

    private ArrayList<PrimesList> result;
    private ArrayList<PrimesList> tempResult = new ArrayList<PrimesList>();
    private int tempResultIndex = -1; // no BitSet added yet so initial index = -1

    boolean offsetInitialized = false;


    private long primeNumberIndexOffset;
    private long resultIndexOffset;
    private int segmentSize;
    private int nrOfSegments;

    private int counter;

    private List primeRemainders = Arrays.asList(new Integer[] {1, 7, 11, 13, 17, 19, 23, 29});

    private final int segmentThreshold = 500000;
//    private final int segmentThreshold = 1000000;
//    private final int segmentThreshold = Integer.MAX_VALUE;


    public SegmentedSieve(long firstNumber, long lastNumber, ArrayList<Integer> primeFactors, CountDownLatch countDownLatch,
                          ArrayList<PrimesList> result) {
        this.firstNumber = firstNumber;
        this.lastNumber = lastNumber;
        this.primeFactors = primeFactors;
        this.countDownLatch = countDownLatch;
        this.result = result;
        init();
    }

    private void init() {
        long diff = lastNumber - firstNumber;
        long size;
        if(firstNumber%2 != 0 && lastNumber%2 !=0) {
            size = ((long) Math.ceil((double) diff/2));
        } else {
            size = ((long) Math.ceil((double) diff/2)-1);
        }


        if(size >= segmentThreshold) {
            nrOfSegments = (int) Math.ceil(((double) size / segmentThreshold));
            segmentSize = (int) (size / nrOfSegments);
        } else {
            nrOfSegments = 1;
            segmentSize = (int) size;
        }

        primeNumberIndexOffset = firstNumber;
        if(primeNumberIndexOffset % 2 != 0) {
            primeNumberIndexOffset--; //index offset has to be uneven because the index should represent an uneven number in the isprime bitset.
        }
    }

    @Override
    public void run() {
        try {
            segmentedSieve();
            for(PrimesList primeList : tempResult) {
                result.add(primeList);
            }

            countDownLatch.countDown();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void segmentedSieve() {

        // do the first and last numbers make sense? I don't think so.
        for (int i = 0; i < nrOfSegments; i++) {
            long segmentFirstNumber;
            long segmentLastNumber;
            if(i == 0) {
                segmentFirstNumber = firstNumber;
            } else {
                segmentFirstNumber = (i * (long) segmentSize)*2 + 2 + primeNumberIndexOffset;
            }

            if(i == nrOfSegments-1) {
                segmentLastNumber = lastNumber;
            } else {
                segmentLastNumber = ((i+1) * (long) segmentSize)*2 + 1 + primeNumberIndexOffset;
            }

            sieveSegment(segmentFirstNumber, segmentLastNumber);
        }
    }

    private void sieveSegment(long firstNumber, long lastNumber) {
        long diff = lastNumber - firstNumber;
        int size;
        if(firstNumber%2 != 0 && lastNumber%2 !=0) {
            size = ((int) Math.ceil((double) diff/2));
        } else {
            size = ((int) Math.ceil((double) diff/2)-1);
        }

        long segmentIndexOffset = firstNumber;
        if(segmentIndexOffset % 2 != 0) {
            segmentIndexOffset--; // moet dit..?
        }


        BitSet isPrime = new BitSet(size);

        for (int i = 0; i <= size ; i++) {
            isPrime.set(i);
        }


        // primefactor might have to become a long with size bigger than 10^18
        for(int primeFactor : primeFactors) {
            long startJ = (long) Math.ceil((double)firstNumber/primeFactor);
            // we also want it to be uneven.
            if(startJ%2 == 0) {
                startJ++;
            }
            // for every j that is smaller than i compounds have already been found.
            if(startJ < primeFactor) {
                startJ = primeFactor;
            }

            for (long j = startJ; (long) j*primeFactor <= lastNumber; j+=2) {
                isPrime.clear((int) (((long)j*primeFactor - segmentIndexOffset - 1)/2));
            }
        }

        for (int i = 0; i <= isPrime.length(); i++) {
            if(isPrime.get(i)) {
                long prime = (long) i*2 + 1 + segmentIndexOffset;
                long byteOffset = (long) Math.floor((double) prime / 30);
                int remainder = (int) (prime % 30);
                int bitOffset = primeRemainders.indexOf(remainder);
                long initalIndex = byteOffset * 8 + bitOffset;
                if(!offsetInitialized) {
                    resultIndexOffset = initalIndex;
                    offsetInitialized = true;
                }

                if(tempResult.size() == 0 || initalIndex - resultIndexOffset >= Integer.MAX_VALUE) {
                    resultIndexOffset = initalIndex;
                    tempResult.add(new PrimesList(new BitSet(), resultIndexOffset));
                    tempResultIndex++;
                }
                tempResult.get(tempResultIndex).getBitSet().set((int)(initalIndex - resultIndexOffset));
            }
        }
    }

}

// er gebeurd wat als de size groter is dan 3.5 * 10^11
// bij 30 * 10^11 gaat het nog goed. Denk heap size!!!
public class ParallelSegmentedSieveService {

//    int NR_THREADS;
    final static long SIZE = 1000000000;
    static int NR_THREADS = 8;


    public ParallelSegmentedSieveService(int nrThreads) {
        NR_THREADS = nrThreads;
    }

    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        ArrayList<Integer> initalPrimes = findPrimes((int) Math.sqrt(SIZE));
        initalPrimes.remove(0); // remove 2..
        SieveJob sieveJob = new SieveJob(7L, SIZE, initalPrimes);
        ArrayList<ArrayList<PrimesList>> result = performSieve(sieveJob);
        long end = System.currentTimeMillis();
        System.out.println("time taken: " + (end-start));

        long counter = 3; // primes 2, 3, 5
        for(ArrayList<PrimesList> threadPrimesLists : result) {
            for(PrimesList primesList : threadPrimesLists) {

//                primesList.printPrimes();

                for (int i = 0; i <= primesList.getBitSet().length(); i++) {
                    if(primesList.getBitSet().get(i)) {
                        counter++;
                    }
                }
            }
        }
        System.out.println("nr of primes: " + counter);
    }


    public static ArrayList<ArrayList<PrimesList>> performSieve(SieveJob sieveJob) {
        long firstNumber = sieveJob.getFirstNumer();
        long lastNumber = sieveJob.getLastNumber();
        ArrayList<Integer> primeFactors = sieveJob.getPrimeFactor();


        ExecutorService executorService = Executors.newFixedThreadPool(NR_THREADS);
        CountDownLatch countDownLatch = new CountDownLatch(NR_THREADS);

        ArrayList<ArrayList<PrimesList>> result = new ArrayList<ArrayList<PrimesList>>(NR_THREADS);

//         set initial values so that they can be modified later.
        for (int i = 0; i < NR_THREADS; i++) {
            result.add(new ArrayList<PrimesList>());
        }

        long diff = lastNumber - firstNumber;

        for (int i = 0; i < NR_THREADS; i++) {
            long subFirstNumber;
            long subLastNumber;

            if(i == 0) {
                subFirstNumber = firstNumber;
            } else {
                subFirstNumber = firstNumber + (i*(diff/NR_THREADS)) + 1;
            }

            if(i == NR_THREADS-1) {
                subLastNumber = lastNumber;
            } else {
                subLastNumber = firstNumber + (diff/NR_THREADS)*(i+1) ;
            }
            executorService.submit(new SegmentedSieve(subFirstNumber, subLastNumber, primeFactors, countDownLatch, result.get(i)));
        }
        executorService.shutdown();


        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }


    public static ArrayList<Integer> findPrimes(int n) {

        // index offset: index 0: 2. index >0 = 2*i + 1.
        // only store odd indexed.
        int size = ((int) Math.ceil((double) n/2)-1);
        BitSet isPrime = new BitSet(size);
        for (int i = 0; i <= size; i++) {
            isPrime.set(i);
        }

        for (int i = 1; (i*2+1)*(i*2+1) <= n; i++) {
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
}



