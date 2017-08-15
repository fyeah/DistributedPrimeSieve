package main.java;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yorin on 24-7-17.
 */



//    distribute n over multiple computers (4?)
//    if n/4 exceeds integer.MAX divide n/4 by integer.MAX and make subLists/segments that run the compositeMarker parallel!x

//    is it worth to use maximum segment size as L1 cache size? Is it worth to run these segments parallely? or just serial?
//    TEST THOSE! Write in report!


// total size necessary?
class CompositeMarker implements Runnable {
    private final int NR_THREADS = 4;
    private int startIndex;
    private int endIndex;
    private ArrayList<Integer> primeFactors;
    private int size; // size of this CompositeMarker's subset
    private int indexOffset;

    private BitSet isPrime;
    private ArrayList<Integer> result = new ArrayList<>();

    private CountDownLatch countDownLatch;

    public CompositeMarker(int startIndex, int endIndex, ArrayList<Integer> primeFactors, CountDownLatch countDownLatch) {
        this.startIndex = startIndex; // has to be even
        this.endIndex = endIndex; // has to be uneven
        this.primeFactors = primeFactors;
        this.countDownLatch = countDownLatch;
        init();
    }

    private void init() {
        int diff = endIndex - startIndex;
        if(startIndex%2 != 0 && endIndex%2 !=0) {
            size = ((int) Math.ceil((double) diff/2));
        } else {
            size = ((int) Math.ceil((double) diff/2)-1);
        }
//
//        if(size >= Integer.MAX_VALUE) {
//            int nrOfSegments = (int) size / Integer.MAX_VALUE;
//            int segmentSize = (int) size /nrOfSegments;
//
//            if(nrOfSegments > NR_THREADS) {
//                // spawn threads that take care of segments.
//            }
//        }



        indexOffset = startIndex;
        if(indexOffset % 2 != 0) {
            indexOffset--;
        }
        isPrime = new BitSet(size);
        for (int i = 0; i <= size; i++) {
            isPrime.set(i);
        }

    }


    @Override
    public void run() {
        for(Integer primeFactor : primeFactors) {
            markComposites(primeFactor);
        }

        if(startIndex <= 2) {
            result.add(2);
        }

        for (int i = 0; i <= isPrime.length(); i++) {
            if(isPrime.get(i) == true) {
                result.add(i*2 + 1 + indexOffset);
            }
        }
        countDownLatch.countDown();
    }

    public ArrayList<Integer> getPrimes() {
        return result;
    }

    private void markComposites(int primeFactor) {
        int startJ = (int) Math.ceil((double)startIndex/primeFactor);
        // we also want it to be uneven.
        if(startJ%2 == 0) {
            startJ++;
        }
        // for every j that is smaller than i..compounds have already been found.
        if(startJ < primeFactor) {
            startJ = primeFactor;
        }
        for (int j = startJ; j*primeFactor <= endIndex; j+=2) {
            isPrime.clear((j*primeFactor-indexOffset-1)/2);
        }
    }
}


public class SieveParallel {

    static final int NR_THREADS = 4;
    static final int SIZE = 1000000000;

    public static void main(String[] args) {
        // don't run number 2.

        // don't need list with numbers here..? jut incrementing integer?

        // ask sublist whether number is true or false? get sublist back when worker node is done?
        // or get sublist when you know you're gonna need it soon..?
        // like if you know that the composite prime is getting bigger than the sublist's range..you don't need to
        // let that sublist do shit.

        long start = System.currentTimeMillis();

        ArrayList<Integer> initialPrimes = findPrimes((int) Math.sqrt(SIZE));
        initialPrimes.remove(0); //remove 2.
        CountDownLatch countDownLatch = new CountDownLatch(NR_THREADS);
//        ExecutorService executorService = Executors.newFixedThreadPool(NR_THREADS);
//
//        for (int i = 1; i <= NR_THREADS; i++) {
//            if(i == 1) {
//                executorService.submit(new CompositeMarker(2, (int)SIZE/NR_THREADS, initialPrimes, countDownLatch));
//            } else {
//                int startIndex = (int) (SIZE/NR_THREADS)*(i-1)+1;
//                int endIndex = (int) (SIZE/NR_THREADS)*i;
////                System.out.println(startIndex);
////                System.out.println(endIndex);
//                executorService.submit(new CompositeMarker(startIndex, endIndex, initialPrimes, countDownLatch));
//            }
//        }
//
//        executorService.shutdown();

        CompositeMarker compositeMarker = new CompositeMarker(2, SIZE/4, initialPrimes, countDownLatch);
        CompositeMarker compositeMarker2 = new CompositeMarker(SIZE/4+1, (SIZE/4)*2, initialPrimes, countDownLatch);
        CompositeMarker compositeMarker3 = new CompositeMarker((SIZE/4)*2+1, (SIZE/4)*3, initialPrimes, countDownLatch);
        CompositeMarker compositeMarker4 = new CompositeMarker((SIZE/4)*3+1, SIZE, initialPrimes, countDownLatch);


        compositeMarker.run();
        compositeMarker2.run();
        compositeMarker3.run();
        compositeMarker4.run();

        try {
            countDownLatch.await();
//            ArrayList<Integer> list1 = compositeMarker.getPrimes();
//            ArrayList<Integer> list2 = compositeMarker2.getPrimes();
//            ArrayList<Integer> list3 = compositeMarker3.getPrimes();
//            ArrayList<Integer> list4 = compositeMarker4.getPrimes();
//            list1.addAll(list2);
//            list1.addAll(list3);
//            list1.addAll(list4);

            long end = System.currentTimeMillis();

            System.out.println(end-start);
//            int totalSize = list1.size() + list2.size() + list3.size() + list4.size();

//            System.out.println("nr of primes: " + totalSize);

//            System.out.println("Largest prime:" + list4.get(list4.size()-1));

//            for(Integer prime : list1) {
//                System.out.println(prime);
//            }


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

}
