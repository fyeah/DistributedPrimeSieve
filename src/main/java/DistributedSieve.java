package main.java;

import main.java.models.PrimesList;
import main.java.models.SieveJob;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;


/**
 * Created by yorin on 15-8-17.
 */
public class DistributedSieve {

    static final String TOPIC = "SievingJobs";
    static final String LOCAL_BROKER_URL = "tcp://localhost:61616";
    static final String BROKER_URL = "tcp://52.29.35.197:61616";

    static int NR_DISTRIBUTIONS = 2;
    static int SIZE = 1000;
    static int NR_THREADS = 2;


    //args[0] p: producer, c: consumer
    //args[1]: if p: size. if c: nr of threads.
    public static void main(String[] args) {
        // parse producer or consumer
        // init activemq broker
        // if producer parse size, divide work, add to queue. wait for consumers to be done.
        //
        //
        // if consumer nr_threads. connect to producer activemq broker thing
        // get job of queue. start sieving. message back when done.
        if(args.length < 1) {
            System.out.println("Provide arguments");
            System.exit(0);
        }
        String type = args[0];
        if(type.equals("p")) {
            if(args.length == 2) {
                SIZE = Integer.parseInt(args[1]);
            }
            initProducer();
        } else if(type.equals("c")) {
            if(args.length == 2) {
                NR_THREADS = Integer.parseInt(args[1]);
            }
            initConsumer();
        }
    }

    public static void initProducer() {
        try {

            ArrayList<Integer> initalPrimes = findPrimes(SIZE);


            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    LOCAL_BROKER_URL);

            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);

            Destination destination = session.createQueue(TOPIC);

            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            for (int i = 0; i < NR_DISTRIBUTIONS; i++) {
                long firstNumber;
                long lastNumber;

                if(i == 0) {
                    firstNumber = 0;
                } else {
                    firstNumber = (i*SIZE) + 1;
                }

                if(i == NR_DISTRIBUTIONS-1) {
                    lastNumber = SIZE;
                } else {
                    lastNumber = (i+1) * SIZE ;
                }
                int threadNr = i;
                SieveJob sieveJob = new SieveJob(firstNumber, lastNumber, initalPrimes);
                producer.send(session.createObjectMessage((Serializable) sieveJob));
            }


            // wait till consumers are done... another queue?

            // Clean up
//            session.close();
//            connection.close();



//            result.get(0).add(0, (long) 2);
//            result.get(0).add(1, (long) 3);
//            result.get(0).add(2, (long) 5);
            // final result.. / counter.



        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }

    }


    public static void initConsumer() {
        long start = System.currentTimeMillis();

        ParallelSegmentedSieveService parallelSegmentedSieveService = new ParallelSegmentedSieveService(NR_THREADS);

        try {

            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    BROKER_URL);

            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();

            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException e) {
                    System.out.println("Error occured!");
                    e.printStackTrace();

                    // reconnect...?
                }
            });

            // Create a Session
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(TOPIC);

            // Create a MessageConsumer from the Session to the Topic or
            // Queue
            MessageConsumer consumer = session.createConsumer(destination);

            // Wait for a message
            Message message = consumer.receive();

            if (message instanceof ObjectMessage) {
                ObjectMessage objectMessage = (ObjectMessage) message;
                SieveJob sieveJob = (SieveJob) objectMessage.getObject();
                System.out.println("Received. First number: " + sieveJob.getFirstNumer());

                ArrayList<ArrayList<PrimesList>> result = parallelSegmentedSieveService.performSieve(sieveJob);

                long end = System.currentTimeMillis();
                System.out.println("time taken: " + (end-start));
            } else {
                System.out.println("RECEIVED OTHER TYPE OF MESSAGE!");
                System.out.println("Received: " + message);
            }


            // notify producer I'm done.

            consumer.close();
            session.close();
            connection.close();
        } catch (Exception e) {
            System.out.println("Caught: " + e);
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
