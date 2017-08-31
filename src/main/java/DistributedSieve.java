package main.java;

import main.java.models.PrimesList;
import main.java.models.PrimesListsResult;
import main.java.models.SieveJob;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.BlobMessage;

import javax.jms.*;
import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;





/**
 * Created by yorin on 15-8-17.
 */
public class DistributedSieve {

    static final String QUEUE_NAME = "SievingJobs";
    static final String TOPIC_NAME = "ResultPrimes";
    static final String LOCAL_BROKER_URL = "tcp://localhost:61616";
//    static final String BROKER_URL = "tcp://localhost:61616";
    static final String BROKER_URL = "tcp://35.156.159.190:61616";

    static int NR_DISTRIBUTIONS = 2;
    static long SIZE = 2000L;
    static int NR_THREADS = 2;


    //args[0] p: producer, c: consumer
    //args[1]: if p: size. if c: nr of threads.
    //args[2]: if p: number of distributions
    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Provide arguments");
            System.exit(0);
        }
        String type = args[0];
        if(type.equals("p")) {
            if(args.length == 2) {
                SIZE = Long.parseLong(args[1]);
            }
            if(args.length == 3) {
                SIZE = Long.parseLong(args[1]);
                NR_DISTRIBUTIONS = Integer.parseInt(args[2]);
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
            long start = System.currentTimeMillis();
            ArrayList<Integer> initalPrimes = findPrimes((int) Math.sqrt(SIZE));
            initalPrimes.remove(0); // remove 2.


            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    LOCAL_BROKER_URL);

            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);

            Destination destination = session.createQueue(QUEUE_NAME);

            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            long distributionSize = SIZE / NR_DISTRIBUTIONS;

            for (int i = 0; i < NR_DISTRIBUTIONS; i++) {
                long firstNumber;
                long lastNumber;

                if(i == 0) {
                    firstNumber = 7; //start at 7. add 2, 3, 5 at the end.
                } else {
                    firstNumber = (i*distributionSize) + 1;
                }

                if(i == NR_DISTRIBUTIONS-1) {
                    lastNumber = SIZE;
                } else {
                    lastNumber = (i+1) * distributionSize ;
                }

                System.out.println(firstNumber);
                System.out.println(lastNumber);
                SieveJob sieveJob = new SieveJob(firstNumber, lastNumber, initalPrimes);
                producer.send(session.createObjectMessage(sieveJob));
            }


            Destination resultDestination = session.createTopic(TOPIC_NAME);

            MessageConsumer resultConsumer = session.createConsumer(resultDestination);

            int received = 0;
            long counter = 3; // primes 2, 3, 5
            long biggest;

            while(received != NR_DISTRIBUTIONS) {
                // Wait for a message
                Message resultMessage = resultConsumer.receive();

                if (resultMessage instanceof ObjectMessage) {
                    ObjectMessage objectMessage = (ObjectMessage) resultMessage;
                    PrimesListsResult primesListsResult = (PrimesListsResult) objectMessage.getObject();
//                    Byte[] bytes = new Byte[(int)bytesMessage.getBodyLength()];
//                    bytesMessage.readBytes(bytes);
//                    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream();
//
//                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//                    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(byteArrayOutputStream);
//                    bytesMessage.setObjectProperty("JMS_AMQ_SaveStream", bufferedOutputStream);
//
//                    objectOutputStream.writeObject(primesListsResult);
//                    objectOutputStream.flush();
//                    objectOutputStream.close();


//                    PrimesListsResult primesListsResult = (PrimesListsResult) bytesMessage.setObjectProperty();
                    ArrayList<ArrayList<PrimesList>> result = primesListsResult.getPrimesListsResult();

                    for(ArrayList<PrimesList> threadPrimesLists : result) {
                        for(PrimesList primesList : threadPrimesLists) {
                            //                        primesList.printPrimes();
                            for (int i = 0; i <= primesList.getBitSet().length(); i++) {
                                if(primesList.getBitSet().get(i)) {
                                    counter++;
                                }
                            }
                        }
                    }
                    received++;
                } else {
                    System.out.println("Received different message..!");
                }
            }

            long end = System.currentTimeMillis();


            System.out.println("nr of primes: " + counter);

            System.out.println("time taken including counting: " + (end-start));

            // wait till consumers are done... another queue?

            // Clean up
            session.close();
            connection.close();



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
            Destination destination = session.createQueue(QUEUE_NAME);

            // Create a MessageConsumer from the Session to the Topic or
            // Queue
            MessageConsumer consumer = session.createConsumer(destination);

            // Wait for a message
            Message message = consumer.receive();

            if (message instanceof ObjectMessage) {
                ObjectMessage objectMessage = (ObjectMessage) message;
                SieveJob sieveJob = (SieveJob) objectMessage.getObject();
                System.out.println("Received. First number: " + sieveJob.getFirstNumer());

                PrimesListsResult primesListsResult = parallelSegmentedSieveService.performSieve(sieveJob);

                Destination resultDestination = session.createTopic(TOPIC_NAME);

                MessageProducer resultProducer = session.createProducer(resultDestination);

//                PrimesListsResult primesListsResult = new PrimesLists(primesListsResult.getPrimesListsResult().get(0));

//                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
//                objectOutputStream.writeObject(primesListsResult);
//                objectOutputStream.flush();
//                objectOutputStream.close();


//                InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
//                bytesMessage.writeBytes(byteArrayOutputStream.toByteArray());
//
//                BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

//                session.createStreamMessage();

//                BytesMessage bytesMessage = session.createBytesMessage();
//                bytesMessage.setObjectProperty("JMS_AMQ_InputStream", bufferedInputStream);

//                resultProducer.send(bytesMessage);

                resultProducer.send(session.createObjectMessage(primesListsResult));

//                ArrayList<ArrayList<PrimesList>> result = primesListsResult.getPrimesListsResult();

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
