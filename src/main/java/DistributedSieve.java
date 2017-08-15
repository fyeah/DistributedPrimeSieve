package main.java;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Created by yorin on 15-8-17.
 */
public class DistributedSieve {

    static final String TOPIC = "SievingJobs";

    public static void main(String[] args) {
        // parse producer or consumer
        // init activemq broker
        // if producer parse size, divide work, add to queue. wait for consumers to be done.
        //
        //
        // if consumer nr_threads. connect to producer activemq broker thing
        // get job of queue. start sieving. message back when done.
        String type = args[0];
        if(type == "p") {
            initProducer();
        } else if(type == "c") {
            initConsumer();
        }
    }

    public static void initProducer() {
        try {
            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    "tcp://localhost:61616");

            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();

            // Create a Session
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(TOPIC);

            // Create a MessageProducer from the Session to the Topic or
            // Queue
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            // Create a messages
            String text = "Hello world! From: "
                    + Thread.currentThread().getName() + " : Producer";
            TextMessage message = session.createTextMessage(text);

            // Tell the producer to send the message
            System.out.println("Sent message: " + message.hashCode()
                    + " : " + Thread.currentThread().getName());
            producer.send(message);

            // Clean up
            session.close();
            connection.close();
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }

    }


    public static void initConsumer() {
        try {

            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    "tcp://52.29.35.197:61616");

            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();

            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException e) {
                    System.out.println("Error occured!");
                    e.printStackTrace();
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
            Message message = consumer.receive(1000);

            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String text = textMessage.getText();
                System.out.println("Received: " + text);
            } else {
                System.out.println("Received: " + message);
            }

            consumer.close();
            session.close();
            connection.close();
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    public synchronized void onException(JMSException ex) {
        System.out.println("JMS Exception occured.  Shutting down client.");
    }
}
