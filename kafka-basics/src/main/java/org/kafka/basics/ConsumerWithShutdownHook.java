package org.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithShutdownHook {

    public static void main(String[] args) {

        //Create properties for consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //Security props for conductor pg.

        //Deserialize data from topic, should be same as that of producer.
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        String consumerGrpID = "kafka-basics";
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGrpID);

        /*
        * 3 possible values of AUTO_OFFSET_RESET_CONFIG - none/earliest/latest
        * none - If we don't have any existing consumer group we fail.
        * earliest - We want to read data from beginning (correlates to --from-beginning flag in kafka-console-consumer)
        * latest - We want to read the new data produced to the topic.
        */
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Shutdown Hook -
        //create a reference to main thread
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                System.out.println("Shutdown Detected");
                consumer.wakeup();
                //consumer.wakeup() and join the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        //consumer.wakeup() will make consumer.poll() to throw Wakeup exp, i.e. an exception is expected in while true


        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList("confluent.kafka.basics"));
            //can pass multiple topics as a list above.

            //poll data from kafka topic
            //poll indefinitely
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Key: " + record.key());
                    System.out.println("Value: " + record.value());
                    System.out.println("Offset: " + record.offset());
                    System.out.println("Read from Partition: " + record.partition());
                }

            }
            //As soon as we consume the offsets are committed, now if the consumer is restarted then it'll start with the
            //last committed offsets.
        } catch (WakeupException w) {
            System.out.println("Consumer is shutting down");
        } catch (Exception e) {
            System.out.println("Unexpected Exception " + e);
        } finally {
            consumer.close();
            //close consumer and commit offsets
            System.out.println("Consumer shutdown successful");
        }
    }
}
