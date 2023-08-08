package org.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerWithCallbacks {

    public static void main(String[] args) {



        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.setProperty(ProducerConfig.SECURITY_PROVIDERS_CONFIG, "SASL_SSL");

        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "500");
        //To send batches to different partitions (not recommended)
        //properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        //Producing with keys - same key goes to same partition. If no key is passed then data will go RoundRobin
        ProducerRecord<String, String> record = new ProducerRecord<>("confluent.kafka.basics",
                                                                                   "id",
                                                                                   "12345");
        ProducerRecord<String, String> record1 = new ProducerRecord<>("confluent.kafka.basics",
                "id",
                "67890");

        //send data
        //producer and producer1 will goto the same partition as they are produced with same key
        producer.send(record);
        producer.send(record1, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes everytime a record is processed
                if (e == null){
                    System.out.println("Metadata received \n" +
                             "Topic:" + recordMetadata.topic() + "\n" +
                             "Partition: " +recordMetadata.partition() + "\n" +
                             "Offset: " + recordMetadata.offset() + "\n" +
                             "Timestamp: " + recordMetadata.timestamp() + "\n");

                }
                else {
                    System.out.println("Error while processing");
                }
            }
        });

        //producing multiple records at the same time - Use a for loop after creating producer record.
        //Concept of StickyPartitioner - performance efficiency - batches are produced to a topic-partition instead of
        //each chunk of data to a different partition in default RoundRobinPartitioner.
        for (int i=0; i<10; i++){
            for (int j=0; j<30; j++){
                ProducerRecord<String, String> record2 = new ProducerRecord<>("confluent.kafka.basics", "Hi" + i);
                producer.send(record2);
            }
            //Next batch will goto a different partition
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        //flush and close
        producer.flush();
        producer.close();
    }
}