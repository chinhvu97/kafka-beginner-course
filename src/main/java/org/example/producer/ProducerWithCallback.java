package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

        String bootstrapServer = "localhost:9092";
        // create producer prop
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i< 10; i++) {

            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hi mom I'm on kafka again");
            //send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //record sent successfully
                        logger.info("Received new metadata. \n" +
                                "Topic: {} \n" +
                                "Partition: {} \n" +
                                "Offset: {} \n" +
                                "Timestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
