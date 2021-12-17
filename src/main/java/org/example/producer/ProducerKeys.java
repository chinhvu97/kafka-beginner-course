package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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

            String topic = "first_topic";
            String value = "hi mom " + i;
            String key = "id_" + i;

            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("key: {}", key);
            //send data - asynchronous
            /*
            0 - 1
            1 - 0
            2 2
            3 0
            4 2
            5 2
            6 0
            7 2
            8 1
            9 2
            */
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
            }); //block the .send() to make it synchronous - dont do in prodection
        }

        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
