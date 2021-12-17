package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroupAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class);
        String bootstrapServers = "localhost:9092";
        String topic = "first_topic";
        // consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to topic(s)
//        consumer.subscribe(Collections.singleton(topic));

        //assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 1);
        consumer.assign(Arrays.asList(partitionToReadFrom));
        long offsetToReadFrom = 5L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesAlreadyRead = 0;
        // poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record: records) {
                numberOfMessagesAlreadyRead++;
                logger.info("Key: {}, Value: {}, Partition: {}, Offset: {}",
                        record.key(), record.value(), record.partition(), record.offset());
                if (numberOfMessagesAlreadyRead >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

    }
}
