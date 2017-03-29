package com.samsoft.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * Subscribe to multiple topics / pattern and process message in worker thread.
 * <p>
 * Created by kumarsambhav.jain on 3/22/2017.
 */
public class SingleThreadConsumer {

    private static final Logger log = LoggerFactory.getLogger(SingleThreadConsumer.class);

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        log.debug("started SingleThreadConsumer");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("metadata.max.age.ms", 60_000); // how often check for new topics that might match the pattern
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props)) {

            // SUBSCRIBE to a pattern and not a single topic
            kafkaConsumer.subscribe(Pattern.compile("N6_.*"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    log.debug("partition revoked for {}", collection);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    log.debug("partition assigned for {}", collection);
                }
            });
            ExecutorService executorService = Executors.newFixedThreadPool(10);
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(3000);
                int count = records.count();
                log.debug("will process {} records", count);
                records.forEach(r -> executorService.submit(new MessageProcessor(r, kafkaConsumer)));
            }
        } catch (Exception ex) {
            log.error("error while consuming message.", ex);
        } finally {
            log.debug("shutting down");
        }
    }
}
