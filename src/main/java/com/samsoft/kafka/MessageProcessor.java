package com.samsoft.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class MessageProcessor implements Callable<ConsumerRecord<String, String>> {
    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private final ConsumerRecord<String, String> record;
    private final KafkaConsumer<String, String> kafkaConsumer;


    MessageProcessor(ConsumerRecord<String, String> record, KafkaConsumer<String, String> kafkaConsumer) {
        this.record = record;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public ConsumerRecord<String, String> call() throws Exception {
        String value = this.record.value();
        log.debug("processing record for topic {}", value);
        try {
            synchronized (kafkaConsumer) {
                log.debug("acquired lock on consumer");
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>(1);
                map.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset(), record.timestamp() + ""));
                log.debug("will try commit syn now.");
                kafkaConsumer.commitSync(map);
            }
            log.debug("consumer lock released");
        } catch (Exception e) {
            log.error("error while consuming msg", e);
        }
        return record;
    }
}
