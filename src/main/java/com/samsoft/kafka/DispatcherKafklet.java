package com.samsoft.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kumarsambhav.jain
 * @since 3/29/2017.
 */
public final class DispatcherKafklet implements Runnable {

    private final Set<String> subscribedTopics = Collections.synchronizedSet(new HashSet<>());
    private static final Logger log = LoggerFactory.getLogger(DispatcherKafklet.class);
    private static final ConsumerRebalanceListener consumerRebalanceListener;
    private static final Properties props;
    private AtomicBoolean started = new AtomicBoolean(false);
    private KafkaConsumer<String, String> kafkaConsumer;
    private final Map<String, ExecutorService> workerPoolByTopic;
    private final AtomicBoolean refresh = new AtomicBoolean(false);

    static {
        consumerRebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.debug("onPartitionsRevoked");

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.debug("onPartitionsAssigned");
            }
        };

        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");  // auto or manual commit
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }


    public DispatcherKafklet() {
        workerPoolByTopic = new HashMap<>();
    }

    /**
     * incremental topic subscriber to kafka
     *
     * @param topic
     */
    public void subscribe(String topic) throws InterruptedException {
        log.debug("will try to subscribe to {}", topic);
        subscribedTopics.add(topic);
        refresh.set(true);
    }

    public synchronized void start() throws InterruptedException {
        boolean alreadyStarted = this.started.getAndSet(true);
        if (alreadyStarted) {
            log.warn("DispatcherKafklet already started");
        } else {
            log.debug("starting DispatcherKafklet");
            try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props)) {
                this.kafkaConsumer = kafkaConsumer;
                while (subscribedTopics.isEmpty()) {
                    log.debug("no topics subscribed yet, will wait");
                    Thread.sleep(2000);
                }
                log.debug("starting poll loop");
                while (started.get()) {
                    if (this.refresh.compareAndSet(true, false)) {
                        kafkaConsumer.subscribe(subscribedTopics, consumerRebalanceListener);
                    }
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(2000);
                    int count = records.count();
                    log.debug("polled {} records", count);
                    records.forEach(this::handleRecord);
                }
                kafkaConsumer.unsubscribe();
                subscribedTopics.clear();
            } catch (Exception e) {
                log.error("error while polling", e);
            } finally {
                log.debug("stopped polling");
            }
        }

    }

    private void handleRecord(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        String value = record.value();
        ExecutorService service = workerPoolByTopic.computeIfAbsent(topic, t -> Executors.newCachedThreadPool());
        service.submit(new MessageHandler(value));
    }


    public void stop() {
        started.set(false);
        workerPoolByTopic.values().forEach(ExecutorService::shutdown);
    }

    @Override
    public void run() {
        log.debug("running DispatcherKafklet thread.");
        try {
            start();
        } catch (InterruptedException e) {
            log.error("error", e);
        }
    }
}
