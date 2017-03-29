package com.samsoft.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumarsambhav.jain
 * @since 3/29/2017.
 */
public class DispatcherKafkletSample {

    private static final Logger log = LoggerFactory.getLogger(DispatcherKafkletSample.class);

    public static void main(String[] args) throws InterruptedException {
        log.debug("Starting DispatcherKafkletSample application");
        DispatcherKafklet kafklet = new DispatcherKafklet();
        Thread kafkletThread = new Thread(kafklet);
        kafklet.subscribe("tp1");
        kafklet.subscribe("tp2");
        kafkletThread.start();
        Thread.sleep(10_000);
        kafklet.subscribe("tp3"); // here it will fail.
        kafkletThread.join();
        log.debug("consumer thread exit. shutting down");
    }
}
