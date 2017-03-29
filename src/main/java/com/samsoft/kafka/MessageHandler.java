package com.samsoft.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * @author kumarsambhav.jain
 * @since 3/29/2017.
 */
public class MessageHandler implements Callable<String> {

    private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);
    private final String value;

    public MessageHandler(String value) {
        this.value = value;
    }


    @Override
    public String call() throws Exception {
        log.debug("handled message {}", this.value);
        return this.value;
    }

}
