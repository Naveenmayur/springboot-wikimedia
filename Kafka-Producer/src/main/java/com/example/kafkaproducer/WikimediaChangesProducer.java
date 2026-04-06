package com.example.kafkaproducer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    @Value("${kafka.topic.name}")
    private String topicName;

    @Value("${wikimedia.url}")
    private String wikimediaUrl;
    private KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) throws InterruptedException {
        LOGGER.info("Sending message to Kafka topic {}: {}", topicName, message);

        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topicName);

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler,
                new EventSource.Builder(URI.create(wikimediaUrl)));
        BackgroundEventSource eventSource = builder.build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }

}
