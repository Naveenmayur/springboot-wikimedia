package com.example.kafkaproducer;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import okhttp3.Headers;
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
    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {
        LOGGER.info("Starting Wikimedia changes producer for topic: {}", topicName);

        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topicName);

        // Build the required headers (User-Agent is mandatory)
        Headers headers = new Headers.Builder()
                .add("User-Agent", "KafkaWikimediaProducer/1.0 (Bengaluru; https://github.com/naveenmayurkr; naveenmayurkr@gmail.com)")
                .add("Accept", "text/event-stream")
                .build();

        // Correct way in v4.x - set headers on HttpConnectStrategy
        ConnectStrategy connectStrategy = ConnectStrategy.http(URI.create(wikimediaUrl))
                .headers(headers);                    // ← This is the correct method now

        EventSource.Builder eventSourceBuilder = new EventSource.Builder(connectStrategy);

        BackgroundEventSource eventSource = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder)
                .build();

        eventSource.start();

        LOGGER.info("✅ Connected to Wikimedia stream. Listening for real-time changes...");

        // Keep the application running
        TimeUnit.SECONDS.sleep(5);
    }
}
