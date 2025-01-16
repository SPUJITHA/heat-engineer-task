package com.heat.engineer.task.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

@Service
public class ProducerService {
    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);

    @Value("${stock.api.url}")
    private String stockApiUrl;

    @Value("${kafka.topic.name}")
    private String topicName;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate;

    public ProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.restTemplate = new RestTemplate();
    }

    @Scheduled(fixedRate = 5000) // Fetch and publish every 5 seconds
    public void fetchAndSendStockPrice() {
        try {
            // Fetch stock price from external API
            Map<String, Object> stockData = restTemplate.getForObject(stockApiUrl, HashMap.class);

            if (stockData != null) {
                // Publish stock data to Kafka
                kafkaTemplate.send(topicName, new ObjectMapper().writeValueAsString(stockData));
                logger.info("Published stock data: {}", stockData);
            }
        } catch (Exception e) {
            logger.error("Failed to fetch or publish stock price: ", e);
        }
    }
}
