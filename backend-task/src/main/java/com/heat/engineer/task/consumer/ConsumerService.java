package com.heat.engineer.task.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private final Map<String, Object> latestStockData = new ConcurrentHashMap<>(); // Store latest stock data

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "stock-consumer-group")
    public void consumeStockPrice(String message) {
        try {
            // Parse message
            Map<String, Object> stockData = new ObjectMapper().readValue(message, Map.class);

            // Update latest stock data
            latestStockData.putAll(stockData);
            logger.info("Consumed stock data: {}", stockData);
        } catch (Exception e) {
            logger.error("Failed to consume stock price: ", e);
        }
    }

    public Map<String, Object> getLatestStockData() {
        return latestStockData;
    }
}
