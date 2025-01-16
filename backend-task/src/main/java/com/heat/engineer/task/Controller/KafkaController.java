package com.heat.engineer.task.Controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.heat.engineer.task.consumer.ConsumerService;

@RestController
@RequestMapping("/api")
public class KafkaController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Autowired
    private ConsumerService consumerService;
    
    @GetMapping("/health")
    public String healthCheck() {
        try {
            kafkaTemplate.send("health-check-topic", "Health Check").get();
            return "Kafka is reachable!";
        } catch (Exception e) {
            return "Kafka connection failed: " + e.getMessage();
        }
    }
    
    @GetMapping("/{stockSymbol}")
    public ResponseEntity<Map<String, Object>> getStockPrice(@PathVariable String stockSymbol) {
        // Mock stock price data
        double stockPrice = Math.random() * 1000; // Random price for demo
        Map<String, Object> response = new HashMap<>();
        response.put("symbol", stockSymbol);
        response.put("price", stockPrice);
        response.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(response);
}
    
    @GetMapping("/latest-stock-price")
    public ResponseEntity<Map<String, Object>> getLatestStockPrice() {
        Map<String, Object> stockData = consumerService.getLatestStockData();
        if (stockData.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(stockData);
    }
}
