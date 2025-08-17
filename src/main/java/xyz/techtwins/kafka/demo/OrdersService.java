package xyz.techtwins.kafka.demo;

import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class OrdersService {
    String processOrder(Order order) {
        try {
            TimeUnit.SECONDS.sleep(5); // blocking, but offloaded
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return order.toString();
    };
}

