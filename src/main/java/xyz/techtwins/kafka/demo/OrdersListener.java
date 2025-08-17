package xyz.techtwins.kafka.demo;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
@RequiredArgsConstructor
@Getter
public class OrdersListener {
    @Value("${spring.kafka.orders.listener.id}")
    private String listenerId;

    @Value("${spring.kafka.orders.listener.topic}")
    private String topic;

    @Value("${spring.kafka.orders.listener.clientIdPrefix}")
    private String clientIdPrefix;

    private final OrdersService ordersService;

    @KafkaListener(
            id = "#{__listener.listenerId}",
            topics = "#{__listener.topic}",
            clientIdPrefix = "#{__listener.clientIdPrefix}")
    public void listen(Order order) throws ExecutionException, InterruptedException {
        ordersService.processOrder(order);
    }
}
