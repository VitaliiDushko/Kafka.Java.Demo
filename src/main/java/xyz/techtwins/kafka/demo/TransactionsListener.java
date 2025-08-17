package xyz.techtwins.kafka.demo;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import java.util.concurrent.ExecutionException;

@Component
@RequiredArgsConstructor
@Getter
@Slf4j
public class TransactionsListener {
    @Value("${spring.kafka.transactions.listener.id}")
    private String listenerId;

    @Value("${spring.kafka.transactions.listener.topic}")
    private String topic;

    @Value("${spring.kafka.transactions.listener.clientIdPrefix}")
    private String clientIdPrefix;

    private final TransactionService transService;

    @KafkaListener(
            id = "#{__listener.listenerId}",
            topics = "#{__listener.topic}",
//            topicPartitions = @TopicPartition(topic = "#{__listener.topic}", partitions = {"0"}),
            clientIdPrefix = "#{__listener.clientIdPrefix}",
            concurrency = "3")
    public void listen(
            Transaction transaction,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) throws ExecutionException, InterruptedException {
        log.info("!!!Thread '{}' partition: '{}' received {}", Thread.currentThread().getName(), partition, transaction);
        transService.processTransaction(transaction);
    }
}
