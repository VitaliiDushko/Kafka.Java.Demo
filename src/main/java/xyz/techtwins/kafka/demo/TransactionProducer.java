package xyz.techtwins.kafka.demo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionProducer {

    @Value("${spring.kafka.transactions.topic}")
    private String topicName;

    @Value("${spring.kafka.execute.transactions.topic}")
    private String executeTransactionTopicName;

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final ReplyingKafkaTemplate<String, Transaction, Transaction> kafkaReplyingTemplate;

    public CompletableFuture<SendResult<String, Transaction>> initiateTransaction(Transaction transaction) {
        return kafkaTemplate.send(topicName, transaction.getUserId().toString(), transaction);
    }

    public CompletableFuture<Void> initiateTransactionInBatch(Transaction[] transactions) {
        return kafkaTemplate.executeInTransaction(operations -> {
            List<CompletableFuture<SendResult<String, Transaction>>> futures =
                    Arrays.stream(transactions)
                            .map(tx -> operations.send(topicName, tx.getUserId().toString(), tx))
                            .toList();
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        });
    }

    public void executeTransaction(Transaction transaction) throws ExecutionException, InterruptedException {
        ProducerRecord<String, Transaction> record = new ProducerRecord<>("transactions.execute", transaction.getTransactionId().toString(), transaction);
        RequestReplyFuture<String, Transaction, Transaction> replyFuture = kafkaReplyingTemplate.sendAndReceive(record, Duration.ofSeconds(15));
        SendResult<String, Transaction> sendResult = replyFuture.getSendFuture().get();
        ConsumerRecord<String, Transaction> consumerRecord = replyFuture.get();
        var correlationIdHeader = consumerRecord.headers().lastHeader("kafka_correlationId");
        ByteBuffer byteBuffer = ByteBuffer.wrap(correlationIdHeader.value());
        UUID corrUuid = new UUID(byteBuffer.getLong(), byteBuffer.getLong());
        log.info("transaction {} with correlation id {} executed!!!", transaction.getTransactionId(), corrUuid);
    }
}
