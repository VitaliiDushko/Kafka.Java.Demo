package xyz.techtwins.kafka.demo;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
public class TransactionsExecutorService {
    private final ReplyingKafkaTemplate<String, Transaction, Transaction> replyingKafkaTemplate;

    public Transaction executeTransaction(Transaction transaction) throws ExecutionException, InterruptedException {
        ProducerRecord<String, Transaction> record =
                new ProducerRecord<>(transaction.getTransactionId().toString(), transaction);

        RequestReplyFuture<String, Transaction, Transaction> future =
                replyingKafkaTemplate.sendAndReceive(record);
        SendResult<String, Transaction> sendFV = future.getSendFuture().get();
        return future.get().value();
    }
}
