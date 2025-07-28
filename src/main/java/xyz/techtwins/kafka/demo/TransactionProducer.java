package xyz.techtwins.kafka.demo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TransactionProducer {

    @Value("${spring.kafka.transactions.topic}")
    private String topicName;

    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public TransactionProducer(KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTransaction(Transaction transaction) {
        kafkaTemplate.send(topicName, transaction.getTransactionId().toString(), transaction);
        System.out.println("Sent transaction: " + transaction);
    }
}
