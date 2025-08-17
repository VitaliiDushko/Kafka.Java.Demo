package xyz.techtwins.kafka.demo;

import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class TransactionService {
    String processTransaction(Transaction transaction) {
        try {
            TimeUnit.SECONDS.sleep(5); // blocking, but offloaded
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return transaction.toString();
    };
}

