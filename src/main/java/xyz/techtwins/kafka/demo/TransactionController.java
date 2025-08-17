package xyz.techtwins.kafka.demo;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/transactions")
@AllArgsConstructor
public class TransactionController {

    private final TransactionProducer producer;
    private final TransactionMapper mapper;

    //отправить сообщение
    @PostMapping("/initiate")
    public ResponseEntity<Void> initiate(@RequestBody TransactionDto dto) {
        producer.initiateTransaction(mapper.transactionDtoToTransaction(dto));
        return ResponseEntity.ok().build();
    }

    //отправить сообщение в транзакции
    @PostMapping("/initiate-in-batch")
    public ResponseEntity<Void> send(@RequestBody TransactionDto[] dtos) {
        var transactions = Arrays.stream(dtos).map(mapper::transactionDtoToTransaction);
        producer.initiateTransactionInBatch((transactions.toArray(Transaction[]::new))).join();
        return ResponseEntity.ok().build();
    }

    //отправить сообщение и ждать ответ
    @SneakyThrows
    @PostMapping("/execute")
    public ResponseEntity<Void> execute(@RequestBody TransactionDto dto) {
        producer.executeTransaction(mapper.transactionDtoToTransaction(dto));
        return ResponseEntity.ok().build();
    }
}
