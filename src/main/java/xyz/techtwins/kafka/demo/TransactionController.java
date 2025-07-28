package xyz.techtwins.kafka.demo;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final TransactionProducer producer;

    public TransactionController(TransactionProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/send")
    public ResponseEntity<Void> send(@RequestBody TransactionDto dto) {
        var transaction = Transaction.newBuilder()
                .setTransactionId(dto.getId())
                .setStatus(Status.PENDING)
                .setAmount(dto.getAmount())
                .setCurrency(dto.getCurrency())
                .setUserId(dto.getCurrency())
                .setTimestamp(System.currentTimeMillis())
                .build();

        producer.sendTransaction(transaction);
        return ResponseEntity.ok().build();
    }
}
