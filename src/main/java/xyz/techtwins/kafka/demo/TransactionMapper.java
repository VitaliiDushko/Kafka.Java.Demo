package xyz.techtwins.kafka.demo;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface TransactionMapper {

    @Mapping(source = "id", target = "transactionId")
    @Mapping(target = "status", expression = "java(Status.PENDING)")
    @Mapping(target = "timestamp", expression = "java(System.currentTimeMillis())")
    Transaction transactionDtoToTransaction(TransactionDto dto);
}
