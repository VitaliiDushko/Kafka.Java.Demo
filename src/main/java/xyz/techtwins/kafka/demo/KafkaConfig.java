package xyz.techtwins.kafka.demo;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.reply.group-id}")
    private String replyGroupId;

    @Bean
    public NewTopic initiatedTopic() {
        return new NewTopic("transactions.initiated", 3, (short) 1);
    }

    @Bean
    public KafkaTemplate<String, Transaction> kafkaTemplate(ProducerFactory<String, Transaction> pf) {
        KafkaTemplate<String, Transaction> template = new KafkaTemplate<>(pf);
        template.setAllowNonTransactional(true); // разрешить send без транзакции
        return template;
    }

    @Bean
    public ReplyingKafkaTemplate<String, Transaction, Transaction> replyingKafkaTemplate(
            ProducerFactory<String, Transaction> pf,
            ConcurrentKafkaListenerContainerFactory<String, Transaction> factory) {

        ConcurrentMessageListenerContainer<String, Transaction> replyContainer =
                factory.createContainer("transactions.execute.reply");
        replyContainer.getContainerProperties().setGroupId(replyGroupId);
        var replyingTemplate = new ReplyingKafkaTemplate<>(pf, replyContainer);
        replyingTemplate.setAllowNonTransactional(true);
        return replyingTemplate;
    }
}
