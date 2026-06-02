package com.exemplo.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.pedidos}")
    private String topicPedidos;

    @Value("${kafka.topic.notificacoes}")
    private String topicNotificacoes;

    @Bean
    public NewTopic topicPedidos() {
        return TopicBuilder.name(topicPedidos)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topicNotificacoes() {
        return TopicBuilder.name(topicNotificacoes)
                .partitions(2)
                .replicas(1)
                .build();
    }
}
