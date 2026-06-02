package com.exemplo.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.pedidos}")
    private String topicPedidos;

    @Value("${kafka.topic.notificacoes}")
    private String topicNotificacoes;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void enviarPedido(String mensagem) {
        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topicPedidos, mensagem);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[PRODUCER] Erro ao enviar para '{}': {}", topicPedidos, ex.getMessage());
            } else {
                log.info("[PRODUCER] Mensagem enviada para '{}' | Partição: {} | Offset: {}",
                        topicPedidos,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    public void enviarNotificacao(String mensagem) {
        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topicNotificacoes, mensagem);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[PRODUCER] Erro ao enviar para '{}': {}", topicNotificacoes, ex.getMessage());
            } else {
                log.info("[PRODUCER] Mensagem enviada para '{}' | Partição: {} | Offset: {}",
                        topicNotificacoes,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
