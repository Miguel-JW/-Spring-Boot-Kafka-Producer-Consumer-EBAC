package com.exemplo.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(
        topics = "${kafka.topic.pedidos}",
        groupId = "grupo-app"
    )
    public void consumirPedido(
            @Payload String mensagem,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int particao,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[CONSUMER - pedidos] Mensagem: '{}' | Partição: {} | Offset: {}",
                mensagem, particao, offset);
    }

    @KafkaListener(
        topics = "${kafka.topic.notificacoes}",
        groupId = "grupo-app"
    )
    public void consumirNotificacao(
            @Payload String mensagem,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int particao,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[CONSUMER - notificacoes] Mensagem: '{}' | Partição: {} | Offset: {}",
                mensagem, particao, offset);
    }
}
