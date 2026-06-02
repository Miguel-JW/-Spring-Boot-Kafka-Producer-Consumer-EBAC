package com.exemplo.kafka.controller;

import com.exemplo.kafka.dto.MensagemRequest;
import com.exemplo.kafka.producer.KafkaProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaProducer producer;

    public KafkaController(KafkaProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/pedidos")
    public ResponseEntity<String> enviarPedido(@RequestBody MensagemRequest request) {
        producer.enviarPedido(request.getConteudo());
        return ResponseEntity.ok("Mensagem enviada para o tópico 'pedidos': " + request.getConteudo());
    }

    @PostMapping("/notificacoes")
    public ResponseEntity<String> enviarNotificacao(@RequestBody MensagemRequest request) {
        producer.enviarNotificacao(request.getConteudo());
        return ResponseEntity.ok("Mensagem enviada para o tópico 'notificacoes': " + request.getConteudo());
    }
}
