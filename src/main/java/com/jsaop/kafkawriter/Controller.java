package com.jsaop.kafkawriter;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

@EnableKafka
@RestController
public class Controller {

    private final
    KafkaTemplate<String, byte[]> output;

    @Autowired
    public Controller(KafkaTemplate<String, byte[]> output) {
        this.output = output;
    }

    @PostMapping("/topics/{topic}/data")
    public void produce(@PathVariable String topic, @RequestBody String data) throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<String, byte[]>> future = output.send(topic, data.getBytes());

        ProducerRecord<String, byte[]> producerRecord = future.get().getProducerRecord();
        System.out.printf("%s: %s%n", producerRecord.topic(), Arrays.toString(producerRecord.value()));
    }

}
