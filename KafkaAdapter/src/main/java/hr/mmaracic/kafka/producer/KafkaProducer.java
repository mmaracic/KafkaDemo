package hr.mmaracic.kafka.producer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;

@Slf4j
@Component
@Transactional("chainedTransactionManager")
@RequiredArgsConstructor
public class KafkaProducer {

    @NonNull
    private final KafkaTemplate<String, String> kafkaTemplate;

    public SendResult<String, String> sendToKafkaSync(String obj, String id, String topic) throws ExecutionException, InterruptedException {
        return createAndSendProducerRecord(obj, id, topic);
    }

    private SendResult<String, String> createAndSendProducerRecord(String payload, String objectId, String topic) throws ExecutionException, InterruptedException {

        ProducerRecord<String, String> record = createProducerRecord(topic, objectId, payload);
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
        SendResult<String, String> retval = future.get();

        log.debug("Sent message with key {} to topic {}, partition, {}, offset {}", objectId, topic, retval.getRecordMetadata().partition(),
                retval.getRecordMetadata().offset());

        return retval;
    }

    private ProducerRecord<String, String> createProducerRecord(String topic, String key, String value) {
        return new ProducerRecord<>(topic, key, value);
    }
}
