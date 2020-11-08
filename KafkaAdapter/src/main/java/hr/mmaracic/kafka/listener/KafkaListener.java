package hr.mmaracic.kafka.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import hr.mmaracic.kafka.service.MessageProcessingService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Transactional(transactionManager = "chainedTransactionManager", propagation = Propagation.REQUIRED)
public class KafkaListener implements AcknowledgingMessageListener<String, String> {

  @NonNull
  private final List<String> kafkaTopics;

  @NonNull
  private final MessageProcessingService messageProcessingService;

  public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {

    log.debug("Processing message from topic {}, partition {}, with offset {}, key: {}.",
        consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
        consumerRecord.key());

    try {
      messageProcessingService.processMessage(consumerRecord.key(), consumerRecord.value(),
              consumerRecord.topic(), acknowledgment);
      acknowledgment.acknowledge();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    finally {
      log.debug("Ended processing message from topic {}, partition {}, with offset {}, key: {}.",
          consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
          consumerRecord.key());
    }
  }

}
