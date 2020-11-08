package hr.mmaracic.kafka;

import hr.mmaracic.kafka.producer.KafkaProducer;
import hr.mmaracic.kafka.service.MessageProcessingService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@EmbeddedKafka(ports = {39092}, brokerProperties = {"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1"})
@SpringBootTest
public class KafkaTest {

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @Autowired
  private ConcurrentMessageListenerContainer<Object, Object> kafkaListenerContainer;

  @Autowired
  private KafkaProducer kafkaProducer;

  @Value("${hal.dps.kafka.topics}")
  private String kafkaTopics;

  @Autowired
  private MessageProcessingService messageProcessingService;

  @BeforeEach
  void setUp() {
    kafkaListenerContainer.start();
    ContainerTestUtils.waitForAssignment(kafkaListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
  }

  @Test
  public void testKafkaConnection() throws ExecutionException, InterruptedException, IOException {
    String kafkaMessage = "Test Kafka message";
    kafkaProducer.sendToKafkaSync(kafkaMessage, null, kafkaTopics.split(",")[0]);
    Mockito.verify(messageProcessingService, Mockito.timeout(1000).times(1)).processMessage(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.reset(messageProcessingService);
  }

  @Test
  public void testKafkaError() throws ExecutionException, InterruptedException, IOException {
    Mockito.doThrow(IllegalStateException.class).when(messageProcessingService).processMessage(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    String kafkaMessage = "Test Kafka message";
    kafkaProducer.sendToKafkaSync(kafkaMessage, null, kafkaTopics.split(",")[0]);
    Mockito.verify(messageProcessingService, Mockito.timeout(1000).times(1)).processMessage(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.reset(messageProcessingService);
  }

  @AfterEach
  void tearDown() {
    kafkaListenerContainer.stop();
  }

}
