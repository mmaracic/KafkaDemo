package hr.mmaracic.kafka;

import hr.mmaracic.kafka.service.MessageProcessingService;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class KafkaTestConfiguration {

  @Bean
  public MessageProcessingService getMessageProcessingService() {
    return Mockito.mock(MessageProcessingService.class);
  }

  @Bean(name = "chainedTransactionManager")
  public ChainedKafkaTransactionManager<Object, Object> consolidatedDbChainedTransactionManager(
      @Qualifier("kafkaTransactionManager") KafkaTransactionManager<String, String> tmKafka) {
    return new ChainedKafkaTransactionManager(tmKafka);
  }
}
