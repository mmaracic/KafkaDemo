package hr.mmaracic.kafka.configuration;

import hr.mmaracic.kafka.listener.KafkaListener;
import hr.mmaracic.kafka.service.MessageProcessingService;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.adapter.RetryingMessageListenerAdapter;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Value("${hal.dps.kafka.bootstrap-servers}")
    private String kafkaUrl;

    @Value("${hal.dps.kafka.partition.number:1}")
    private int kafkaPartitionNumber;

    @Value("${hal.dps.kafka.partition.replication:1}")
    private short kafkaPartitionReplication;

    @Value("${hal.dps.kafka.retry.time:2000}")
    private long retryTime;

    @Value("${hal.dps.kafka.retry.count:1}")
    private long retryCount;

    @Value("${hal.dps.kafka.topics}")
    private String kafkaTopics;

  @Value("${hal.dps.kafka.consumerGroup}")
  private String kafkaConsumerGroup;

  @Value("${spring.kafka.producer.transaction-id-prefix:null}")
  private String transactionIdPrefix;

  protected AckMode ackMode = AckMode.MANUAL;

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        return new KafkaAdmin(configs);
    }

    @Bean
    public ConsumerFactory<Object, Object> createConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroup);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    public ProducerFactory<?, ?> kafkaProducerFactory() {
      DefaultKafkaProducerFactory<?, ?> factory = new DefaultKafkaProducerFactory<>(
          producerConfigs());
      if (transactionIdPrefix != null) {
        factory.setTransactionIdPrefix(transactionIdPrefix);
      }
      return factory;
    }

  @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(retryTime);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

        KafkaRetryPolicy retryPolicy = new KafkaRetryPolicy(retryCount);
        retryTemplate.setRetryPolicy(retryPolicy);
        return retryTemplate;
    }

    @Bean
    public ConcurrentMessageListenerContainer<Object, Object> kafkaClientContainer(MessageProcessingService messageProcessingService, ChainedKafkaTransactionManager<Object, Object> chainedKafkaTransactionManager,
                                                                                   ConsumerFactory<Object, Object> consumerFactory,
                                                                                   RetryTemplate retryTemplate, KafkaRecoveryCallback kafkaRecoveryCallback) {

        ContainerProperties containerProps = new ContainerProperties(getKafkaTopics());
        containerProps.setAckMode(ackMode);
        containerProps.setTransactionManager(chainedKafkaTransactionManager);
        containerProps.setMessageListener(new RetryingMessageListenerAdapter(
                new KafkaListener(Arrays.asList(getKafkaTopics()),
                        messageProcessingService), retryTemplate, kafkaRecoveryCallback));

        ConcurrentMessageListenerContainer<Object, Object> container = new ConcurrentMessageListenerContainer<>(
            consumerFactory, containerProps);
        container.setConcurrency(kafkaPartitionNumber);
        container.setErrorHandler(new KafkaErrorHandler(false));
        return container;
    }

    private String[] getKafkaTopics() {
        return kafkaTopics.split(",");
    }

}
