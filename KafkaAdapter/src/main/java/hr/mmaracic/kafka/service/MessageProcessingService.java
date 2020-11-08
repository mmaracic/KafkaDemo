package hr.mmaracic.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.kafka.support.Acknowledgment;

import java.io.IOException;

public interface MessageProcessingService {

    void processMessage(String key, String message, String topic, Acknowledgment acknowledgment) throws IOException;
}
