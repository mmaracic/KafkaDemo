package hr.mmaracic.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaClientTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaClientTestApplication.class, args);
	}

}
