package hr.mmaracic.kafka.configuration;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.NeverRetryPolicy;

@Slf4j
@RequiredArgsConstructor
public class KafkaRetryPolicy extends NeverRetryPolicy {

    @NonNull
    private Long retryCount;

    @Override
    public boolean canRetry(RetryContext retryContext) {
        if (retryContext.getLastThrowable() != null) {
            log.error("Kafka consumer retry no {} Error {}\n{}",
                    retryContext.getRetryCount(), retryContext.getLastThrowable().toString());
        }
        return retryContext.getRetryCount() < retryCount;
    }
}
