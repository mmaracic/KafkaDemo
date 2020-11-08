package hr.mmaracic.kafka.configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaRecoveryCallback implements RecoveryCallback<Void> {

    @Override
    public Void recover(RetryContext context) throws Exception {
        if (context.getLastThrowable() != null) {
            log.error("Kafka consumer retry no {} Error {}\n{}",
                    context.getRetryCount(), context.getLastThrowable().toString(),
                    KafkaErrorHandler.createStackTrace(context.getLastThrowable().getStackTrace(),
                            context.getLastThrowable().getStackTrace().length));
        }
        return null;
    }
}
