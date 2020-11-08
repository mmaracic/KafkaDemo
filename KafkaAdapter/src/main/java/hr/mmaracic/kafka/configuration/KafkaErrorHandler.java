package hr.mmaracic.kafka.configuration;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ErrorHandler;

@Slf4j
@RequiredArgsConstructor
public class KafkaErrorHandler implements ErrorHandler {

    private final int maxLines = 3;

    @NonNull
    private Boolean ackAfterHandle;

    @Override
    public void handle(Exception e, ConsumerRecord<?, ?> consumerRecord) {
        if (consumerRecord != null) {
            log.error(
                    "Exception on queue message from topic {}, partition {}, with offset {}, key: {} because error {}\n{}",
                    consumerRecord.topic(), consumerRecord.partition(),
                    consumerRecord.offset(), consumerRecord.key(), e.toString(),
                    createStackTrace(e.getStackTrace(), maxLines));
        } else {
            log.error(
                    "Exception on queue message because error {}\n{}",
                    e.toString(), createStackTrace(e.getStackTrace(), maxLines));
        }
    }

    @Override
    public boolean isAckAfterHandle() {
        return ackAfterHandle;
    }

    public static String createStackTrace(StackTraceElement[] traces, int maxLines) {
        int existingLines = (maxLines < traces.length) ? maxLines : traces.length;
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < existingLines; i++) {
            stringBuffer.append("File: ").append(traces[i].getFileName())
                    .append("Line: ").append(traces[i].getLineNumber()).append("\n");
        }
        return stringBuffer.toString();
    }
}
