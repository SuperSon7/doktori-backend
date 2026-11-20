package com.example.doktoribackend.notification.service;

import com.example.doktoribackend.config.NotificationRabbitConfig;
import com.example.doktoribackend.notification.dto.NotificationDeliveryTask;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class NotificationDeliveryConsumer {

    private static final String RETRY_COUNT_HEADER = "x-retry-count";

    private final SseEmitterService sseEmitterService;
    private final FcmService fcmService;
    private final ConfirmedNotificationPublisher publisher;
    private final Counter deliverySuccessCounter;
    private final Counter deliveryFailureCounter;
    private final Counter deliveryRetriedCounter;
    private final Counter deliveryPermanentFailureCounter;

    public NotificationDeliveryConsumer(SseEmitterService sseEmitterService,
                                       FcmService fcmService,
                                       ConfirmedNotificationPublisher publisher,
                                       MeterRegistry meterRegistry) {
        this.sseEmitterService = sseEmitterService;
        this.fcmService = fcmService;
        this.publisher = publisher;
        this.deliverySuccessCounter = Counter.builder("notification.delivery")
                .tag("result", "success")
                .description("Notification delivery attempts")
                .register(meterRegistry);
        this.deliveryFailureCounter = Counter.builder("notification.delivery")
                .tag("result", "failure")
                .description("Notification delivery attempts")
                .register(meterRegistry);
        this.deliveryRetriedCounter = Counter.builder("notification.delivery")
                .tag("result", "retried")
                .description("Notification delivery retried")
                .register(meterRegistry);
        this.deliveryPermanentFailureCounter = Counter.builder("notification.delivery")
                .tag("result", "permanent_failure")
                .description("Notification delivery permanently failed")
                .register(meterRegistry);
    }

    @RabbitListener(queues = NotificationRabbitConfig.QUEUE)
    public void consume(
            NotificationDeliveryTask task,
            Channel channel,
            Message message,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {
        try {
            deliver(task);
        } catch (Exception e) {
            int retryCount = getRetryCount(message);
            try {
                if (retryCount < NotificationRabbitConfig.MAX_RETRY_COUNT) {
                    log.warn("Notification delivery failed (attempt {}/{}), retrying for userIds: {}",
                        retryCount + 1, NotificationRabbitConfig.MAX_RETRY_COUNT, task.userIds(), e);
                    sendToWaitQueue(task, retryCount + 1);
                    deliveryRetriedCounter.increment();
                } else {
                    log.error("Notification delivery permanently failed after {} retries for userIds: {}",
                        NotificationRabbitConfig.MAX_RETRY_COUNT, task.userIds(), e);
                    sendToDlq(task, retryCount);
                    deliveryFailureCounter.increment();
                }
            } catch (RuntimeException publishFailure) {
                // 원본은 대상 큐의 confirm 전에는 ACK하지 않는다. 응답 불명확 시 재전달될 수 있다.
                log.error("Retry handoff failed; retaining original notification", publishFailure);
                channel.basicNack(deliveryTag, false, true);
                return;
            }
            channel.basicAck(deliveryTag, false);
            return;
        }
        // ACK 실패를 배달 실패로 분류해 재발행하지 않는다.
        channel.basicAck(deliveryTag, false);
        deliverySuccessCounter.increment();
    }

    @RabbitListener(queues = NotificationRabbitConfig.DLQ)
    public void handleDeadLetter(
            NotificationDeliveryTask task,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {
        log.error("Notification permanently failed after max retries. userIds={}, title={}",
                task.userIds(), task.title());
        deliveryPermanentFailureCounter.increment();
        channel.basicAck(deliveryTag, false);
    }

    private void deliver(NotificationDeliveryTask task) {
        List<Long> userIds = task.userIds();
        List<Long> fcmTargetIds = sseEmitterService.filterSseDisconnectedUsers(userIds);

        boolean sseFailed = false;
        boolean fcmFailed = false;

        try {
            sseEmitterService.sendToUsers(userIds, task.sseEvent());
        } catch (Exception e) {
            log.error("SSE delivery failed for userIds: {}", userIds, e);
            sseFailed = true;
        }

        if (!fcmTargetIds.isEmpty()) {
            try {
                fcmService.sendToUsers(fcmTargetIds, task.title(), task.message(), task.linkPath());
            } catch (Exception e) {
                log.error("FCM delivery failed for userIds: {}", fcmTargetIds, e);
                fcmFailed = true;
            }
        }

        if (sseFailed && (fcmTargetIds.isEmpty() || fcmFailed)) {
            throw new RuntimeException("Both SSE and FCM delivery failed for userIds: " + userIds);
        }
    }

    private int getRetryCount(Message message) {
        Map<String, Object> headers = message.getMessageProperties().getHeaders();
        Object count = headers.get(RETRY_COUNT_HEADER);
        if (count instanceof Number) {
            return ((Number) count).intValue();
        }
        return 0;
    }

    private void sendToWaitQueue(NotificationDeliveryTask task, int retryCount) {
        long delay = NotificationRabbitConfig.getRetryDelay(retryCount);
        publisher.publish("", NotificationRabbitConfig.WAIT_QUEUE, task, msg -> {
            msg.getMessageProperties().setExpiration(String.valueOf(delay));
            msg.getMessageProperties().getHeaders().put(RETRY_COUNT_HEADER, retryCount);
            return msg;
        });
    }

    private void sendToDlq(NotificationDeliveryTask task, int retryCount) {
        publisher.publish("", NotificationRabbitConfig.DLQ, task, msg -> {
            msg.getMessageProperties().getHeaders().put(RETRY_COUNT_HEADER, retryCount);
            return msg;
        });
    }
}
