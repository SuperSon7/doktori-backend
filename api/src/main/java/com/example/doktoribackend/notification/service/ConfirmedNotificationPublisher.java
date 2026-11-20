package com.example.doktoribackend.notification.service;

import com.example.doktoribackend.notification.dto.NotificationDeliveryTask;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class ConfirmedNotificationPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final long confirmTimeoutMillis;

    public ConfirmedNotificationPublisher(RabbitTemplate rabbitTemplate,
            @Value("${app.notification.confirm-timeout-millis:5000}") long confirmTimeoutMillis) {
        if (confirmTimeoutMillis <= 0) {
            throw new IllegalArgumentException("confirm timeout must be positive");
        }
        this.rabbitTemplate = rabbitTemplate;
        this.confirmTimeoutMillis = confirmTimeoutMillis;
    }

    public void publish(String exchange, String routingKey, NotificationDeliveryTask task,
                        MessagePostProcessor postProcessor) {
        CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(exchange, routingKey, task, postProcessor, correlation);
        try {
            CorrelationData.Confirm confirm = correlation.getFuture()
                    .get(confirmTimeoutMillis, TimeUnit.MILLISECONDS);
            if (!confirm.isAck() || correlation.getReturned() != null) {
                throw new AmqpException("Notification was not confirmed and routed: " + routingKey);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AmqpException("Interrupted while waiting for notification confirm", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new AmqpException("Notification confirm failed: " + routingKey, e);
        }
    }
}
