package com.example.doktoribackend.notification.service;

import com.example.doktoribackend.config.NotificationRabbitConfig;
import com.example.doktoribackend.notification.dto.NotificationDeliveryTask;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
@RequiredArgsConstructor
public class RabbitMQNotificationQueue implements NotificationEnqueuePort {

    private final ConfirmedNotificationPublisher publisher;

    @Override
    public void enqueue(NotificationDeliveryTask task) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                publisher.publish(
                        NotificationRabbitConfig.EXCHANGE,
                        NotificationRabbitConfig.ROUTING_KEY,
                        task,
                        message -> message
                );
            }
        });
    }
}
