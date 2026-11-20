package com.example.doktoribackend.notification.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import com.example.doktoribackend.notification.dto.NotificationDeliveryTask;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;

@ExtendWith(MockitoExtension.class)
class ConfirmedNotificationPublisherTest {
    @Mock
    RabbitTemplate template;
    ConfirmedNotificationPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new ConfirmedNotificationPublisher(template, 20);
    }

    @Test
    void routedAck_succeeds() {
        confirm(true, false);
        assertDoesNotThrow(() -> publisher.publish("", "queue", null, message -> message));
    }

    @Test
    void nack_fails() {
        confirm(false, false);
        assertThrows(AmqpException.class,
                () -> publisher.publish("", "queue", null, message -> message));
    }

    @Test
    void returnedEvenWithAck_fails() {
        confirm(true, true);
        assertThrows(AmqpException.class,
                () -> publisher.publish("", "queue", null, message -> message));
    }

    @Test
    void missingConfirm_timesOut() {
        assertThrows(AmqpException.class,
                () -> publisher.publish("", "queue", null, message -> message));
    }

    private void confirm(boolean ack, boolean returned) {
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(4);
            if (returned) {
                correlation.setReturned(new ReturnedMessage(
                        new Message(new byte[0], new MessageProperties()), 312, "NO_ROUTE", "", "queue"));
            }
            correlation.getFuture().complete(new CorrelationData.Confirm(ack, ack ? null : "nack"));
            return null;
        }).when(template).convertAndSend(anyString(), anyString(),
                isNull(NotificationDeliveryTask.class), any(MessagePostProcessor.class), any(CorrelationData.class));
    }
}
