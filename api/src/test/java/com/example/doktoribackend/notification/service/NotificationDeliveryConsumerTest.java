package com.example.doktoribackend.notification.service;

import com.example.doktoribackend.config.NotificationRabbitConfig;
import com.example.doktoribackend.notification.domain.NotificationTypeCode;
import com.example.doktoribackend.notification.dto.NotificationDeliveryTask;
import com.example.doktoribackend.notification.dto.SseNotificationEvent;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.AmqpException;
import org.mockito.InOrder;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.inOrder;

@ExtendWith(MockitoExtension.class)
class NotificationDeliveryConsumerTest {

    @Mock
    SseEmitterService sseEmitterService;

    @Mock
    FcmService fcmService;

    @Mock
    ConfirmedNotificationPublisher publisher;

    @Mock
    Channel channel;

    NotificationDeliveryConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new NotificationDeliveryConsumer(
                sseEmitterService, fcmService, publisher, new SimpleMeterRegistry());
    }

    private final Message message = new Message(new byte[0], new MessageProperties());

    @Test
    @DisplayName("SSE 미연결 유저는 SSE + FCM 모두 발송 후 ACK한다")
    void consume_sseDisconnected_sendsBothAndAcks() throws IOException {
        NotificationDeliveryTask task = createTask(List.of(1L));
        given(sseEmitterService.filterSseDisconnectedUsers(List.of(1L))).willReturn(List.of(1L));

        consumer.consume(task, channel, message, 1L);

        then(sseEmitterService).should().sendToUsers(List.of(1L), task.sseEvent());
        then(fcmService).should().sendToUsers(List.of(1L), "제목", "메시지", "/link");
        verify(channel).basicAck(1L, false);
        verify(channel, never()).basicNack(1L, false, false);
    }

    @Test
    @DisplayName("SSE 연결 중인 유저는 SSE만 발송하고 FCM은 발송하지 않는다")
    void consume_sseConnected_sendsOnlySse() throws IOException {
        NotificationDeliveryTask task = createTask(List.of(1L));
        given(sseEmitterService.filterSseDisconnectedUsers(List.of(1L))).willReturn(List.of());

        consumer.consume(task, channel, message, 1L);

        then(sseEmitterService).should().sendToUsers(List.of(1L), task.sseEvent());
        then(fcmService).should(never()).sendToUsers(anyList(), anyString(), anyString(), anyString());
        verify(channel).basicAck(1L, false);
    }

    @Test
    @DisplayName("일부는 SSE 연결, 일부는 미연결이면 미연결 유저에게만 FCM 발송한다")
    void consume_mixed_sendsFcmOnlyToDisconnected() throws IOException {
        List<Long> userIds = List.of(1L, 2L, 3L);
        NotificationDeliveryTask task = createTask(userIds);
        given(sseEmitterService.filterSseDisconnectedUsers(userIds)).willReturn(List.of(2L, 3L));

        consumer.consume(task, channel, message, 1L);

        then(sseEmitterService).should().sendToUsers(userIds, task.sseEvent());
        then(fcmService).should().sendToUsers(List.of(2L, 3L), "제목", "메시지", "/link");
        verify(channel).basicAck(1L, false);
    }

    @Test
    @DisplayName("SSE가 성공하면 FCM 실패만으로 재전송하지 않는다")
    void consume_fcmFails_acksAndRetries() throws IOException {
        NotificationDeliveryTask task = createTask(List.of(1L));
        given(sseEmitterService.filterSseDisconnectedUsers(List.of(1L))).willReturn(List.of(1L));
        willThrow(new RuntimeException("FCM 에러"))
                .given(fcmService).sendToUsers(anyList(), anyString(), anyString(), anyString());

        consumer.consume(task, channel, message, 1L);

        verify(channel).basicAck(1L, false);
        verify(channel, never()).basicNack(1L, false, false);
    }

    @Test
    @DisplayName("SSE 실패 시에도 FCM 미연결 유저에게 발송 후 ACK한다")
    void consume_sseFails_fcmDeliveredAndAcks() throws IOException {
        NotificationDeliveryTask task = createTask(List.of(1L));
        given(sseEmitterService.filterSseDisconnectedUsers(List.of(1L))).willReturn(List.of(1L));
        willThrow(new RuntimeException("SSE 에러"))
                .given(sseEmitterService).sendToUsers(anyList(), any(SseNotificationEvent.class));

        consumer.consume(task, channel, message, 1L);

        then(fcmService).should().sendToUsers(List.of(1L), "제목", "메시지", "/link");
        verify(channel).basicAck(1L, false);
    }

    @Test
    @DisplayName("DLQ 메시지는 로그 후 ACK하며 SSE, FCM을 발송하지 않는다")
    void handleDeadLetter_acks() throws IOException {
        NotificationDeliveryTask task = createTask(List.of(1L));

        consumer.handleDeadLetter(task, channel, 1L);

        verify(channel).basicAck(1L, false);
        then(sseEmitterService).should(never()).sendToUsers(anyList(), any());
        then(fcmService).should(never()).sendToUsers(anyList(), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("실패 알림의 재발행이 confirm된 뒤 원본을 ACK한다")
    void failedDelivery_confirmsBeforeAck() throws IOException {
        NotificationDeliveryTask task = failBothChannels();
        consumer.consume(task, channel, message, 1L);

        InOrder order = inOrder(publisher, channel);
        order.verify(publisher).publish(anyString(),
                org.mockito.ArgumentMatchers.eq(NotificationRabbitConfig.WAIT_QUEUE),
                org.mockito.ArgumentMatchers.eq(task), any());
        order.verify(channel).basicAck(1L, false);
    }

    @Test
    @DisplayName("재발행 confirm 실패 시 원본을 ACK하지 않고 requeue한다")
    void retryHandoffFails_nacksOriginal() throws IOException {
        NotificationDeliveryTask task = failBothChannels();
        willThrow(new AmqpException("confirm timeout"))
                .given(publisher).publish(anyString(), anyString(), any(), any());
        consumer.consume(task, channel, message, 1L);

        verify(channel, never()).basicAck(1L, false);
        verify(channel).basicNack(1L, false, true);
    }

    @Test
    @DisplayName("DLQ 인계 실패도 원본을 ACK하지 않는다")
    void dlqHandoffFails_nacksOriginal() throws IOException {
        NotificationDeliveryTask task = failBothChannels();
        MessageProperties properties = new MessageProperties();
        properties.setHeader("x-retry-count", NotificationRabbitConfig.MAX_RETRY_COUNT);
        willThrow(new AmqpException("DLQ unavailable"))
                .given(publisher).publish(anyString(),
                        org.mockito.ArgumentMatchers.eq(NotificationRabbitConfig.DLQ), any(), any());

        consumer.consume(task, channel, new Message(new byte[0], properties), 2L);
        verify(channel, never()).basicAck(2L, false);
        verify(channel).basicNack(2L, false, true);
    }

    @Test
    @DisplayName("FCM 대상이 없는 SSE 실패도 재시도한다")
    void sseOnlyFails_retries() throws IOException {
        NotificationDeliveryTask task = createTask(List.of(1L));
        given(sseEmitterService.filterSseDisconnectedUsers(List.of(1L))).willReturn(List.of());
        willThrow(new RuntimeException("SSE failed"))
                .given(sseEmitterService).sendToUsers(anyList(), any());
        consumer.consume(task, channel, message, 1L);

        verify(publisher).publish(anyString(),
                org.mockito.ArgumentMatchers.eq(NotificationRabbitConfig.WAIT_QUEUE), any(), any());
        verify(channel).basicAck(1L, false);
    }

    private NotificationDeliveryTask failBothChannels() {
        NotificationDeliveryTask task = createTask(List.of(1L));
        given(sseEmitterService.filterSseDisconnectedUsers(List.of(1L))).willReturn(List.of(1L));
        willThrow(new RuntimeException("SSE failed"))
                .given(sseEmitterService).sendToUsers(anyList(), any());
        willThrow(new RuntimeException("FCM failed"))
                .given(fcmService).sendToUsers(anyList(), anyString(), anyString(), anyString());
        return task;
    }

    @Test
    @DisplayName("배달 성공 후 ACK 연결 실패를 새 알림 발행으로 처리하지 않는다")
    void ackFailure_doesNotRepublish() throws IOException {
        NotificationDeliveryTask task = createTask(List.of(1L));
        given(sseEmitterService.filterSseDisconnectedUsers(List.of(1L))).willReturn(List.of());
        doThrow(new IOException("channel closed")).when(channel).basicAck(1L, false);

        assertThrows(IOException.class, () -> consumer.consume(task, channel, message, 1L));
        verify(publisher, never()).publish(anyString(), anyString(), any(), any());
    }

    private NotificationDeliveryTask createTask(List<Long> userIds) {
        SseNotificationEvent sseEvent = new SseNotificationEvent(
                null,
                NotificationTypeCode.ROUND_START_10M_BEFORE,
                "제목",
                "메시지",
                "/link",
                LocalDateTime.now()
        );
        return new NotificationDeliveryTask(userIds, "제목", "메시지", "/link", sseEvent);
    }
}
