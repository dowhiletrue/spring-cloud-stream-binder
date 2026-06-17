package com.solace.spring.cloud.stream.binder.inbound.queue;

import com.solace.spring.cloud.stream.binder.util.WatchdogLogger;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Topic;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class FlowXMLMessageListenerTest {

    @Test
    void testStartReceiverThreads_StartsSpecifiedNumberOfThreads() throws InterruptedException {
        FlowXMLMessageListener listener = new FlowXMLMessageListener(new WatchdogLogger());
        Consumer<BytesXMLMessage> messageConsumer = Mockito.mock(Consumer.class);

        int threadCount = 3;
        String threadNamePrefix = "testStartReceiverThreads_StartsSpecifiedNumberOfThreads";

        // Start the receiver threads
        listener.startReceiverThreads(threadCount, threadNamePrefix, messageConsumer, 3, 300, 2000);

        // Wait briefly to let threads initialize
        Thread.sleep(1000);

        AtomicInteger runningThreads = new AtomicInteger(0);
        Thread.getAllStackTraces().keySet().forEach(thread -> {
            if (thread.getName().startsWith(threadNamePrefix)) {
                runningThreads.incrementAndGet();
            }
        });

        // Verify that the expected number of threads were created
        assert runningThreads.get() == threadCount + 1; // Include watchdog thread
    }

    @Test
    void testStartReceiverThreads_CallsMessageConsumerWhenMessageIsPolled() throws InterruptedException {
        FlowXMLMessageListener listener = new FlowXMLMessageListener(new WatchdogLogger());
        Consumer<BytesXMLMessage> messageConsumer = Mockito.mock(Consumer.class);

        int threadCount = 1;
        String threadNamePrefix = "TestThread";

        // Start the receiver threads
        listener.startReceiverThreads(threadCount, threadNamePrefix, messageConsumer, 3, 300, 2000);

        // Simulate a message being received
        BytesXMLMessage mockMessage = mock(BytesXMLMessage.class);
        Mockito.when(mockMessage.getMessageId()).thenReturn("TestMessageId");
        Mockito.when(mockMessage.getDestination()).thenReturn(JCSMPFactory.onlyInstance().createTopic("test/topic"));

        listener.onReceive(mockMessage);

        // Wait briefly to allow message to be processed
        Thread.sleep(2000);

        // Verify that the consumer was called with the message
        verify(messageConsumer, timeout(2000)).accept(mockMessage);
    }

    @Test
    void testStartReceiverThreads_useAllThreads() throws InterruptedException {
        FlowXMLMessageListener listener = new FlowXMLMessageListener(new WatchdogLogger());
        Consumer<BytesXMLMessage> messageConsumer = Mockito.mock(Consumer.class);
        List<BytesXMLMessage> results = new ArrayList<>();
        doAnswer(invocation -> {
            Thread.sleep(1000);
            synchronized (results) {
                results.add(invocation.getArgument(0));
            }
            return null;
        })
                .when(messageConsumer)
                .accept(any(BytesXMLMessage.class));

        int threadCount = 10;
        String threadNamePrefix = "testStartReceiverThreads_useAllThreads";

        // Start the receiver threads
        listener.startReceiverThreads(threadCount, threadNamePrefix, messageConsumer, 3, 300, 2000);

        // Wait briefly to let threads initialize
        Thread.sleep(1000);

        long start = System.nanoTime();
        for (int i = 0; i < 20; i++) {
            BytesXMLMessage mockMessage = mock(BytesXMLMessage.class);
            Mockito.when(mockMessage.getMessageId()).thenReturn("TestMessageId");
            Mockito.when(mockMessage.getDestination()).thenReturn(JCSMPFactory.onlyInstance().createTopic("test/topic"));

            listener.onReceive(mockMessage);
        }
        long afterSend = System.nanoTime();
        long sendTimeMs = (afterSend - start) / 1000000L;
        assertThat(sendTimeMs)
                .as("sendTimeMs is not within the expected range")
                .isBetween(0L, 500L);

        // Wait until 20 elements are in the results list
        assertThat(results).satisfies(r ->
                await().atMost(2500, TimeUnit.MILLISECONDS).until(() -> r.size() == 20)
        );
    }

    @Test
    void testStartReceiverThreads_WatchdogLogsWarningForCongestedQueue() {
        WatchdogLogger mockWatchdogLogger = Mockito.mock(WatchdogLogger.class);
        doCallRealMethod().when(mockWatchdogLogger).warnIfNecessary(any(), any(), any(), any());
        doCallRealMethod().when(mockWatchdogLogger).setLatestWarning(any());

        FlowXMLMessageListener listener = new FlowXMLMessageListener(mockWatchdogLogger);

        Consumer<BytesXMLMessage> messageConsumer = message -> {
            try {
                // Simulate a long message processing time
                Thread.sleep(8000);
            } catch (InterruptedException ignored) {
            }
        };

        int threadCount = 2;
        String threadNamePrefix = "WatchdogTestThread";

        // Start the receiver threads
        listener.startReceiverThreads(threadCount, threadNamePrefix, messageConsumer, 3, 300, 2000);

        // Simulate messages being received
        Topic topic = JCSMPFactory.onlyInstance().createTopic("test/topic");
        for (int i = 0; i < 6; i++) {
            BytesXMLMessage mockMessage = mock(BytesXMLMessage.class);
            Mockito.when(mockMessage.getMessageId()).thenReturn("TestMessageId");
            Mockito.when(mockMessage.getDestination()).thenReturn(topic);
            listener.onReceive(mockMessage);
        }

        mockWatchdogLogger.setLatestWarning(System.currentTimeMillis() - (6 * 60 * 1000));

        // Wait for the warning to be logged
        verify(mockWatchdogLogger, timeout(8000).times(1)).logRelaxed(any(), any());

        for (int i = 0; i < 10; i++) {
            BytesXMLMessage mockMessage = mock(BytesXMLMessage.class);
            Mockito.when(mockMessage.getMessageId()).thenReturn("TestMessageId");
            Mockito.when(mockMessage.getDestination()).thenReturn(topic);
            listener.onReceive(mockMessage);
        }

        mockWatchdogLogger.setLatestWarning(System.currentTimeMillis() - (6 * 60 * 1000));

        verify(mockWatchdogLogger, timeout(8000).times(1)).logUrgent(any(), any(), any());
    }

    /**
     * Graceful-shutdown regression test.
     *
     * <p>Models the {@code JCSMPInboundQueueMessageProducer.doStop()} ordering at the listener level.
     * The {@link Consumer} passed to {@code startReceiverThreads} stands in for
     * {@code onReceiveConcurrent}, whose {@code activeMessages} window spans the message ACK. A shared
     * {@link AtomicBoolean} simulates {@code FlowReceiver.close()}: any "ACK" that runs after it is set
     * is the bug we are guarding against (an ACK on a closed flow throws {@code IllegalStateException}).
     *
     * <p>With the fix, {@code drain()} blocks until every queued and in-flight message has been fully
     * processed, so closing the flow afterwards is safe. If {@code drain()} were a no-op, some messages
     * would be "ACKed" after close and the assertions below would fail.
     */
    @Test
    void testDrain_allInFlightMessagesSettledBeforeFlowClose() {
        FlowXMLMessageListener listener = new FlowXMLMessageListener(new WatchdogLogger());

        AtomicBoolean flowClosed = new AtomicBoolean(false);
        AtomicInteger acked = new AtomicInteger();
        List<Throwable> ackAfterClose = Collections.synchronizedList(new ArrayList<>());

        Consumer<BytesXMLMessage> consumer = msg -> {
            sleepQuietly(200); // simulate message processing
            if (flowClosed.get()) {
                // simulates settling/ACK on an already-closed flow -> IllegalStateException in real life
                ackAfterClose.add(new IllegalStateException("ACK on closed flow"));
            } else {
                acked.incrementAndGet();
            }
        };

        int n = 12;
        listener.startReceiverThreads(3, "drainTest", consumer, 3, 300, 2000);
        for (int i = 0; i < n; i++) {
            listener.onReceive(mockMessage());
        }

        // Mirror doStop(): FlowReceiver.stop() (no new deliveries) -> drain() -> close() -> stop threads
        listener.drain(10_000);
        flowClosed.set(true); // simulate FlowReceiver.close()
        listener.stopReceiverThreads();

        assertThat(listener.isIdle())
                .as("listener must be idle once drain() returns")
                .isTrue();
        assertThat(ackAfterClose)
                .as("no message may be ACKed after the flow is closed")
                .isEmpty();
        assertThat(acked.get())
                .as("every in-flight message must be settled before the flow is closed")
                .isEqualTo(n);
    }

    private static BytesXMLMessage mockMessage() {
        BytesXMLMessage mockMessage = mock(BytesXMLMessage.class);
        Mockito.when(mockMessage.getMessageId()).thenReturn("TestMessageId");
        Mockito.when(mockMessage.getDestination()).thenReturn(JCSMPFactory.onlyInstance().createTopic("test/topic"));
        return mockMessage;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}