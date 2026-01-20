package org.openflexo.pamela.test.sync;

import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openflexo.pamela.sync.*;

import java.beans.PropertyChangeEvent;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * JUnit tests for ArtemisMQTTSyncManager
 */
@ExtendWith(MockitoExtension.class)
class ArtemisMQTTSyncManagerTest {

    private ArtemisMQTTSyncManager syncManager;

    @BeforeEach
    void setUp() {
        syncManager = new ArtemisMQTTSyncManager();
    }

    @AfterEach
    void tearDown() {
        if (syncManager != null && syncManager.isConnected()) {
            syncManager.close();
        }
    }

    // ========== Helper class for testing listeners ==========

    /**
     * Helper class that implements SyncOperationListener for testing
     */
    private static class TestListener implements SyncOperationListener {
        private final CountDownLatch latch;
        private final String expectedProperty;
        private PropertyChangeEvent capturedEvent;
        private SyncOperation capturedOperation;

        public TestListener(CountDownLatch latch, String expectedProperty) {
            this.latch = latch;
            this.expectedProperty = expectedProperty;
        }

        @Override
        public void propertyChange(PropertyChangeEvent evt) {
            if (expectedProperty == null || expectedProperty.equals(evt.getPropertyName())) {
                this.capturedEvent = evt;
                latch.countDown();
            }
        }

        @Override
        public void onOperationReceived(SyncOperation operation) {
            this.capturedOperation = operation;
            if (expectedProperty == null || "OPERATION_RECEIVED".equals(expectedProperty)) {
                latch.countDown();
            }
        }

        public PropertyChangeEvent getCapturedEvent() {
            return capturedEvent;
        }

        public SyncOperation getCapturedOperation() {
            return capturedOperation;
        }
    }

    // ========== Constructor Tests ==========

    @Test
    void testDefaultConstructor() {
        ArtemisMQTTSyncManager manager = new ArtemisMQTTSyncManager();

        assertNotNull(manager);
        assertNotNull(manager.getReplicaId());
        assertNotNull(manager.getVectorClock());
        assertFalse(manager.isConnected());
    }

    @Test
    void testParameterizedConstructor() {
        ArtemisMQTTSyncManager manager = new ArtemisMQTTSyncManager(
                "test-host", 1234, "testuser", "testpass",
                "test-exchange", "test-routing", true
        );

        assertNotNull(manager);
        assertNotNull(manager.getReplicaId());
        assertNotNull(manager.getVectorClock());
        assertFalse(manager.isConnected());
    }

    @Test
    void testReplicaIdIsUnique() {
        ArtemisMQTTSyncManager manager1 = new ArtemisMQTTSyncManager();
        ArtemisMQTTSyncManager manager2 = new ArtemisMQTTSyncManager();

        assertNotEquals(manager1.getReplicaId(), manager2.getReplicaId());
    }

    // ========== Builder Tests ==========

    @Test
    void testBuilderWithDefaultValues() {
        ArtemisMQTTSyncManager manager = ArtemisMQTTSyncManager.builder().build();

        assertNotNull(manager);
        assertFalse(manager.isConnected());
    }

    @Test
    void testBuilderWithCustomValues() {
        ArtemisMQTTSyncManager manager = ArtemisMQTTSyncManager.builder()
                .host("custom-host")
                .port(9999)
                .credentials("user", "pass")
                .exchangeName("custom-exchange")
                .routingKey("custom-routing")
                .useSsl(true)
                .build();

        assertNotNull(manager);
        assertFalse(manager.isConnected());
    }

    @Test
    void testBuilderChaining() {
        ArtemisMQTTSyncManager.Builder builder = ArtemisMQTTSyncManager.builder();

        assertSame(builder, builder.host("host"));
        assertSame(builder, builder.port(1234));
        assertSame(builder, builder.credentials("user", "pass"));
        assertSame(builder, builder.exchangeName("exchange"));
        assertSame(builder, builder.routingKey("routing"));
        assertSame(builder, builder.useSsl(true));
    }

    // ========== Connection Tests ==========

    @Test
    void testConnect() throws Exception {
        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();

            assertTrue(syncManager.isConnected());
            MqttClient client = mqttClientMock.constructed().get(0);
            verify(client).connect(any(MqttConnectOptions.class));
            verify(client).subscribe(anyString(), eq(1));
        }
    }

    @Test
    void testConnectAlreadyConnected() throws Exception {
        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();
            assertTrue(syncManager.isConnected());

            // Try to connect again
            syncManager.connect();

            // Should only construct one client
            assertEquals(1, mqttClientMock.constructed().size());
        }
    }

    @Test
    void testConnectWithListener() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, "CONNECTION");

        syncManager.addListener(listener);

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();

            assertTrue(latch.await(2, TimeUnit.SECONDS));
            assertEquals(Boolean.TRUE, listener.getCapturedEvent().getNewValue());
        }
    }

    @Test
    void testConnectionLost() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, "DISCONNECTION");

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
                    doNothing().when(mock).setCallback(callbackCaptor.capture());
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.addListener(listener);
            syncManager.connect();

            MqttClient client = mqttClientMock.constructed().get(0);
            ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
            verify(client).setCallback(callbackCaptor.capture());

            MqttCallback callback = callbackCaptor.getValue();
            callback.connectionLost(new Exception("Test connection lost"));

            assertTrue(latch.await(2, TimeUnit.SECONDS));
        }
    }

    // ========== Close Tests ==========

    @Test
    void testClose() throws Exception {
        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();
            assertTrue(syncManager.isConnected());

            syncManager.close();

            assertFalse(syncManager.isConnected());
            MqttClient client = mqttClientMock.constructed().get(0);
            verify(client).disconnect();
            verify(client).close();
        }
    }

    @Test
    void testCloseWhenNotConnected() {
        assertDoesNotThrow(() -> syncManager.close());
        assertFalse(syncManager.isConnected());
    }

    @Test
    void testCloseNotifiesListeners() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, "DISCONNECTION");

        syncManager.addListener(listener);

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();
            syncManager.close();

            assertTrue(latch.await(2, TimeUnit.SECONDS));
        }
    }

    // ========== Publish Operation Tests ==========

    @Test
    void testPublishOperation() throws Exception {
        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();

            SyncOperation operation = new SyncOperation.Builder(SyncOperation.OperationType.CREATE)
                    .replicaId(syncManager.getReplicaId())
                    .objectId("test-object")
                    .entityType("TestEntity")
                    .build();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.serializeToBytes(any()))
                        .thenReturn(new byte[]{1, 2, 3});

                syncManager.publishOperation(operation);

                MqttClient client = mqttClientMock.constructed().get(0);
                verify(client).publish(anyString(), any(MqttMessage.class));
            }
        }
    }

    @Test
    void testPublishOperationWhenNotConnected() {
        SyncOperation operation = new SyncOperation.Builder(SyncOperation.OperationType.CREATE)
                .replicaId(syncManager.getReplicaId())
                .objectId("test-object")
                .entityType("TestEntity")
                .build();

        assertDoesNotThrow(() -> syncManager.publishOperation(operation));
    }

    @Test
    void testPublishOperationIncrementsVectorClock() throws Exception {
        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();

            VectorClock initialClock = syncManager.getVectorClock().copy();

            SyncOperation operation = new SyncOperation.Builder(SyncOperation.OperationType.CREATE)
                    .replicaId(syncManager.getReplicaId())
                    .objectId("test-object")
                    .entityType("TestEntity")
                    .build();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.serializeToBytes(any()))
                        .thenReturn(new byte[]{1, 2, 3});

                syncManager.publishOperation(operation);

                // Vector clock should have been incremented
                assertNotEquals(initialClock.get(syncManager.getReplicaId()),
                        syncManager.getVectorClock().get(syncManager.getReplicaId()));
            }
        }
    }

    // ========== Request State Tests ==========

    @Test
    void testRequestState() throws Exception {
        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.serializeToBytes(any()))
                        .thenReturn(new byte[]{1, 2, 3});

                syncManager.requestState();

                MqttClient client = mqttClientMock.constructed().get(0);
                verify(client).publish(anyString(), any(MqttMessage.class));
            }
        }
    }

    @Test
    void testRequestStateWhenNotConnected() {
        assertDoesNotThrow(() -> syncManager.requestState());
    }

    // ========== Send State Response Tests ==========

    @Test
    void testSendStateResponse() throws Exception {
        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.serializeToBytes(any()))
                        .thenReturn(new byte[]{1, 2, 3});

                syncManager.sendStateResponse("test-state", "target-replica");

                MqttClient client = mqttClientMock.constructed().get(0);
                verify(client).publish(anyString(), any(MqttMessage.class));
            }
        }
    }

    @Test
    void testSendStateResponseWithNullTarget() throws Exception {
        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.serializeToBytes(any()))
                        .thenReturn(new byte[]{1, 2, 3});

                syncManager.sendStateResponse("test-state", null);

                MqttClient client = mqttClientMock.constructed().get(0);
                verify(client).publish(anyString(), any(MqttMessage.class));
            }
        }
    }

    @Test
    void testSendStateResponseWhenNotConnected() {
        assertDoesNotThrow(() -> syncManager.sendStateResponse("test-state", "target"));
    }

    // ========== Message Handling Tests ==========

    @Test
    void testMessageArrivedWithRegularOperation() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, "OPERATION_RECEIVED");

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
                    doNothing().when(mock).setCallback(callbackCaptor.capture());
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.addListener(listener);
            syncManager.connect();

            MqttClient client = mqttClientMock.constructed().get(0);
            ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
            verify(client).setCallback(callbackCaptor.capture());

            MqttCallback callback = callbackCaptor.getValue();

            SyncOperation testOperation = new SyncOperation.Builder(SyncOperation.OperationType.SET)
                    .replicaId("other-replica")
                    .objectId("test-object")
                    .entityType("TestEntity")
                    .vectorClock(new VectorClock())
                    .build();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.deserializeFromBytes(any()))
                        .thenReturn(testOperation);

                MqttMessage message = new MqttMessage(new byte[]{1, 2, 3});
                callback.messageArrived("test-topic", message);

                assertTrue(latch.await(2, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void testMessageArrivedIgnoresOwnOperations() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, "OPERATION_RECEIVED");

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
                    doNothing().when(mock).setCallback(callbackCaptor.capture());
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.addListener(listener);
            syncManager.connect();

            MqttClient client = mqttClientMock.constructed().get(0);
            ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
            verify(client).setCallback(callbackCaptor.capture());

            MqttCallback callback = callbackCaptor.getValue();

            // Operation from this replica (should be ignored)
            SyncOperation testOperation = new SyncOperation.Builder(SyncOperation.OperationType.SET)
                    .replicaId(syncManager.getReplicaId())
                    .objectId("test-object")
                    .entityType("TestEntity")
                    .build();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.deserializeFromBytes(any()))
                        .thenReturn(testOperation);

                MqttMessage message = new MqttMessage(new byte[]{1, 2, 3});
                callback.messageArrived("test-topic", message);

                assertFalse(latch.await(1, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void testMessageArrivedWithStateRequest() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, "STATE_REQUEST");

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
                    doNothing().when(mock).setCallback(callbackCaptor.capture());
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.addListener(listener);
            syncManager.connect();

            MqttClient client = mqttClientMock.constructed().get(0);
            ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
            verify(client).setCallback(callbackCaptor.capture());

            MqttCallback callback = callbackCaptor.getValue();

            SyncOperation stateRequest = new SyncOperation.Builder(SyncOperation.OperationType.STATE_REQUEST)
                    .replicaId("other-replica")
                    .objectId("state-request")
                    .entityType("StateRequest")
                    .build();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.deserializeFromBytes(any()))
                        .thenReturn(stateRequest);

                MqttMessage message = new MqttMessage(new byte[]{1, 2, 3});
                callback.messageArrived("test-topic", message);

                assertTrue(latch.await(2, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void testMessageArrivedWithStateResponse() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, "STATE_RECEIVED");

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
                    doNothing().when(mock).setCallback(callbackCaptor.capture());
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.addListener(listener);
            syncManager.connect();

            MqttClient client = mqttClientMock.constructed().get(0);
            ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
            verify(client).setCallback(callbackCaptor.capture());

            MqttCallback callback = callbackCaptor.getValue();

            SyncOperation stateResponse = new SyncOperation.Builder(SyncOperation.OperationType.STATE_RESPONSE)
                    .replicaId("other-replica")
                    .objectId("state-response")
                    .entityType("StateResponse")
                    .propertyIdentifier(syncManager.getReplicaId())
                    .newValue("test-state-data")
                    .build();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.deserializeFromBytes(any()))
                        .thenReturn(stateResponse);

                MqttMessage message = new MqttMessage(new byte[]{1, 2, 3});
                callback.messageArrived("test-topic", message);

                assertTrue(latch.await(2, TimeUnit.SECONDS));
            }
        }
    }

    // ========== Listener Tests ==========

    @Test
    void testAddListener() {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, null);

        assertDoesNotThrow(() -> syncManager.addListener(listener));
    }

    @Test
    void testRemoveListener() {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, null);

        syncManager.addListener(listener);
        assertDoesNotThrow(() -> syncManager.removeListener(listener));
    }

    @Test
    void testMultipleListeners() throws Exception {
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        TestListener listener1 = new TestListener(latch1, "CONNECTION");
        TestListener listener2 = new TestListener(latch2, "CONNECTION");

        syncManager.addListener(listener1);
        syncManager.addListener(listener2);

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();

            assertTrue(latch1.await(2, TimeUnit.SECONDS));
            assertTrue(latch2.await(2, TimeUnit.SECONDS));
        }
    }

    // ========== Vector Clock Tests ==========

    @Test
    void testVectorClockMerge() throws Exception {
        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
                    doNothing().when(mock).setCallback(callbackCaptor.capture());
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.connect();

            MqttClient client = mqttClientMock.constructed().get(0);
            ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
            verify(client).setCallback(callbackCaptor.capture());

            MqttCallback callback = callbackCaptor.getValue();

            VectorClock otherClock = new VectorClock();
            otherClock.increment("other-replica");

            SyncOperation operation = new SyncOperation.Builder(SyncOperation.OperationType.SET)
                    .replicaId("other-replica")
                    .objectId("test-object")
                    .entityType("TestEntity")
                    .vectorClock(otherClock)
                    .build();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.deserializeFromBytes(any()))
                        .thenReturn(operation);

                MqttMessage message = new MqttMessage(new byte[]{1, 2, 3});
                callback.messageArrived("test-topic", message);

                // Give time for async processing
                Thread.sleep(100);

                // Vector clock should have merged
                assertTrue(syncManager.getVectorClock().get("other-replica") > 0);
            }
        }
    }

    // ========== Error Handling Tests ==========

    @Test
    void testErrorNotification() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, "ERROR");

        syncManager.addListener(listener);

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    when(mock.isConnected()).thenReturn(true);
                    doThrow(new MqttException(1)).when(mock).publish(anyString(), any(MqttMessage.class));
                })) {

            syncManager.connect();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.serializeToBytes(any()))
                        .thenReturn(new byte[]{1, 2, 3});

                SyncOperation operation = new SyncOperation.Builder(SyncOperation.OperationType.CREATE)
                        .replicaId(syncManager.getReplicaId())
                        .objectId("test-object")
                        .entityType("TestEntity")
                        .build();

                syncManager.publishOperation(operation);

                assertTrue(latch.await(2, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void testDeserializationError() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        TestListener listener = new TestListener(latch, "ERROR");

        try (MockedConstruction<MqttClient> mqttClientMock = mockConstruction(MqttClient.class,
                (mock, context) -> {
                    ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
                    doNothing().when(mock).setCallback(callbackCaptor.capture());
                    when(mock.isConnected()).thenReturn(true);
                })) {

            syncManager.addListener(listener);
            syncManager.connect();

            MqttClient client = mqttClientMock.constructed().get(0);
            ArgumentCaptor<MqttCallback> callbackCaptor = ArgumentCaptor.forClass(MqttCallback.class);
            verify(client).setCallback(callbackCaptor.capture());

            MqttCallback callback = callbackCaptor.getValue();

            try (MockedStatic<SyncOperationSerializer> serializerMock = mockStatic(SyncOperationSerializer.class)) {
                serializerMock.when(() -> SyncOperationSerializer.deserializeFromBytes(any()))
                        .thenThrow(new SyncOperationSerializer.SyncSerializationException("Test error", new RuntimeException("Test cause")));

                MqttMessage message = new MqttMessage(new byte[]{1, 2, 3});
                callback.messageArrived("test-topic", message);

                assertTrue(latch.await(2, TimeUnit.SECONDS));
            }
        }
    }
}