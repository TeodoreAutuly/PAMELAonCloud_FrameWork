package org.openflexo.pamela.sync;

import java.beans.PropertyChangeSupport;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * MQTT based synchronization manager for PAMELA collaborative editing.
 */
public class ArtemisMQTTSyncManager implements SyncManager, AutoCloseable {

    private static final Logger logger = Logger.getLogger(ArtemisMQTTSyncManager.class.getName());

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final boolean useSsl;

    private final String exchangeName;
    private final String routingKey;
    private final String mqttTopic;

    private final String replicaId;
    private final VectorClock vectorClock;

    private MqttClient mqttClient;
    private MqttConnectOptions mqttOptions;

	private final PropertyChangeSupport pcs;

    private volatile boolean connected = false;
    private volatile boolean closing = false;

    public ArtemisMQTTSyncManager() {
        this("127.0.0.1", 1883, "guest", "guest", "pamela.sync", "operations", false);
    }

    public ArtemisMQTTSyncManager(String host, int port, String username, String password,
                                  String exchangeName, String routingKey, boolean useSsl) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
        this.useSsl = useSsl;
        this.replicaId = UUID.randomUUID().toString();
        this.vectorClock = new VectorClock();
        this.mqttTopic = exchangeName + "/" + routingKey;
		this.pcs = new PropertyChangeSupport(this);
    }

    public String getReplicaId() {
        return replicaId;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public boolean isConnected() {
        return connected;
    }

	/**
	 * Connect to MQTT broker and start consuming messages
	 */
    public void connect() throws Exception {
    	if (connected) {
			logger.warning("Already connected to MQTT broker");
			return;
		}
    	
        //startEmbeddedBroker();

        String brokerUrl = "tcp://" + host + ":" + port;
        mqttClient = new MqttClient(brokerUrl, "pamela-" + replicaId);

        mqttOptions = new MqttConnectOptions();
        mqttOptions.setAutomaticReconnect(true);
        mqttOptions.setCleanSession(true);
        mqttOptions.setUserName(username);
        mqttOptions.setPassword(password.toCharArray());
        
        

        mqttClient.setCallback(new MqttCallback() {
        	@Override
        	public void deliveryComplete(IMqttDeliveryToken token) {
        	    // Not needed
        	}
        	
            public void connectionLost(Throwable cause) {
                if (!closing) {
                	logger.warning("Connexion lost with caue:" + cause.getMessage());
                	notifyDisconnected(cause.getMessage());
                }
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
            	byte[] payload = message.getPayload();
                if (closing) return;

                try {
                    SyncOperation operation = SyncOperationSerializer.deserializeFromBytes(payload);

                    // Ignore operations from this replica
        			if (replicaId.equals(operation.getReplicaId())) {
        				return;
        			}
        			
                    // Handle STATE_REQUEST and STATE_RESPONSE specially
        			if (operation.getOperationType() == SyncOperation.STATE_REQUEST) {
        				// Another replica is requesting state
        				notifyStateRequested(operation.getReplicaId());
        				return;
        			}

                    if (operation.getOperationType() == SyncOperation.STATE_RESPONSE) {
        				// Check if this response is for us (targetReplicaId stored in propertyIdentifier)
        				String targetReplicaId = operation.getPropertyIdentifier();
        				if (targetReplicaId == null || targetReplicaId.equals(replicaId)) {
        					notifyStateReceived(operation.getNewValueSerialized(), operation.getReplicaId());
        				}
        				return;
        			}

                    // Update our vector clock
        			if (operation.getVectorClock() != null) {
        				vectorClock.merge(operation.getVectorClock());
        			}

                    // Notify listeners
    				try {
    					pcs.firePropertyChange("OPERATION_RECEIVED", null, operation);
    				} catch (Exception e) {
    					logger.log(Level.SEVERE, "Error in operation listener", e);
    				}

                } catch (SyncOperationSerializer.SyncSerializationException e) {
        			logger.log(Level.SEVERE, "Failed to deserialize operation", e);
        			notifyError(e);
        		}
            }

        });
        
        mqttClient.connect(mqttOptions);
        mqttClient.subscribe(mqttTopic, 1);

        connected = true;
        logger.info("Connected to MQTT broker at " + host + ":" + port + " as replica " + replicaId);
		notifyConnected();
    }


    /**
	 * Publish a synchronization operation to all replicas
	 * 
	 * @param operation the operation to publish
	 */
	@Override
    public void publishOperation(SyncOperation operation) {
    	if (!connected) {
			logger.warning("Cannot publish: not connected to RabbitMQ");
			return;
		}

        try {
            vectorClock.increment(replicaId);

            SyncOperation operationWithClock = new SyncOperation.Builder(operation.getOperationType())
                    .operationId(operation.getOperationId())
                    .timestamp(operation.getTimestamp())
                    .replicaId(operation.getReplicaId())
                    .objectId(operation.getObjectId())
                    .entityType(operation.getEntityType())
                    .propertyIdentifier(operation.getPropertyIdentifier())
                    .oldValue(operation.getOldValueSerialized())
                    .newValue(operation.getNewValueSerialized())
                    .valueType(operation.getValueType())
                    .index(operation.getIndex())
                    .vectorClock(vectorClock.copy())
                    .build();

            byte[] body = SyncOperationSerializer.serializeToBytes(operationWithClock);

            MqttMessage message = new MqttMessage(body);
            message.setQos(1);

            mqttClient.publish(mqttTopic, message);
            logger.fine("Published operation: " + operation);

		} catch (SyncOperationSerializer.SyncSerializationException e) {
			logger.log(Level.SEVERE, "Failed to serialize operation", e);
		} catch (MqttException e) {
			logger.log(Level.SEVERE, "Failed to publish operation", e);
			notifyError(e);
		}
    }

    /**
	 * Request the current state from other replicas.
	 * Used when a new client joins and needs to synchronize.
	 */
	@Override
    public void requestState() {
		if (!connected) {
			logger.warning("Cannot request state: not connected to RabbitMQ");
			return;
		}

        try {
            SyncOperation stateRequest = new SyncOperation.Builder(SyncOperation.STATE_REQUEST)
                    .replicaId(replicaId)
                    .objectId("state-request")
                    .entityType("StateRequest")
                    .vectorClock(vectorClock.copy())
                    .build();

            byte[] body = SyncOperationSerializer.serializeToBytes(stateRequest);

            MqttMessage message = new MqttMessage(body);
            message.setQos(1);

            mqttClient.publish(mqttTopic, message);
            logger.info("Requested state from other replicas");

		} catch (SyncOperationSerializer.SyncSerializationException e) {
			logger.log(Level.SEVERE, "Failed to serialize state request", e);
		} catch (MqttException e) {
			logger.log(Level.SEVERE, "Failed to publish state request", e);
			notifyError(e);
		}
    }

    /**
	 * Send the current state as a response to a state request.
	 * 
	 * @param stateSnapshot the serialized state snapshot
	 * @param targetReplicaId the replica that requested the state (optional, null for broadcast)
	 */
	@Override
    public void sendStateResponse(String stateSnapshot, String targetReplicaId) {
    	if (!connected) {
			logger.warning("Cannot send state response: not connected to MQTT broker");
			return;
		}

        try {
            SyncOperation stateResponse = new SyncOperation.Builder(SyncOperation.STATE_RESPONSE)
                    .replicaId(replicaId)
                    .objectId("state-response")
                    .entityType("StateResponse")
                    .propertyIdentifier(targetReplicaId)
                    .newValue(stateSnapshot)
                    .vectorClock(vectorClock.copy())
                    .build();

            byte[] body = SyncOperationSerializer.serializeToBytes(stateResponse);

            MqttMessage message = new MqttMessage(body);
            message.setQos(1);

            mqttClient.publish(mqttTopic, message);
            logger.info("Sent state response to replica: " + (targetReplicaId != null ? targetReplicaId : "all"));

        } catch (SyncOperationSerializer.SyncSerializationException e) {
			logger.log(Level.SEVERE, "Failed to serialize state response", e);
		} catch (MqttException e) {
			logger.log(Level.SEVERE, "Failed to publish state response", e);
			notifyError(e);
		}
    }
    
    /**
	 * Add a listener for synchronization operations
	 */
    public void addListener(SyncOperationListener listener) {
		pcs.addPropertyChangeListener(listener);
    }
    
    /**
	 * Remove a listener
	 */
    public void removeListener(SyncOperationListener listener) {
        pcs.removePropertyChangeListener(listener);
    }
    
    @Override
    /**
	 * Closing connection from MQTT broker
	 */
    public void close() {
    	if (!connected) {
			return;
		}
        closing = true;

        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
                mqttClient.close();
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error during closing connection", e);
        }

        connected = false;
        closing = false;
        logger.info("Closing connection from MQTT broker");
        notifyDisconnected("Manual closing");
    }

    private void notifyConnected() {
        try {
    		pcs.firePropertyChange("CONNECTION", false, true);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error in connection listener", e);
        }
    }

    private void notifyDisconnected(String reason) {
        try {
    		pcs.firePropertyChange("DISCONNECTION", null, reason);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error in disconnection listener", e);
        }
        
    }

    private void notifyError(Throwable error) {
        try {
    		pcs.firePropertyChange("ERROR", null, error);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error in error listener", e);
        }
        
    }

    private void notifyStateRequested(String requestingReplicaId) {
        try {
    		pcs.firePropertyChange("STATE_REQUEST", null, requestingReplicaId);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error in state request listener", e);
        }
    }

    private void notifyStateReceived(String stateSnapshot, String fromReplicaId) {
        try {
    		pcs.firePropertyChange("STATE_RECEIVED", null, stateSnapshot + "FROM_REPLICA_ID" + fromReplicaId);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error in state received listener", e);
        }
    }
    
    /**
	 * Builder for creating ArtemisMQTTSyncManager instances
	 */
	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private String host = "localhost";
		private int port = 1883;
		private String username = "guest";
		private String password = "guest";
		private String exchangeName = "pamela.sync";
		private String routingKey = "operations";
		private boolean useSsl = false;

		public Builder host(String host) {
			this.host = host;
			return this;
		}

		public Builder port(int port) {
			this.port = port;
			return this;
		}

		public Builder credentials(String username, String password) {
			this.username = username;
			this.password = password;
			return this;
		}

		public Builder exchangeName(String exchangeName) {
			this.exchangeName = exchangeName;
			return this;
		}

		public Builder routingKey(String routingKey) {
			this.routingKey = routingKey;
			return this;
		}

		public Builder useSsl(boolean useSsl) {
			this.useSsl = useSsl;
			return this;
		}

		public ArtemisMQTTSyncManager build() {
			return new ArtemisMQTTSyncManager(host, port, username, password, exchangeName, routingKey, useSsl);
		}
	}
}
