/**
 * Copyright (c) 2026, Openflexo
 * 
 * This file is part of Pamela-core, a component of the software infrastructure 
 * developed at Openflexo.
 * 
 * Openflexo is dual-licensed under the European Union Public License (EUPL, either 
 * version 1.1 of the License, or any later version ), which is available at 
 * https://joinup.ec.europa.eu/software/page/eupl/licence-eupl
 * and the GNU General Public License (GPL, either version 3 of the License, or any 
 * later version), which is available at http://www.gnu.org/licenses/gpl.html .
 */

package org.openflexo.pamela.sync;

import java.beans.PropertyChangeEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openflexo.pamela.AccessibleProxyObject;
import org.openflexo.pamela.factory.EditingContextImpl;
import org.openflexo.pamela.factory.PamelaModelFactory;
import org.openflexo.pamela.factory.ProxyMethodHandler;
import org.openflexo.pamela.model.ModelProperty;
import org.openflexo.pamela.sync.SyncOperation;

import org.openflexo.pamela.undo.AddCommand;
import org.openflexo.pamela.undo.AtomicEdit;
import org.openflexo.pamela.undo.CreateCommand;
import org.openflexo.pamela.undo.DeleteCommand;
import org.openflexo.pamela.undo.SetCommand;

/**
 * Synchronized editing context that broadcasts PAMELA operations via RabbitMQ.
 * This class extends the standard EditingContext to add real-time collaborative
 * synchronization capabilities.
 * 
 * @author PAMELA Team
 */
public class SyncEditingContext extends EditingContextImpl implements SyncOperationListener {

	private static final Logger logger = Logger.getLogger(SyncEditingContext.class.getName());

	private final Map<String, RemoteOperationHandler> handlers = new ConcurrentHashMap<>();
	private final Map<Class<? extends AtomicEdit>, LocalEditHandler> localEditHandlers = new ConcurrentHashMap<>();
	private SyncManager syncManager;
	private final ObjectIdentityManager identityManager;
	private final SyncValueSerializer valueSerializer;
	private PamelaModelFactory modelFactory;
	private PropertyStateManager propertyStateManager;
	private org.openflexo.pamela.undo.UndoManager customUndoManager;
    

	@FunctionalInterface
	public interface LocalEditHandler {
		void handle(AtomicEdit<?> edit);
	}

	// Flag to prevent recursive sync when applying remote operations
	private final ThreadLocal<Boolean> applyingRemoteOperation = ThreadLocal.withInitial(() -> false);

	// Stores the replicaId of the remote operation currently being applied
	// Used by ProxyMethodHandler.getCurrentReplicaId() to tag AtomicEdits with the correct replicaId
	private final ThreadLocal<String> currentRemoteReplicaId = new ThreadLocal<>();

	// Buffer for operations during object creation (ensures CREATE is sent before SETs)
	// Key: objectId, Value: list of buffered operations
	private final Map<String, List<SyncOperation>> pendingOperations = new ConcurrentHashMap<>();
	
	// Track objects for which CREATE has been sent (by objectId)
	private final Map<String, Boolean> createdObjects = new ConcurrentHashMap<>();

	// Flag to automatically request state from other replicas on connection
	private boolean autoRequestStateOnConnect = false;

	// Flag to track if state has been received (to avoid multiple requests)
	private volatile boolean stateReceived = false;
	private CrdtLowestIdStrategy crdtStrategy; 
	private CrdtContext crdtContext; 

	// Root objects by entity type - these are used instead of creating new ones for remote operations
	// Key: entity class name, Value: the root object for that type
	private final Map<String, Object> rootObjects = new ConcurrentHashMap<>();

	/**
	 * Create a synchronized editing context without a sync manager.
	 * Call setSyncManager() to set one later.
	 */
	public SyncEditingContext() {
		super();
		this.syncManager = null;
		this.identityManager = new ObjectIdentityManager();
		this.valueSerializer = new SyncValueSerializer();
		this.propertyStateManager = new PropertyStateManager(this.identityManager); 
		this.crdtStrategy = new CrdtLowestIdStrategy(); 
		this.crdtContext = new CrdtContext(identityManager, propertyStateManager, modelFactory, valueSerializer, logger, createdObjects); 
		registerDefaultHandlers();
	}

	/**
	 * Create a synchronized editing context with a PamelaModelFactory.
	 * Call setSyncManager() to set a sync manager later.
	 */
	public SyncEditingContext(PamelaModelFactory modelFactory) {
		super();
		this.modelFactory = modelFactory;
		this.syncManager = null;
		this.identityManager = new ObjectIdentityManager();
		this.valueSerializer = new SyncValueSerializer();
		this.propertyStateManager = new PropertyStateManager(this.identityManager); 
		this.crdtStrategy = new CrdtLowestIdStrategy(); 
		this.crdtContext = new CrdtContext(identityManager, propertyStateManager, modelFactory, valueSerializer, logger, createdObjects); 
		this.customUndoManager = null;
		registerDefaultHandlers();
	}

// createUndoManager() is implemented later in this class to configure the UndoManager
// (kept here intentionally as a single definition to avoid duplicate method declarations).

	public <T extends AtomicEdit<?>> void registerLocalHandler(Class<T> editClass, LocalEditHandler handler) {
		if (editClass == null || handler == null) return;
		localEditHandlers.put(editClass, handler);
	}
 
    private void registerDefaultHandlers() {
        // Remote handlers (inbound)
        handlers.put(SyncOperation.SET, op -> crdtStrategy.applyRemoteModification(op, crdtContext, this));
        handlers.put(SyncOperation.ADD, op -> crdtStrategy.applyRemoteModification(op, crdtContext, this));
        handlers.put(SyncOperation.REMOVE, op -> crdtStrategy.applyRemoteModification(op, crdtContext, this));
		handlers.put(SyncOperation.REINDEX, op -> crdtStrategy.applyRemoteModification(op, crdtContext, this));
        handlers.put(SyncOperation.CREATE, op -> crdtStrategy.applyRemoteCreate(op, crdtContext));
		handlers.put(SyncOperation.DELETE, op -> crdtStrategy.applyRemoteDelete(op, crdtContext));


		 handlers.put(SyncOperation.STATE_REQUEST, operation -> {
           try {
                // caller requests state; pass requesting replica id
               onStateRequested(operation.getReplicaId());
           } catch (Exception e) {
               logger.log(Level.WARNING, "Failed to handle STATE_REQUEST", e);
           }
       });

       handlers.put(SyncOperation.STATE_RESPONSE, operation -> {
           try {
               // STATE_RESPONSE carries the serialized snapshot
               onStateReceived(operation.getNewValueSerialized(), operation.getReplicaId());
           } catch (Exception e) {
               logger.log(Level.WARNING, "Failed to handle STATE_RESPONSE", e);
           }
       });
	   
       // Local handlers (outbound)
       registerLocalHandler(SetCommand.class, edit -> {
            SetCommand<?> s = (SetCommand<?>) edit;
            // Skip SET operations for list properties - lists use ADD/REMOVE
            if (s.getNewValue() instanceof java.util.List || s.getOldValue() instanceof java.util.List) {
                return;
            }
            sendToCloud(buildBaseOp(SyncOperation.SET, s.getObject(), s)
                .oldValue(serializeValue(s.getOldValue()))
                .newValue(serializeValue(s.getNewValue()))
                .build());
        });

        registerLocalHandler(AddCommand.class, edit -> {
            AddCommand<?> a = (AddCommand<?>) edit;
            sendToCloud(buildBaseOp(SyncOperation.ADD, a.getObject(), a)
                .newValue(serializeValue(a.getAddedValue()))
                .index(a.getIndex())
                .build());
        });

        registerLocalHandler(CreateCommand.class, edit -> {
            sendToCloud(new SyncOperation.Builder(SyncOperation.CREATE)
                .replicaId(getReplicaId())
                .objectId(identityManager.getOrCreateObjectId(edit.getObject()))
                .entityType(getEntityTypeName(edit.getObject()))
                .build());
        });

        registerLocalHandler(DeleteCommand.class, edit -> {
            sendToCloud(new SyncOperation.Builder(SyncOperation.DELETE)
                .replicaId(getReplicaId())
                .objectId(identityManager.getObjectId(edit.getObject()))
                .entityType(getEntityTypeName(edit.getObject()))
                .build());
        });

		registerLocalHandler(org.openflexo.pamela.undo.RemoveCommand.class, edit -> {
    	org.openflexo.pamela.undo.RemoveCommand<?> r = (org.openflexo.pamela.undo.RemoveCommand<?>) edit;
    	sendToCloud(buildBaseOp(SyncOperation.REMOVE, r.getObject(), r)
        .oldValue(serializeValue(r.getRemovedValue()))
        .build());
		});
    }
    
	/**
	 * Serialize a value for sync operations.
	 * Uses reference serialization for PAMELA proxy objects.
	 */
	private String serializeValue(Object value) {
		if (value == null) {
			return valueSerializer.serialize(null);
		}
		if (modelFactory != null && modelFactory.isProxyObject(value)) {
			return valueSerializer.serializeReference(value, identityManager);
		}
		return valueSerializer.serialize(value);
	}

	private void sendToCloud(SyncOperation op) {
    if (isApplyingRemoteOperation() || syncManager == null || !syncManager.isConnected()) {
        return;
    }

    syncManager.publishOperation(op);
}


	private SyncOperation.Builder buildBaseOp(String type, Object target, AtomicEdit<?> edit) {
        ModelProperty<?> prop = resolveModelPropertyFromEdit(edit, target);
        return new SyncOperation.Builder(type)
            .replicaId(getReplicaId())
            .objectId(identityManager.getOrCreateObjectId(target))
            .entityType(getEntityTypeName(target))
            .propertyIdentifier(prop != null ? prop.getPropertyIdentifier() : null)
            .valueType(prop != null ? prop.getType().getName() : null);
    }

    // Helper: resolve ModelProperty from an AtomicEdit using either a property object or a property identifier exposed by the edit.
    private ModelProperty<?> resolveModelPropertyFromEdit(AtomicEdit<?> edit, Object targetObject) {
        if (targetObject == null || modelFactory == null) return null;
        try {
            // Try direct getProperty() returning a ModelProperty (if present)
            try {
                java.lang.reflect.Method mProp = edit.getClass().getMethod("getProperty");
                Object propObj = mProp.invoke(edit);
                if (propObj instanceof ModelProperty) {
                    return (ModelProperty<?>) propObj;
                }
            } catch (NoSuchMethodException ignored) {}

            // Try getPropertyIdentifier() or getPropertyName()
            String propId = null;
            try {
                java.lang.reflect.Method mId = edit.getClass().getMethod("getPropertyIdentifier");
                propId = (String) mId.invoke(edit);
            } catch (NoSuchMethodException ignored) {
                try {
                    java.lang.reflect.Method mName = edit.getClass().getMethod("getPropertyName");
                    propId = (String) mName.invoke(edit);
                } catch (NoSuchMethodException ignored2) {}
            }

            if (propId != null) {
                ProxyMethodHandler<?> handler = modelFactory.getHandler(targetObject);
                if (handler != null) {
                    return handler.getModelEntity().getModelProperty(propId);
                }
            }
        } catch (Exception e) {
            logger.log(Level.FINE, "Failed to resolve ModelProperty from edit: " + edit.getClass(), e);
        }
        return null;
    }

    /**
	 * Add new operation at runtime 
     */
    public void registerHandler(String type, RemoteOperationHandler handler) {
        handlers.put(type, handler);
    }

	
/* 
	private LocalEditHandler findLocalHandlerFor(Class<? extends AtomicEdit> cls) {
		// Exact match first
		LocalEditHandler h = localEditHandlers.get(cls);
		if (h != null) return h;
		// Then try assignable matches (allows registering handlers for supertypes)
		for (Map.Entry<Class<? extends AtomicEdit>, LocalEditHandler> e : localEditHandlers.entrySet()) {
			if (e.getKey().isAssignableFrom(cls)) {
				return e.getValue();
			}
		}
		return null;
	}
		*/

	/**
	 * Create a synchronized editing context with custom sync manager
	 */
	public SyncEditingContext(SyncManager syncManager) {
		super();
		this.syncManager = syncManager;
		this.identityManager = new ObjectIdentityManager();
		this.valueSerializer = new SyncValueSerializer();
		this.propertyStateManager = new PropertyStateManager(this.identityManager); 
		if (this.syncManager != null) {
			this.syncManager.addListener(this);
		}
		this.crdtStrategy = new CrdtLowestIdStrategy(); 
		this.crdtContext = new CrdtContext(identityManager, propertyStateManager, modelFactory, valueSerializer, logger, createdObjects); 
		registerDefaultHandlers();
	}

	/**
	 * Set the model factory for this context
	 */
	public void setModelFactory(PamelaModelFactory modelFactory) {
		this.modelFactory = modelFactory;
	}

	/**
	 * Get the sync manager
	 */
	public SyncManager getSyncManager() {
		return syncManager;
	}

	/**
	 * Set the sync manager
	 */
	public void setSyncManager(SyncManager syncManager) {
		this.syncManager = syncManager;
		if (this.syncManager != null) {
			this.syncManager.addListener(this);
		}
	}

	/**
	 * Get the identity manager
	 */
	public ObjectIdentityManager getIdentityManager() {
		return identityManager;
	}

	/**
	 * Get the value serializer for registering custom type serializers.
	 * Applications can register their own serializers for custom types.
	 */
	public SyncValueSerializer getValueSerializer() {
		return valueSerializer;
	}

	/**
	 * Get the replica ID
	 */
	public String getReplicaId() {
		return syncManager != null ? syncManager.getReplicaId() : null;
	}

	/**
	 * Check if currently applying a remote operation
	 */
	public boolean isApplyingRemoteOperation() {
		return applyingRemoteOperation.get();
	}
	
	public PropertyStateManager getPropertyStateManager() {
		return this.propertyStateManager;
	}

	/**
	 * Get the replicaId of the operation currently being processed.
	 * If applying a remote operation, returns the remote replica's ID.
	 * Otherwise, returns the local replica's ID.
	 * This is used by ProxyMethodHandler to tag AtomicEdits with the correct replicaId.
	 */
	public String getCurrentOperationReplicaId() {
		String remoteId = currentRemoteReplicaId.get();
		if (remoteId != null) {
			return remoteId;
		}
		return getReplicaId();
	}

	/**
	 * Get the interface name for a PAMELA object.
	 * Returns the implemented interface name (e.g., "org.example.Book") instead of
	 * the proxy class name (e.g., "Book$BookImpl_$$_jvst806_1").
	 */
	private <I> String getEntityTypeName(I object) {
		if (modelFactory != null && modelFactory.isProxyObject(object)) {
			ProxyMethodHandler<?> handler = modelFactory.getHandler(object);
			if (handler != null) {
				return handler.getModelEntity().getImplementedInterface().getName();
			}
		}
		return object.getClass().getName();
	}

	/**
	 * Ensure a PAMELA proxy object has been created (has ID and CREATE operation sent).
	 * This is used to ensure embedded objects are properly synced before being referenced.
	 *
	 * @param object the object to ensure is created
	 * @return the serialized reference string for the object
	 */
	private String ensureObjectCreatedAndSerialize(Object object) {
		if (object == null || modelFactory == null || !modelFactory.isProxyObject(object)) {
			return valueSerializer.serialize(object);
		}

		// Get or create an ID for this object
		String objectId = identityManager.getOrCreateObjectId(object);

		// Check if CREATE was already sent for this object
		if (!createdObjects.containsKey(objectId)) {
			// Send CREATE operation for this embedded object
			String entityType = getEntityTypeName(object);
			SyncOperation createOp = new SyncOperation.Builder(SyncOperation.CREATE)
					.replicaId(syncManager.getReplicaId())
					.objectId(objectId)
					.entityType(entityType)
					.build();
			syncManager.publishOperation(createOp);
			createdObjects.put(objectId, Boolean.TRUE);
			logger.fine("Sent CREATE for embedded object: " + objectId + " (" + entityType + ")");
		}

		// Now serialize as reference
		return valueSerializer.serializeReference(object, identityManager);
	}

public <I> void broadcast(I object,ModelProperty<? super I> property,Object oldValue,Object newValue,int index,String operationType){
	// Debug: log broadcast attempts
	logger.info("broadcast() called: type=" + operationType + ", property=" +
			(property != null ? property.getPropertyIdentifier() : "null") +
			", connected=" + (syncManager != null && syncManager.isConnected()));

	if (syncManager == null || isApplyingRemoteOperation() || !syncManager.isConnected()) {
		logger.info("broadcast() skipped: syncManager=" + (syncManager != null) +
				", applyingRemote=" + isApplyingRemoteOperation() +
				", connected=" + (syncManager != null && syncManager.isConnected()));
        return;
    }

	// Skip properties with ignoreType=true (editor-local state like mouseClickControls)
	if (property != null && property.ignoreType()) {
		logger.fine("broadcast() skipped ignoreType property: " + property.getPropertyIdentifier());
		return;
	}

	// Skip SET operations for list properties - lists should use ADD/REMOVE
	if (operationType.equals(SyncOperation.SET) && property != null
			&& property.getCardinality() == org.openflexo.pamela.annotations.Getter.Cardinality.LIST) {
		logger.info("broadcast() skipped SET for list property: " + property.getPropertyIdentifier());
		return;
	}

	try{
		String objectId = identityManager.getOrCreateObjectId(object);
        String entityType = getEntityTypeName(object);
		 SyncOperation.Builder builder = new SyncOperation.Builder(operationType)
                .replicaId(syncManager.getReplicaId())
                .objectId(objectId)
                .entityType(entityType);

		if (property != null) {
            builder.propertyIdentifier(property.getPropertyIdentifier())
                   .valueType(property.getType().getName());
        }

		// Serialize oldValue if relevant (remove and set)
		if (oldValue != null && (operationType.equals(SyncOperation.SET)
				|| operationType.equals(SyncOperation.REMOVE))) {
			String serializedOld = ensureObjectCreatedAndSerialize(oldValue);
			// Skip empty/meaningless serialized values (e.g., empty DataBinding)
			if (serializedOld != null && !serializedOld.isEmpty()) {
				builder.oldValue(serializedOld);
			}
		}

		// Serialize newValue if relevant (add and set and reindex)
		// Uses ensureObjectCreatedAndSerialize to ensure embedded PAMELA objects
		// have CREATE operations sent before they are referenced
		if (newValue != null && (operationType.equals(SyncOperation.SET)
				|| operationType.equals(SyncOperation.ADD)
				|| operationType.equals(SyncOperation.REINDEX))) {
			String serializedNew = ensureObjectCreatedAndSerialize(newValue);
			// Skip empty/meaningless serialized values (e.g., empty DataBinding)
			if (serializedNew != null && !serializedNew.isEmpty()) {
				builder.newValue(serializedNew);
			}
		}
		 // Index for ADD/REINDEX
		if (operationType.equals(SyncOperation.ADD) || operationType.equals(SyncOperation.REINDEX)) {
            builder.index(index);
        }
		VectorClock clock = syncManager.getVectorClock();
		builder.vectorClock(clock.copy());

		SyncOperation operation = builder.build();
		propertyStateManager.storeIntoMap(operation);
		syncManager.publishOperation(operation);
		if(operationType.equals(SyncOperation.CREATE)){
		createdObjects.put(objectId, Boolean.TRUE);
			
		// Then send any buffered operations for this object
		List<SyncOperation> buffered = pendingOperations.remove(objectId);
		if (buffered != null) {
			for (SyncOperation bufferedOp : buffered) {
				syncManager.publishOperation(bufferedOp);
			}
			logger.fine("Sent " + buffered.size() + " buffered operations for: " + objectId);
		}

	}
	} catch (Exception e) {
	        logger.log(Level.SEVERE, "Failed to broadcast " + operationType + " operation", e);
	    }
	}

	/**
	 * Receive the evt from RabbitMQSyncManager and redirect to the correct operation
	 */
	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		String msg = evt.getPropertyName();
		if(msg.equals("OPERATION_RECEIVED"))
			onOperationReceived((SyncOperation)evt.getNewValue());
		else if(msg.equals("CONNECTION"))
			onConnected();
		else if(msg.equals("DISCONNECTION"))
			onDisconnected((String)evt.getNewValue());
		else if(msg.equals("ERROR"))
			onError((Throwable)evt.getNewValue());
		else if(msg.equals("STATE_REQUEST"))
			onStateRequested((String)evt.getNewValue());
		else if(msg.equals("STATE_RECEIVED")) {
			String value = (String)evt.getNewValue();
			String stateSnapshot = value.split("FROM_REPLICA_ID")[0];
			String fromReplicaId = value.split("FROM_REPLICA_ID")[1];
			onStateReceived(stateSnapshot, fromReplicaId);
		}
	}
	
	// SyncOperationListener implementation
	@Override
    public void onOperationReceived(SyncOperation operation) {
        if (modelFactory == null) {
			logger.warning("ModelFactory not set, cannot apply remote operation");
			return;
		}
	
		applyingRemoteOperation.set(true);
		currentRemoteReplicaId.set(operation.getReplicaId());
		try {
			switch (operation.getOperationType()) {
				case "CREATE":
					crdtStrategy.applyRemoteCreate(operation,crdtContext);
					break;
				case "DELETE":
					crdtStrategy.applyRemoteDelete(operation,crdtContext);
					break;
				case "SET": case "ADD": case "REMOVE":
					crdtStrategy.applyRemoteModification(operation,crdtContext,this);
					break;
				case "REINDEX":
					crdtStrategy.applyRemoteModification(operation,crdtContext,this);
					break;
				default:
					logger.warning("Unknown operation type: " + operation.getOperationType());
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to apply remote operation: " + operation, e);
		} finally {
			applyingRemoteOperation.set(false);
			currentRemoteReplicaId.remove();

        }
    }

	@Override
	public void onConnected() {
		logger.info("Sync connected as replica: " + syncManager.getReplicaId());
		
		// Automatically request state from other replicas if enabled
		if (autoRequestStateOnConnect && !stateReceived) {
			logger.info("Automatically requesting state from other replicas...");
			requestStateSync();
		}
	}

	@Override
	public void onDisconnected(String reason) {
		logger.info("Sync disconnected: " + reason);
	}

	@Override
	public void onError(Throwable error) {
		logger.log(Level.SEVERE, "Sync error", error);
	}

	@Override
	public void onStateRequested(String requestingReplicaId) {
		logger.info("State requested by replica: " + requestingReplicaId);
		if (syncManager == null) {
			return;
		}
		
		try {
			String stateSnapshot = serializeCurrentState();
			syncManager.sendStateResponse(stateSnapshot, requestingReplicaId);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to send state response", e);
		}
	}

	@Override
	public void onStateReceived(String stateSnapshot, String fromReplicaId) {
		logger.info("State received from replica: " + fromReplicaId);

		// Mark state as received to prevent duplicate requests
		stateReceived = true;

		// Set the remote replicaId so AtomicEdits are tagged correctly
		currentRemoteReplicaId.set(fromReplicaId);
		try {
			restoreFromSnapshot(stateSnapshot);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to restore from snapshot", e);
		} finally {
			currentRemoteReplicaId.remove();
		}
	}

	/**
	 * Enable or disable automatic state request on connection.
	 * When enabled, the context will automatically request state from other replicas
	 * upon connecting to the sync manager.
	 * 
	 * @param autoRequest true to enable automatic state request
	 */
	public void setAutoRequestStateOnConnect(boolean autoRequest) {
		this.autoRequestStateOnConnect = autoRequest;
	}

	/**
	 * Register a root object for a specific entity type.
	 * When remote operations arrive for this entity type and the object doesn't exist,
	 * the root object will be used instead of creating a new one.
	 * This is essential for objects like Diagram that are created locally and observed by the UI.
	 *
	 * @param object the root object to register
	 */
	public void registerRootObject(Object object) {
		if (object == null) return;
		ProxyMethodHandler<?> handler = modelFactory.getHandler(object);
		if (handler != null) {
			String entityType = handler.getModelEntity().getImplementedInterface().getName();
			rootObjects.put(entityType, object);
			logger.info("Registered root object for type: " + entityType);
		}
	}

	/**
	 * Get the root object for an entity type, if registered.
	 *
	 * @param entityType the entity class name
	 * @return the root object, or null if not registered
	 */
	public Object getRootObject(String entityType) {
		return rootObjects.get(entityType);
	}

	/**
	 * Check if automatic state request on connect is enabled.
	 * 
	 * @return true if enabled
	 */
	public boolean isAutoRequestStateOnConnect() {
		return autoRequestStateOnConnect;
	}

	/**
	 * Check if state has been received from another replica.
	 *
	 * @return true if state was received
	 */
	public boolean isStateReceived() {
		return stateReceived;
	}

	/**
	 * Creates and configures an UndoManager for this sync editing context.
	 * The UndoManager is configured with the local replica ID to filter out remote edits.
	 */
	@Override
	public org.openflexo.pamela.undo.UndoManager createUndoManager() {
		org.openflexo.pamela.undo.UndoManager undoManager = super.createUndoManager();

		// Configure the UndoManager with the local replica ID
		// This ensures it only tracks edits from the local replica
		if (syncManager != null) {
			undoManager.setLocalReplicaId(syncManager.getReplicaId());
		}

		return undoManager;
	}

	/**
	 * Request state from other replicas.
	 * Call this when a new client joins to get the current state.
	 */
	public void requestStateSync() {
		if (syncManager != null && syncManager.isConnected()) {
			syncManager.requestState();
		}
	}

	/**
	 * Serialize the current state of all tracked objects into a snapshot.
	 * 
	 * @return JSON string containing all objects and their properties
	 */
	public String serializeCurrentState() {
		StringBuilder json = new StringBuilder();
		json.append("{\"objects\":[");
		
		Map<String, Object> allObjects = identityManager.getAllObjects();
		boolean first = true;
		
		for (Map.Entry<String, Object> entry : allObjects.entrySet()) {
			String objectId = entry.getKey();
			Object object = entry.getValue();
			
			if (!first) {
				json.append(",");
			}
			first = false;
			
			json.append("{");
			json.append("\"id\":\"").append(escapeJson(objectId)).append("\",");
			json.append("\"type\":\"").append(escapeJson(getEntityTypeName(object))).append("\",");
			json.append("\"properties\":{");
			
			try {
				ProxyMethodHandler<?> handler = modelFactory.getHandler(object);
				if (handler != null) {
					boolean firstProp = true;
					java.util.Iterator<? extends ModelProperty<?>> propIterator = handler.getModelEntity().getProperties();
					while (propIterator.hasNext()) {
						ModelProperty<?> property = propIterator.next();
						if (property.getGetter() != null) {
							Object value = handler.invokeGetter(property.getPropertyIdentifier());
							if (value != null) {
								if (!firstProp) {
									json.append(",");
								}
								firstProp = false;
								
								String serializedValue;
								if (modelFactory.isProxyObject(value)) {
									serializedValue = valueSerializer.serializeReference(value, identityManager);
								} else if (value instanceof List) {
									serializedValue = serializeList((List<?>) value);
								} else {
									serializedValue = valueSerializer.serialize(value);
								}
								
								json.append("\"").append(escapeJson(property.getPropertyIdentifier())).append("\":");
								json.append("\"").append(escapeJson(serializedValue)).append("\"");
							}
						}
					}
				}
			} catch (Exception e) {
				logger.log(Level.WARNING, "Failed to serialize object: " + objectId, e);
			}
			
			json.append("}}");
		}
		
		json.append("]}");
		return json.toString();
	}

	/**
	 * Restore the local state from a snapshot received from another replica.
	 * 
	 * @param stateSnapshot JSON string containing the state snapshot
	 */
	public void restoreFromSnapshot(String stateSnapshot) {
		if (stateSnapshot == null || stateSnapshot.isEmpty()) {
			logger.warning("Empty state snapshot received");
			return;
		}
		
		applyingRemoteOperation.set(true);
		try {
			// Simple JSON parsing for the snapshot format
			// Format: {"objects":[{"id":"...","type":"...","properties":{...}}, ...]}
			
			int objectsStart = stateSnapshot.indexOf("[");
			int objectsEnd = stateSnapshot.lastIndexOf("]");
			if (objectsStart < 0 || objectsEnd < 0) {
				logger.warning("Invalid snapshot format");
				return;
			}
			
			String objectsJson = stateSnapshot.substring(objectsStart + 1, objectsEnd);
			if (objectsJson.trim().isEmpty()) {
				logger.info("Empty state snapshot - no objects to restore");
				return;
			}
			
			// Parse each object
			List<ObjectSnapshot> snapshots = parseObjectSnapshots(objectsJson);
			
			// First pass: create all objects
			for (ObjectSnapshot snapshot : snapshots) {
				if (!identityManager.hasObject(snapshot.id)) {
					try {
						Class<?> entityClass = Class.forName(snapshot.type);
						Object newObject = modelFactory._newInstance(entityClass, false);
						
						ProxyMethodHandler<?> handler = modelFactory.getHandler(newObject);
						if (handler != null) {
							handler.setDeserializing(true);
						}
						
						identityManager.registerObject(newObject, snapshot.id);
						createdObjects.put(snapshot.id, Boolean.TRUE);
						logger.fine("Created object from snapshot: " + snapshot.id);
					} catch (Exception e) {
						logger.log(Level.WARNING, "Failed to create object from snapshot: " + snapshot.id, e);
					}
				}
			}
			
			// Second pass: set properties (after all objects exist for reference resolution)
			for (ObjectSnapshot snapshot : snapshots) {
				Object object = identityManager.getObject(snapshot.id);
				if (object == null) {
					continue;
				}
				
				try {
					ProxyMethodHandler<?> handler = modelFactory.getHandler(object);
					if (handler != null) {
						for (Map.Entry<String, String> prop : snapshot.properties.entrySet()) {
							try {
								ModelProperty<?> modelProperty = handler.getModelEntity().getModelProperty(prop.getKey());
								if (modelProperty != null) {
									String serializedValue = prop.getValue();
									
									// Handle list properties specially
									if (serializedValue.startsWith("[") && serializedValue.endsWith("]")) {
										// It's a list - use adder for each element
										restoreListProperty(handler, modelProperty, serializedValue);
									} else {
										Object value = valueSerializer.deserialize(serializedValue, modelProperty.getType(), this);
										handler.invokeSetter(prop.getKey(), value);
									}
								}
							} catch (Exception e) {
								logger.log(Level.FINE, "Failed to set property: " + prop.getKey(), e);
							}
						}
					}
				} catch (Exception e) {
					logger.log(Level.WARNING, "Failed to restore object properties: " + snapshot.id, e);
				}
			}
			
			logger.info("Restored " + snapshots.size() + " objects from snapshot");
			
		} finally {
			applyingRemoteOperation.set(false);
		}
	}

	/**
	 * Restore a list property from serialized format using adder method.
	 */
	private void restoreListProperty(ProxyMethodHandler<?> handler, ModelProperty<?> property, String serializedList) {
		// Remove brackets: [item1,item2,item3] -> item1,item2,item3
		String content = serializedList.substring(1, serializedList.length() - 1);
		if (content.trim().isEmpty()) {
			return; // Empty list
		}
		
		// Split by comma (careful with nested structures)
		List<String> items = splitListItems(content);
		
		for (String item : items) {
			item = item.trim();
			if (item.isEmpty()) {
				continue;
			}
			
			try {
				// Deserialize the item
				Object value = valueSerializer.deserialize(item, property.getType(), this);
				if (value != null) {
					// Use adder to add to the list
					handler.invokeAdder(property.getPropertyIdentifier(), value);
					logger.fine("Added item to list property " + property.getPropertyIdentifier() + ": " + value);
				}
			} catch (Exception e) {
				logger.log(Level.WARNING, "Failed to add item to list: " + item, e);
			}
		}
	}

	/**
	 * Split list items by comma, handling nested structures.
	 */
	private List<String> splitListItems(String content) {
		List<String> items = new ArrayList<>();
		int depth = 0;
		StringBuilder current = new StringBuilder();
		
		for (int i = 0; i < content.length(); i++) {
			char c = content.charAt(i);
			if (c == '[' || c == '{') {
				depth++;
				current.append(c);
			} else if (c == ']' || c == '}') {
				depth--;
				current.append(c);
			} else if (c == ',' && depth == 0) {
				items.add(current.toString());
				current = new StringBuilder();
			} else {
				current.append(c);
			}
		}
		
		if (current.length() > 0) {
			items.add(current.toString());
		}
		
		return items;
	}

	private String serializeList(List<?> list) {
		StringBuilder sb = new StringBuilder("[");
		boolean first = true;
		for (Object item : list) {
			if (!first) {
				sb.append(",");
			}
			first = false;
			
			if (item != null && modelFactory != null && modelFactory.isProxyObject(item)) {
				sb.append(valueSerializer.serializeReference(item, identityManager));
			} else {
				sb.append(valueSerializer.serialize(item));
			}
		}
		sb.append("]");
		return sb.toString();
	}

	private String escapeJson(String value) {
		if (value == null) return "";
		return value.replace("\\", "\\\\")
					.replace("\"", "\\\"")
					.replace("\n", "\\n")
					.replace("\r", "\\r")
					.replace("\t", "\\t");
	}

	private static class ObjectSnapshot {
		String id;
		String type;
		Map<String, String> properties = new java.util.HashMap<>();
	}

	private List<ObjectSnapshot> parseObjectSnapshots(String objectsJson) {
		List<ObjectSnapshot> snapshots = new ArrayList<>();
		
		// Simple parsing - find each object block
		int depth = 0;
		int objectStart = -1;
		
		for (int i = 0; i < objectsJson.length(); i++) {
			char c = objectsJson.charAt(i);
			if (c == '{') {
				if (depth == 0) {
					objectStart = i;
				}
				depth++;
			} else if (c == '}') {
				depth--;
				if (depth == 0 && objectStart >= 0) {
					String objectJson = objectsJson.substring(objectStart, i + 1);
					ObjectSnapshot snapshot = parseObjectSnapshot(objectJson);
					if (snapshot != null) {
						snapshots.add(snapshot);
					}
					objectStart = -1;
				}
			}
		}
		
		return snapshots;
	}

	private ObjectSnapshot parseObjectSnapshot(String objectJson) {
		ObjectSnapshot snapshot = new ObjectSnapshot();
		
		// Extract id
		snapshot.id = extractJsonValue(objectJson, "id");
		snapshot.type = extractJsonValue(objectJson, "type");
		
		if (snapshot.id == null || snapshot.type == null) {
			return null;
		}
		
		// Extract properties block
		int propsStart = objectJson.indexOf("\"properties\":{");
		if (propsStart >= 0) {
			propsStart += 14; // length of "properties":{ 
			int propsEnd = findMatchingBrace(objectJson, propsStart - 1);
			if (propsEnd > propsStart) {
				String propsJson = objectJson.substring(propsStart, propsEnd);
				parseProperties(propsJson, snapshot.properties);
			}
		}
		
		return snapshot;
	}

	private String extractJsonValue(String json, String key) {
		String pattern = "\"" + key + "\":\"";
		int start = json.indexOf(pattern);
		if (start < 0) return null;
		start += pattern.length();
		
		int end = start;
		while (end < json.length()) {
			char c = json.charAt(end);
			if (c == '"' && json.charAt(end - 1) != '\\') {
				break;
			}
			end++;
		}
		
		if (end > start) {
			return unescapeJson(json.substring(start, end));
		}
		return null;
	}

	private int findMatchingBrace(String json, int openPos) {
		int depth = 0;
		for (int i = openPos; i < json.length(); i++) {
			char c = json.charAt(i);
			if (c == '{') depth++;
			else if (c == '}') {
				depth--;
				if (depth == 0) return i;
			}
		}
		return -1;
	}

	private void parseProperties(String propsJson, Map<String, String> properties) {
		// Simple key-value parsing for "key":"value" pairs
		int i = 0;
		while (i < propsJson.length()) {
			// Find key start
			int keyStart = propsJson.indexOf('"', i);
			if (keyStart < 0) break;
			keyStart++;
			
			int keyEnd = propsJson.indexOf('"', keyStart);
			if (keyEnd < 0) break;
			
			String key = propsJson.substring(keyStart, keyEnd);
			
			// Find value after ":"
			int colonPos = propsJson.indexOf(':', keyEnd);
			if (colonPos < 0) break;
			
			int valueStart = propsJson.indexOf('"', colonPos);
			if (valueStart < 0) break;
			valueStart++;
			
			int valueEnd = valueStart;
			while (valueEnd < propsJson.length()) {
				char c = propsJson.charAt(valueEnd);
				if (c == '"' && propsJson.charAt(valueEnd - 1) != '\\') {
					break;
				}
				valueEnd++;
			}
			
			if (valueEnd > valueStart) {
				properties.put(key, unescapeJson(propsJson.substring(valueStart, valueEnd)));
			}
			
			i = valueEnd + 1;
		}
	}

	private String unescapeJson(String value) {
		return value.replace("\\\"", "\"")
					.replace("\\\\", "\\")
					.replace("\\n", "\n")
					.replace("\\r", "\r")
					.replace("\\t", "\t");
	}	
}
