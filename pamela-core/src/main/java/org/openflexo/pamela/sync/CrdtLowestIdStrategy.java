package org.openflexo.pamela.sync; 
import java.util.logging.Logger;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import org.openflexo.pamela.factory.ProxyMethodHandler;
import org.openflexo.pamela.model.ModelProperty;
import org.openflexo.pamela.sync.SyncOperation.OperationType;



public class CrdtLowestIdStrategy implements ICrdtStrategy{
	
	public void applyRemoteCreate(SyncOperation operation, CrdtContext crdtContext) {
		// Check if object already exists
		if (crdtContext.identityManager.hasObject(operation.getObjectId())) {
			crdtContext.logger.fine("Object already exists: " + operation.getObjectId());
			return;
		}

		try {
			// Load the entity class
			Class<?> entityClass = Class.forName(operation.getEntityType());

			// Create a new instance using _newInstance (like deserialization does)
			// This creates the object without requiring the initializer
			Object newObject = crdtContext.modelFactory._newInstance(entityClass, false);
			
			// Mark the object as deserializing so it can receive property updates
			// without failing the "uninitialized" check
			ProxyMethodHandler<?> handler = crdtContext.modelFactory.getHandler(newObject);
			if (handler != null) {
				handler.setDeserializing(true);
			}

			// Register with the specified ID
			crdtContext.identityManager.registerObject(newObject, operation.getObjectId());
			
			// Mark as known so SET operations work properly
			crdtContext.createdObjects.put(operation.getObjectId(), Boolean.TRUE);

			crdtContext.logger.fine("Created remote object: " + operation.getObjectId());

		} catch (Exception e) {
			crdtContext.logger.log(Level.SEVERE, "Failed to apply remote CREATE", e);
		}
	}

	public void applyRemoteDelete(SyncOperation operation, CrdtContext crdtContext) {
		Object target = crdtContext.identityManager.getObject(operation.getObjectId());
		if (target == null) {
			crdtContext.logger.fine("Object already deleted or not found: " + operation.getObjectId());
			return;
		}

		try {
			ProxyMethodHandler<?> handler = crdtContext.modelFactory.getHandler(target);
			if (handler != null) {
				handler.invokeDeleter(target);
			}

			crdtContext.identityManager.unregisterById(operation.getObjectId());

		} catch (Exception e) {
			crdtContext.logger.log(Level.SEVERE, "Failed to apply remote DELETE", e);
		}
	
	}

	/**
	 * Ensure a remote object exists, creating it if necessary.
	 * This handles the case where operations arrive out of order.
	 * 
	 * @param objectId the object ID
	 * @param entityType the entity class name
	 * @return the object, or null if creation failed
	 */
	private Object ensureRemoteObjectExists(String objectId, String entityType,CrdtContext crdtContext) {					
		if (entityType == null) {
			crdtContext.logger.warning("Cannot create object without entityType for ID: " + objectId);
			return null;
		}

		try {
			Class<?> entityClass = Class.forName(entityType);
			
			// Create using _newInstance (bypasses initializer requirement)
			Object newObject = crdtContext.modelFactory._newInstance(entityClass, false);
			
			// Mark as deserializing to allow setters without initialization
			ProxyMethodHandler<?> handler = crdtContext.modelFactory.getHandler(newObject);
			if (handler != null) {
				handler.setDeserializing(true);
			}
			
			// Register with the specified ID
			crdtContext.identityManager.registerObject(newObject, objectId);
			
			// Mark as known so SET operations work properly
			crdtContext.createdObjects.put(objectId, Boolean.TRUE);
			
			crdtContext.logger.fine("Auto-created remote object: " + objectId);
			return newObject;
			
		} catch (Exception e) {
			crdtContext.logger.log(Level.WARNING, "Failed to auto-create remote object: " + objectId, e);
			return null;
		}
	}


	@Override
	public void applyRemoteModification(SyncOperation operation, CrdtContext crdtContext,
			SyncEditingContext syncContext) {
		Object target = crdtContext.identityManager.getObject(operation.getObjectId());
		Map<String, SyncOperation> objectMap = crdtContext.propertyStateManager.getMapCrdt().get(operation.getObjectId());
		SyncOperation lastOp = null;
		if(objectMap != null)
			lastOp = objectMap.get(operation.getPropertyIdentifier()); 

		if (target == null) {
			// Object doesn't exist yet : if it has already been deleted then don't apply the modification and return 
			// Else create the object 			
			if (lastOp != null && lastOp.getOperationType().equals(SyncOperation.OperationType.DELETE)){
				return; 
			}
			target = ensureRemoteObjectExists(operation.getObjectId(), operation.getEntityType(),crdtContext);			
		}
		try { 
			ProxyMethodHandler<?> handler = crdtContext.modelFactory.getHandler(target);
			if (handler != null) {
				ModelProperty<?> property = handler.getModelEntity().getModelProperty(operation.getPropertyIdentifier());
				if (property != null) {
					String value; 
					if(operation.getOperationType().equals(OperationType.REMOVE)){
						value = operation.getOldValueSerialized(); 
					}
					else{value= operation.getNewValueSerialized();}

					Object newValue = crdtContext.valueSerializer.deserialize(						
							value,
							property.getType(),
							syncContext
					);
					int index = operation.getIndex();

					switch(operation.getOperationType()){
						case SET:
						if (lastOp != null && lastOp.getOperationType().equals(SyncOperation.OperationType.SET)){
							//If the operation received is before the last operation in local according to the vector clock do nothing
							//Otherwise if the operation received is concurrent to the last operation in local and the id of the replica from distant operation is higher also do nothing
							if(operation.getVectorClock().compareTo(lastOp.getVectorClock())==-1 ||(operation.getVectorClock().compareTo(lastOp.getVectorClock())== 0 && UUID.fromString(lastOp.getReplicaId()).compareTo(UUID.fromString(operation.getReplicaId()))==-1)){
								return; 
							}
							else {
								handler.invokeSetter(operation.getPropertyIdentifier(), newValue); 
							}						
						}else{
							handler.invokeSetter(operation.getPropertyIdentifier(), newValue); 
						}				
						break; 
						case ADD: 	
						handler.invokeAdder(operation.getPropertyIdentifier(), newValue);
						break; 
						case REMOVE: 
						handler.invokeRemover(operation.getPropertyIdentifier(), newValue); 
						break; 
						case REINDEX:
						handler.invokeReindexer(operation.getPropertyIdentifier(), newValue, index);
						default: 
						break; 
					}		
					crdtContext.propertyStateManager.storeIntoMap(operation);			
				}
			}
		} catch (Exception e) {
			crdtContext.logger.log(Level.SEVERE, "Failed to apply" + operation.getOperationType(), e);
		}
	}
}