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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.openflexo.pamela.factory.ProxyMethodHandler;

import javassist.util.proxy.ProxyObject;

/**
 * Manages unique identifiers for PAMELA objects across distributed instances.
 * Each object gets a UUID that is stable across all replicas.
 * 
 * @author PAMELA Team
 */
public class ObjectIdentityManager {

	// Maps IDs to objects (for receiving remote operations)
	private final Map<String, Object> idToObject = new ConcurrentHashMap<>();

	/**
	 * Get the ProxyMethodHandler for a PAMELA object.
	 *
	 * @param object the PAMELA object
	 * @return the handler, or null if not a PAMELA proxy
	 */
	private ProxyMethodHandler<?> getHandler(Object object) {
		if (object instanceof ProxyObject) {
			Object handler = ((ProxyObject) object).getHandler();
			if (handler instanceof ProxyMethodHandler) {
				return (ProxyMethodHandler<?>) handler;
			}
		}
		return null;
	}

	/**
	 * Register an object with a new generated UUID.
	 * The UUID is stored on the object's ProxyMethodHandler.
	 *
	 * @param object the object to register
	 * @return the generated UUID
	 * @throws IllegalArgumentException if object is not a PAMELA proxy
	 */
	public String registerObject(Object object) {
		// Check if already registered
		String existingId = getObjectId(object);
		if (existingId != null) {
			return existingId;
		}

		// Get handler (must exist for PAMELA proxy)
		ProxyMethodHandler<?> handler = getHandler(object);
		if (handler == null) {
			throw new IllegalArgumentException("Object must be a PAMELA proxy: " + object.getClass().getName());
		}

		// Generate new ID and store on handler
		String newId = UUID.randomUUID().toString();
		handler.setSyncObjectId(newId);
		idToObject.put(newId, object);
		return newId;
	}

	/**
	 * Register an object with a specific ID (used when receiving remote objects).
	 * The UUID is stored on the object's ProxyMethodHandler.
	 *
	 * @param object the object to register
	 * @param objectId the specific ID to use
	 * @throws IllegalArgumentException if object is not a PAMELA proxy
	 */
	public void registerObject(Object object, String objectId) {
		ProxyMethodHandler<?> handler = getHandler(object);
		if (handler == null) {
			throw new IllegalArgumentException("Object must be a PAMELA proxy: " + object.getClass().getName());
		}

		handler.setSyncObjectId(objectId);
		idToObject.put(objectId, object);
	}

	/**
	 * Get the ID for an object.
	 * Retrieves the ID directly from the object's ProxyMethodHandler.
	 *
	 * @param object the object
	 * @return the object's ID, or null if not registered or not a PAMELA proxy
	 */
	public String getObjectId(Object object) {
		ProxyMethodHandler<?> handler = getHandler(object);
		return handler != null ? handler.getSyncObjectId() : null;
	}

	/**
	 * Get the ID for an object, registering it if necessary.
	 *
	 * @param object the object
	 * @return the object's ID
	 * @throws IllegalArgumentException if object is not a PAMELA proxy
	 */
	public String getOrCreateObjectId(Object object) {
		String id = getObjectId(object);
		if (id == null) {
			id = registerObject(object);
		} else if (!idToObject.containsKey(id)) {
			// Object already has an ID (from another manager or source) but is not
			// registered with this manager - add it to our map
			idToObject.put(id, object);
		}
		return id;
	}

	/**
	 * Get an object by its ID
	 * 
	 * @param objectId the object ID
	 * @return the object, or null if not found
	 */
	public Object getObject(String objectId) {
		return idToObject.get(objectId);
	}

	/**
	 * Check if an object is registered.
	 *
	 * @param object the object
	 * @return true if registered
	 */
	public boolean isRegistered(Object object) {
		return getObjectId(object) != null;
	}

	/**
	 * Check if an ID is registered
	 * 
	 * @param objectId the object ID
	 * @return true if registered
	 */
	public boolean hasObject(String objectId) {
		return idToObject.containsKey(objectId);
	}

	/**
	 * Unregister an object
	 * 
	 * @param object the object to unregister
	 */
	public void unregisterObject(Object object) {
		ProxyMethodHandler<?> handler = getHandler(object);
		if (handler != null) {
			String id = handler.getSyncObjectId();
			if (id != null) {
				idToObject.remove(id);
				handler.setSyncObjectId(null);
			}
		}
	}

	/**
	 * Unregister an object by ID
	 * 
	 * @param objectId the object ID
	 */
	public void unregisterById(String objectId) {
		Object object = idToObject.remove(objectId);
		if (object != null) {
			ProxyMethodHandler<?> handler = getHandler(object);
			if (handler != null) {
				handler.setSyncObjectId(null);
			}
		}
	}

	/**
	 * Clear all registrations
	 */
	public void clear() {
		for (Object object : idToObject.values()) {
			ProxyMethodHandler<?> handler = getHandler(object);
			if (handler != null) {
				handler.setSyncObjectId(null);
			}
		}
		idToObject.clear();
	}

	/**
	 * Get the number of registered objects
	 * 
	 * @return the count
	 */
	public int size() {
		return idToObject.size();
	}

	/**
	 * Get all registered objects as a map from ID to object.
	 * Returns a copy of the internal map to prevent modification.
	 * 
	 * @return a map of object ID to object
	 */
	public Map<String, Object> getAllObjects() {
		return new ConcurrentHashMap<>(idToObject);
	}
}
