/**
 * Copyright (c) 2024, Openflexo
 *
 * This file is part of Pamela-core, a component of the software infrastructure
 * developed at Openflexo.
 *
 * Openflexo is dual-licensed under the European Union Public License (EUPL, either
 * version 1.1 of the License, or any later version), which is available at
 * https://joinup.ec.europa.eu/software/page/eupl/licence-eupl
 * and the GNU General Public License (GPL, either version 3 of the License, or any
 * later version), which is available at http://www.gnu.org/licenses/gpl.html.
 */

package org.openflexo.pamela.sync;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Serializer for property values in sync operations.
 * Handles conversion of PAMELA property values to/from string representation
 * for transmission over RabbitMQ.
 *
 * Supports custom type serializers for application-specific types like
 * geometry objects, colors, etc.
 *
 * @author PAMELA Team
 */
public class SyncValueSerializer {

	private static final Logger logger = Logger.getLogger(SyncValueSerializer.class.getName());

	// Special markers for null and object references
	private static final String NULL_MARKER = "__NULL__";
	private static final String OBJECT_REF_PREFIX = "__REF__:";

	/**
	 * Interface for custom type serializers.
	 * Allows applications to register serializers for their specific types.
	 *
	 * @param <T> the type this serializer handles
	 */
	public interface CustomTypeSerializer<T> {
		/**
		 * Get the prefix used in serialized strings to identify this type.
		 * Should be unique and uppercase (e.g., "POINT:", "COLOR:").
		 */
		String getPrefix();

		/**
		 * Get the class this serializer handles.
		 */
		Class<T> getType();

		/**
		 * Serialize a value to string representation.
		 * The prefix will be automatically added.
		 *
		 * @param value the value to serialize (never null)
		 * @return string representation without prefix
		 */
		String serialize(T value);

		/**
		 * Deserialize a string representation to a value.
		 * The prefix will be already stripped.
		 *
		 * @param serialized the serialized string without prefix
		 * @return the deserialized value
		 */
		T deserialize(String serialized);
	}

	// Registry of custom type serializers by class
	private final Map<Class<?>, CustomTypeSerializer<?>> serializersByClass = new ConcurrentHashMap<>();

	// Registry of custom type serializers by prefix
	private final Map<String, CustomTypeSerializer<?>> serializersByPrefix = new ConcurrentHashMap<>();

	/**
	 * Register a custom type serializer.
	 *
	 * @param serializer the custom type serializer to register
	 * @param <T> the type the serializer handles
	 */
	public <T> void registerCustomSerializer(CustomTypeSerializer<T> serializer) {
		if (serializer == null || serializer.getType() == null || serializer.getPrefix() == null) {
			throw new IllegalArgumentException("Serializer, type, and prefix must not be null");
		}
		serializersByClass.put(serializer.getType(), serializer);
		serializersByPrefix.put(serializer.getPrefix(), serializer);
		logger.fine("Registered custom serializer for type: " + serializer.getType().getName());
	}

	/**
	 * Unregister a custom type serializer.
	 *
	 * @param type the type to unregister
	 */
	public void unregisterCustomSerializer(Class<?> type) {
		CustomTypeSerializer<?> removed = serializersByClass.remove(type);
		if (removed != null) {
			serializersByPrefix.remove(removed.getPrefix());
		}
	}

	/**
	 * Check if a custom serializer is registered for the given type.
	 *
	 * @param type the type to check
	 * @return true if a serializer is registered
	 */
	public boolean hasCustomSerializer(Class<?> type) {
		return serializersByClass.containsKey(type);
	}

	/**
	 * Serialize a value to string representation
	 *
	 * @param value the value to serialize
	 * @return string representation
	 */
	@SuppressWarnings("unchecked")
	public String serialize(Object value) {
		if (value == null) {
			return NULL_MARKER;
		}

		// Check for custom type serializer first
		CustomTypeSerializer<Object> customSerializer = (CustomTypeSerializer<Object>) findSerializerForClass(value.getClass());
		if (customSerializer != null) {
			return customSerializer.getPrefix() + customSerializer.serialize(value);
		}

		// Handle primitive types and common types
		if (value instanceof String) {
			return (String) value;
		}

		if (value instanceof Number || value instanceof Boolean) {
			return value.toString();
		}

		if (value instanceof Enum) {
			return ((Enum<?>) value).name();
		}

		// Handle PAMELA objects by reference
		// This requires the object to be registered in the identity manager
		// For now, we'll use toString() and hope for the best
		// A more robust solution would check if it's a proxy object

		return value.toString();
	}

	/**
	 * Find a serializer for the given class, checking superclasses and interfaces.
	 */
	private CustomTypeSerializer<?> findSerializerForClass(Class<?> clazz) {
		// Direct match
		CustomTypeSerializer<?> serializer = serializersByClass.get(clazz);
		if (serializer != null) {
			return serializer;
		}

		// Check superclass
		Class<?> superclass = clazz.getSuperclass();
		if (superclass != null && superclass != Object.class) {
			serializer = findSerializerForClass(superclass);
			if (serializer != null) {
				return serializer;
			}
		}

		// Check interfaces
		for (Class<?> iface : clazz.getInterfaces()) {
			serializer = serializersByClass.get(iface);
			if (serializer != null) {
				return serializer;
			}
		}

		return null;
	}

	/**
	 * Serialize a PAMELA object reference
	 * 
	 * @param object the PAMELA object
	 * @param identityManager the identity manager to get the object ID
	 * @return reference string
	 */
	public String serializeReference(Object object, ObjectIdentityManager identityManager) {
		if (object == null) {
			return NULL_MARKER;
		}

		String objectId = identityManager.getObjectId(object);
		if (objectId != null) {
			return OBJECT_REF_PREFIX + objectId;
		}

		// Not a registered object, serialize as string
		return serialize(object);
	}

	/**
	 * Deserialize a string representation to a value
	 *
	 * @param serialized the serialized string
	 * @param targetType the expected type
	 * @param syncContext the sync context for resolving object references
	 * @return the deserialized value
	 */
	public Object deserialize(String serialized, Class<?> targetType, SyncEditingContext syncContext) {
		if (serialized == null || NULL_MARKER.equals(serialized)) {
			return null;
		}

		// Handle object references
		if (serialized.startsWith(OBJECT_REF_PREFIX)) {
			String objectId = serialized.substring(OBJECT_REF_PREFIX.length());
			return syncContext.getIdentityManager().getObject(objectId);
		}

		// Check for custom type serializer by prefix
		for (Map.Entry<String, CustomTypeSerializer<?>> entry : serializersByPrefix.entrySet()) {
			if (serialized.startsWith(entry.getKey())) {
				String value = serialized.substring(entry.getKey().length());
				return entry.getValue().deserialize(value);
			}
		}

		// Check for custom type serializer by target type
		CustomTypeSerializer<?> customSerializer = findSerializerForClass(targetType);
		if (customSerializer != null) {
			// The serialized string might not have a prefix if it was serialized externally
			// Try to deserialize directly if it doesn't match another format
			try {
				return customSerializer.deserialize(serialized);
			} catch (Exception e) {
				// Fall through to default handling
				logger.fine("Custom serializer failed for: " + serialized + ", falling back to default");
			}
		}

		try {
			// Handle primitive types
			if (targetType == String.class) {
				return serialized;
			}

			if (targetType == int.class || targetType == Integer.class) {
				return Integer.parseInt(serialized);
			}

			if (targetType == long.class || targetType == Long.class) {
				return Long.parseLong(serialized);
			}

			if (targetType == double.class || targetType == Double.class) {
				return Double.parseDouble(serialized);
			}

			if (targetType == float.class || targetType == Float.class) {
				return Float.parseFloat(serialized);
			}

			if (targetType == boolean.class || targetType == Boolean.class) {
				return Boolean.parseBoolean(serialized);
			}

			if (targetType == byte.class || targetType == Byte.class) {
				return Byte.parseByte(serialized);
			}

			if (targetType == short.class || targetType == Short.class) {
				return Short.parseShort(serialized);
			}

			if (targetType == char.class || targetType == Character.class) {
				return serialized.isEmpty() ? '\0' : serialized.charAt(0);
			}

			// Handle enums
			if (targetType.isEnum()) {
				@SuppressWarnings({"unchecked", "rawtypes"})
				Object enumValue = Enum.valueOf((Class<Enum>) targetType, serialized);
				return enumValue;
			}

			// For other types, return the string and let PAMELA's converters handle it
			return serialized;

		} catch (Exception e) {
			logger.log(Level.WARNING, "Failed to deserialize value: " + serialized + " to type " + targetType, e);
			return serialized;
		}
	}

	/**
	 * Check if a serialized value is an object reference
	 * 
	 * @param serialized the serialized string
	 * @return true if it's an object reference
	 */
	public boolean isObjectReference(String serialized) {
		return serialized != null && serialized.startsWith(OBJECT_REF_PREFIX);
	}

	/**
	 * Extract the object ID from a reference string
	 * 
	 * @param serialized the serialized reference
	 * @return the object ID, or null if not a reference
	 */
	public String extractObjectId(String serialized) {
		if (isObjectReference(serialized)) {
			return serialized.substring(OBJECT_REF_PREFIX.length());
		}
		return null;
	}
}
