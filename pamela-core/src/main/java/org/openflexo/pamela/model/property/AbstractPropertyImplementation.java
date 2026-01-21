package org.openflexo.pamela.model.property;

import java.beans.PropertyChangeSupport;
import java.util.logging.Logger;

import org.openflexo.pamela.factory.IProxyMethodHandler;
import org.openflexo.pamela.factory.PamelaModelFactory;
import org.openflexo.pamela.factory.PamelaModelFactory.PAMELAProxyFactory;
import org.openflexo.pamela.factory.ProxyMethodHandler;
import org.openflexo.pamela.model.ModelEntity;
import org.openflexo.pamela.model.ModelProperty;
import org.openflexo.toolbox.HasPropertyChangeSupport;

/**
 * Base abstract class providing property implementation
 * 
 * 
 * @author sylvain
 *
 * @param <I>
 *            type of entity defining such property
 * @param <T>
 *            accessed type for the property
 * @param <M>
 *            internal memory adressable for a given entity instance and property
 */
public abstract class AbstractPropertyImplementation<I, T> implements PropertyImplementation<I, T> {

	private static final Logger logger = Logger.getLogger(AbstractPropertyImplementation.class.getName());

	private final ProxyMethodHandler<I> handler;
	private final ModelProperty<I> property;

	public AbstractPropertyImplementation(ProxyMethodHandler<I> handler, ModelProperty<I> property) {
		this.handler = handler;
		this.property = property;
	}

	protected ProxyMethodHandler<I> getHandler() {
		return handler;
	}

	@Override
	public I getObject() {
		return getHandler().getObject();
	}

	public PamelaModelFactory getModelFactory() {
		return getHandler().getModelFactory();
	}

	final public ModelEntity<I> getModelEntity() {
		return getHandler().getModelEntity();
	}

	public PAMELAProxyFactory<I> getPamelaProxyFactory() {
		return getHandler().getPamelaProxyFactory();
	}

	@Override
	public ModelProperty<I> getProperty() {
		return property;
	}

	protected void firePropertyChange(String propertyIdentifier, Object oldValue, Object value) {
		// Debug logging only for shapes property (sync debugging)
		boolean debugThis = "shapes".equals(propertyIdentifier);
		if (debugThis) {
			logger.info("firePropertyChange called: property=" + propertyIdentifier + ", object=" + getObject() +
					", isHasPCS=" + (getObject() instanceof HasPropertyChangeSupport) +
					", isDeleting=" + getHandler().isDeleting());
		}
		if (getObject() instanceof HasPropertyChangeSupport && !getHandler().isDeleting()) {
			PropertyChangeSupport propertyChangeSupport = ((HasPropertyChangeSupport) getObject()).getPropertyChangeSupport();
			if (propertyChangeSupport != null) {
				if (debugThis) {
					int listenerCount = propertyChangeSupport.getPropertyChangeListeners().length;
					logger.info("Firing property change for " + propertyIdentifier + " on " + getObject().getClass().getSimpleName() +
							", listeners=" + listenerCount);
				}
				propertyChangeSupport.firePropertyChange(propertyIdentifier, oldValue, value);
			} else if (debugThis) {
				logger.warning("PropertyChangeSupport is NULL for " + getObject());
			}
		} else if (debugThis) {
			logger.warning("NOT firing property change: isHasPCS=" + (getObject() instanceof HasPropertyChangeSupport) +
					", isDeleting=" + getHandler().isDeleting());
		}
	}

	protected static boolean isEqual(Object oldValue, Object newValue) {
		return IProxyMethodHandler.isEqual(oldValue, newValue);

	}

}
