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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.config.impl.RoleSet;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;

/**
 * Class that is responsible of managing the embedded MQTT broker
 * for PAMELA collaborative synchronization.
 * 
 * This class creates an Artemis broker in a separate process (inside the JVM).
 * 
 * @author PAMELA Sync
 */
public class ArtemisEmbeddedMQTTBroker {

    private static final Logger logger = Logger.getLogger(ArtemisEmbeddedMQTTBroker.class.getName());

    private static EmbeddedActiveMQ embeddedBroker;
    private static boolean started = false;
    
    /**
	 * Starts the broker.
	 * 
	 * @param username the username to allow connections to the broker.
	 * @param password the password to allow connections to the broker.
	 */
    public static void startEmbeddedBroker(String username, String password) {
        try {
            embeddedBroker = new EmbeddedActiveMQ();

            ConfigurationImpl config = new ConfigurationImpl();
            config.setSecurityEnabled(true);
            config.setPersistenceEnabled(false);
            
            // Here you can setup the IP Adress where the broker will be accessible.
            // By default it is localhost.
            config.addAcceptorConfiguration(
                "mqtt",
                "tcp://127.0.0.1:1883?protocols=MQTT;allowAnonymous=false"
            );

            // ============================
            // ROLE PERMISSIONS
            // ============================
            Role mqttRole = new Role(
                "mqtt",
                true,  // send
                true,  // consume
                true,  // createDurableQueue
                true,  // deleteDurableQueue
                true,  // createNonDurableQueue
                true,  // deleteNonDurableQueue
                true,  // manage
                true,  // browse
                true,  // createAddress
                true   // deleteAddress
            );

            RoleSet roleSet = new RoleSet();
            roleSet.add(mqttRole);
            config.addSecurityRole("#", roleSet);

            // ============================
            // USERS (IN-MEMORY)
            // ============================
            SecurityConfiguration securityConfig = new SecurityConfiguration();

            securityConfig.addUser(username, password);
            securityConfig.addRole(username, "mqtt");

            ActiveMQSecurityManagerImpl securityManager =
                new ActiveMQSecurityManagerImpl(securityConfig);

            embeddedBroker.setConfiguration(config);
            embeddedBroker.setSecurityManager(securityManager);
            embeddedBroker.start();

            started = true;
            logger.info("Broker started");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to start broker", e);
        }
    }
    
    /**
	 * Stops the broker.
	 */
    public static void stopEmbeddedBroker() {
        if (embeddedBroker != null && started) {
            try {
                embeddedBroker.stop();
                logger.info("Broker stopped");
                started = false;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to stop broker", e);
            }
        }
    }
}
