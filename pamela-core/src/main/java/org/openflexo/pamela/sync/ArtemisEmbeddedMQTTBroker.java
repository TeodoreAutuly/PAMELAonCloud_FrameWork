package org.openflexo.pamela.sync;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.config.impl.RoleSet;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;

public class ArtemisEmbeddedMQTTBroker {

    private static final Logger logger = Logger.getLogger(ArtemisEmbeddedMQTTBroker.class.getName());

    private static EmbeddedActiveMQ embeddedBroker;
    private static boolean started = false;

    public static void startEmbeddedBroker(String username, String password) {
        try {
            embeddedBroker = new EmbeddedActiveMQ();

            ConfigurationImpl config = new ConfigurationImpl();
            config.setSecurityEnabled(true);
            config.setPersistenceEnabled(false);

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
