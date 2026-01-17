package org.openflexo.pamela.sync;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.config.impl.RoleSet;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;

public class ArtemisEmbeddedMQTTBroker {

    private static final Logger logger = Logger.getLogger(ArtemisEmbeddedMQTTBroker.class.getName());

    private static EmbeddedActiveMQ embeddedBroker;
    private static boolean started = false;

    public static void startEmbeddedBroker(String username, String password) {
        try {
            embeddedBroker = new EmbeddedActiveMQ();
            logger.info(password + username);

            ConfigurationImpl config = new ConfigurationImpl();
            config.setSecurityEnabled(true);
            config.setPersistenceEnabled(false);
            config.addAcceptorConfiguration("mqtt", "tcp://127.0.0.1:1883?protocols=MQTT;allowAnonymous=false");

            // JAAS IN-MEMORY CONFIG
            javax.security.auth.login.Configuration.setConfiguration(
                    new InMemoryJaasConfig(username, password, "mqtt")
            );

            ActiveMQJAASSecurityManager securityManager =
                    new ActiveMQJAASSecurityManager("artemis");

            // ROLES
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

            embeddedBroker.setConfiguration(config);
            embeddedBroker.setSecurityManager(securityManager);
            embeddedBroker.createActiveMQServer();
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

    // In-memory JAAS configuration (NO FILES)
    static class InMemoryJaasConfig extends Configuration {

        private final String username;
        private final String password;
        private final String role;

        public InMemoryJaasConfig(String username, String password, String role) {
            this.username = username;
            this.password = password;
            this.role = role;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            System.out.println("JAAS domain requested: " + name);

            Map<String, Object> options = new HashMap<>();
            options.put("users", username + "=" + password);
            options.put("roles", username + "=" + role);
            options.put("reload", "true");
            options.put("debug", "true");

            return new AppConfigurationEntry[]{
                new AppConfigurationEntry(
                    "org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule",
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    options
                )
            };
        }

    }
}
