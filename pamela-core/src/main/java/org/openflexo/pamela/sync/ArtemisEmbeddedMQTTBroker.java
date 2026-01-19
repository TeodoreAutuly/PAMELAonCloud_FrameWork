package org.openflexo.pamela.sync;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

public class ArtemisEmbeddedMQTTBroker {
	private static final Logger logger = Logger.getLogger(ArtemisEmbeddedMQTTBroker.class.getName());
	
    private static EmbeddedActiveMQ embeddedBroker;
    private static boolean started = false;
	
    public static void startEmbeddedBroker(String username, String password) {
    	try {
    		embeddedBroker = new EmbeddedActiveMQ();
    		
    		URL configUrl = ArtemisEmbeddedMQTTBroker.class
                    .getClassLoader()
                    .getResource("broker.xml");

            if (configUrl == null) {
                throw new RuntimeException("broker.xml not found in classpath");
            }
            
            // Artemis embedded broker wants a file path, not a jar URL
            InputStream in = ArtemisEmbeddedMQTTBroker.class.getClassLoader()
                    .getResourceAsStream("broker.xml");
            if (in == null) throw new RuntimeException("broker.xml not found in resources");

            File tempFile = File.createTempFile("broker-", ".xml");
            tempFile.deleteOnExit();

            Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            embeddedBroker.setConfigResourcePath("file:///" + tempFile.getAbsolutePath().replace("\\", "/"));
            //embeddedBroker.setConfigResourcePath(configUrl.toString());

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
