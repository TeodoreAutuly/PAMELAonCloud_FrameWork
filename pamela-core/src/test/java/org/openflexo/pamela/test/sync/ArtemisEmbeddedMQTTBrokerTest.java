/**
 * Copyright (c) 2013-2015, Openflexo
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

package org.openflexo.pamela.test.sync;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openflexo.pamela.sync.ArtemisEmbeddedMQTTBroker;

/**
 * Tests for {@link ArtemisEmbeddedMQTTBroker}.
 *
 * Note: This is effectively a lightweight integration test (embedded broker + real MQTT client),
 * but it runs fully in-process and doesn't require external infrastructure.
 */
public class ArtemisEmbeddedMQTTBrokerTest {

	private static final String HOST = "127.0.0.1";
	private static final int PORT = 1883;

	private String username;
	private String password;

	@Before
	public void setUp() {
		// Avoid running if another MQTT broker is already using the fixed port.
		if (isPortOpen(HOST, PORT, 100)) {
			System.out.println("SKIPPING TEST: port " + PORT + " already in use on " + HOST);
			return;
		}

		username = "user_" + UUID.randomUUID();
		password = "pass_" + UUID.randomUUID();
		ArtemisEmbeddedMQTTBroker.startEmbeddedBroker(username, password);

		boolean started = waitForPortOpen(HOST, PORT, 5000);
		assertTrue("Embedded MQTT broker should start and listen on " + HOST + ":" + PORT, started);
	}

	@After
	public void tearDown() {
		// Always attempt to stop (should be safe if it wasn't started).
		ArtemisEmbeddedMQTTBroker.stopEmbeddedBroker();

		// If the port was in use before the test (skipped), we must not assert anything here.
		// Otherwise verify the embedded broker really stopped.
		if (username != null) {
			boolean closed = waitForPortClosed(HOST, PORT, 5000);
			assertTrue("Embedded MQTT broker should stop and release " + HOST + ":" + PORT, closed);
		}
	}

	@Test
	public void testConnectWithValidCredentials() throws Exception {
		if (username == null) {
			// setUp() skipped because port is already in use
			return;
		}

		MqttClient client = newClient();
		try {
			MqttConnectOptions options = new MqttConnectOptions();
			options.setCleanSession(true);
			options.setAutomaticReconnect(false);
			options.setConnectionTimeout(3);
			options.setKeepAliveInterval(5);
			options.setUserName(username);
			options.setPassword(password.toCharArray());

			client.connect(options);
			assertTrue("Client should be connected", client.isConnected());
		} finally {
			disconnectQuietly(client);
		}
	}

	@Test
	public void testRejectConnectWithoutCredentials() throws Exception {
		if (username == null) {
			return;
		}

		MqttClient client = newClient();
		try {
			MqttConnectOptions options = new MqttConnectOptions();
			options.setCleanSession(true);
			options.setAutomaticReconnect(false);
			options.setConnectionTimeout(3);

			try {
				client.connect(options);
				fail("Expected broker to reject connection without credentials (allowAnonymous=false)");
			} catch (MqttException e) {
				// authentication should fail
				assertFalse("Client should not be connected", client.isConnected());
			}
		} finally {
			disconnectQuietly(client);
		}
	}

	@Test
	public void testRejectConnectWithWrongPassword() throws Exception {
		if (username == null) {
			return;
		}

		MqttClient client = newClient();
		try {
			MqttConnectOptions options = new MqttConnectOptions();
			options.setCleanSession(true);
			options.setAutomaticReconnect(false);
			options.setConnectionTimeout(3);
			options.setUserName(username);
			options.setPassword(("wrong-" + password).toCharArray());

			try {
				client.connect(options);
				fail("Expected broker to reject connection with invalid credentials");
			} catch (MqttException e) {
				// Expected
				assertFalse("Client should not be connected", client.isConnected());
			}
		} finally {
			disconnectQuietly(client);
		}
	}

	private static MqttClient newClient() throws MqttException {
		String serverUri = "tcp://" + HOST + ":" + PORT;
		String clientId = "pamela-test-" + UUID.randomUUID();
		return new MqttClient(serverUri, clientId, new MemoryPersistence());
	}

	private static void disconnectQuietly(MqttClient client) {
		if (client == null) {
			return;
		}
		try {
			if (client.isConnected()) {
				client.disconnectForcibly(500, 1000, true);
			}
		} catch (Exception e) {
			// ignore
		}
		try {
			client.close();
		} catch (Exception e) {
			// ignore
		}
	}

	private static boolean waitForPortOpen(String host, int port, long timeoutMs) {
		long end = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < end) {
			if (isPortOpen(host, port, 150)) {
				return true;
			}
			sleep(50);
		}
		return false;
	}

	private static boolean waitForPortClosed(String host, int port, long timeoutMs) {
		long end = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < end) {
			if (!isPortOpen(host, port, 150)) {
				return true;
			}
			sleep(50);
		}
		return false;
	}

	private static boolean isPortOpen(String host, int port, int connectTimeoutMs) {
		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	private static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}

