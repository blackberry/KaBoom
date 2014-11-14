/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blackberry.kaboom;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetClientInfo {
	private static final Logger LOG = LoggerFactory
			.getLogger(GetClientInfo.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");

	public static void main(String[] args) throws Exception {
		new GetClientInfo().run();
	}

	public void run() throws Exception {
		Map<String, String> partitionToHost = new HashMap<String, String>();
		Map<String, List<String>> hostToPartition = new HashMap<String, List<String>>();
		final Map<String, KaBoomNodeInfo> clients = new HashMap<String, KaBoomNodeInfo>();
		Map<String, List<String>> clientToPartitions = new HashMap<String, List<String>>();
		Map<String, String> partitionToClient = new HashMap<String, String>();
		List<String> topics = new ArrayList<String>();

		// Load configs from kaboom.properties
		Properties props = new Properties();
		try {
			InputStream propsIn = null;
			if (System.getProperty("kaboom.configuration") != null) {
				propsIn = new FileInputStream(
						System.getProperty("kaboom.configuration"));
				props.load(propsIn);
			} else {
				LOG.info("Loading configs from {}", KaBoom.class.getClassLoader()
						.getResource("kaboom.properties"));
				propsIn = KaBoom.class.getClassLoader().getResourceAsStream(
						"kaboom.properties");
				props.load(propsIn);
			}
		} catch (Throwable t) {
			System.err.println("Error getting config file.");
			t.printStackTrace();
			System.exit(1);
		}

		String kafkaZkConnectionString = props
				.getProperty("kafka.zookeeper.connection.string");
		String kafkaSeedBrokers = props.getProperty("metadata.broker.list");

		// Connect to ZooKeeper
		String zookeeperConnectionString = null;
		if (props.containsKey("zookeeper.connection.string")) {
			zookeeperConnectionString = props
					.getProperty("zookeeper.connection.string");
		} else {
			LOG.error("Missing required property: zookeeper.connection.string");
			return;
		}
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		String[] connStringAndPrefix = zookeeperConnectionString.split("/", 2);
		final CuratorFramework curator;
		if (connStringAndPrefix.length == 1) {
			curator = CuratorFrameworkFactory.newClient(zookeeperConnectionString,
					retryPolicy);
		} else {
			curator = CuratorFrameworkFactory.builder()
					.namespace(connStringAndPrefix[1])
					.connectString(connStringAndPrefix[0]).retryPolicy(retryPolicy)
					.build();
		}

		curator.start();

		// Get a full set of metadata from Kafka
		StateUtils.readTopicsFromZooKeeper(kafkaZkConnectionString, topics);

		// Map partition to host and host to partition
		StateUtils.getPartitionHosts(kafkaSeedBrokers, topics, partitionToHost,
				hostToPartition);

		// Get a list of active clients from zookeeper
		StateUtils.getActiveClients(curator, clients);

		// Get a list of current assignments
		for (String partition : partitionToHost.keySet()) {
			Stat stat = curator.checkExists().forPath(
					"/kaboom/assignments/" + partition);
			if (stat != null) {
				// check if the client is still connected, and delete node if it is
				// not.
				String client = new String(curator.getData().forPath(
						"/kaboom/assignments/" + partition), UTF8);

				if (clients.get(client) == null) {
					LOG.debug("Partition {} : client {} is not connected", partition,
							client);
					clients.put(client, new KaBoomNodeInfo());
				} else {
					LOG.debug("Partition {} : client {} is connected", partition, client);
				}

				List<String> parts = clientToPartitions.get(client);
				if (parts == null) {
					parts = new ArrayList<String>();
					clientToPartitions.put(client, parts);
				}
				parts.add(partition);

				partitionToClient.put(partition, client);
			}
		}

		// For each node, figure out its target load, and current load.
		StateUtils.calculateLoad(partitionToHost, clients, clientToPartitions);

		// Get a full set of metadata from Kafka

		// For each node, figure out its target load, current load, count of local
		// and remote. And report it.
		System.out.println(String.format(
				"%-10s | %-6s | %-32s | %6s | %11s | %10s | %11s | %10s", "Client ID",
				"Status", "Hostname", "Weight", "Target Load", "Local Load",
				"Remote Load", "Total Load"));
		{
			int totalPartitions = partitionToHost.size();
			int totalWeight = 0;
			for (Entry<String, KaBoomNodeInfo> e : clients.entrySet()) {
				if (e.getValue() != null) {
					totalWeight += e.getValue().getWeight();
				}
			}

			List<String> clientList = new ArrayList<String>();
			clientList.addAll(clients.keySet());
			Collections.sort(clientList);
			for (String client : clientList) {
				KaBoomNodeInfo info = clients.get(client);

				if (info != null) {
					info.setTargetLoad(totalPartitions
							* (1.0 * info.getWeight() / totalWeight));
				}

				int localLoad = 0;
				int remoteLoad = 0;
				int totalLoad = 0;
				List<String> parts = clientToPartitions.get(client);
				if (parts != null) {
					for (String part : parts) {
						if (clients.get(client) != null
								&& partitionToHost.get(part).equals(
										clients.get(client).getHostname())) {
							localLoad++;
						} else {
							remoteLoad++;
						}
						totalLoad++;
					}
				}

				System.out.println(String.format(
						"%-10s | %-6s | %-32s | %6d | %11.2f | %10d | %11d | %10d", client,
						info.getHostname() == null ? "OFF" : "OK",
						info.getHostname() == null ? "" : info.getHostname(),
						info.getWeight(), info.getTargetLoad(), localLoad, remoteLoad,
						totalLoad));
			}
		}

	}
}
