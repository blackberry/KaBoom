package com.blackberry.kaboom;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class StateUtils {
	private static final Logger LOG = LoggerFactory.getLogger(StateUtils.class);

	public static void getPartitionHosts(String kafkaSeedBrokers,
			List<String> topics, Map<String, String> partitionToHost,
			Map<String, List<String>> hostToPartition) {
		LOG.debug("Getting partition to host mappings for {}", topics);

		// Map partition to host and host to partition
		for (String seed : kafkaSeedBrokers.split(",")) {
			String seedHost = seed.split(":")[0];
			int seedPort = Integer.parseInt(seed.split(":")[1]);

			LOG.debug("Trying broker @ {}:{}", seedHost, seedPort);
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seedHost, seedPort, 100000, 64 * 1024,
						"leaderLookup");

				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();

				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						String host = part.leader().host();
						String partition = item.topic() + "-" + part.partitionId();
						LOG.debug("Got partition {} ({})", partition, host);
						partitionToHost.put(partition, host);

						List<String> parts = hostToPartition.get(host);
						if (parts == null) {
							parts = new ArrayList<String>();
							hostToPartition.put(host, parts);
						}
						parts.add(partition);
					}
				}
			} catch (Exception e) {
				LOG.error("Error getting meta data", e);
				continue;
			} finally {
				if (consumer != null)
					consumer.close();
			}

			LOG.debug("Successfully got partition to host mappings");
			return;
		}
	}

	public static void getActiveClients(CuratorFramework curator,
			Map<String, KaBoomNodeInfo> clients) throws Exception {
		// Get a list of active clients from zookeeper

		Yaml yaml = new Yaml(new Constructor(KaBoomNodeInfo.class));
		for (String client : curator.getChildren().forPath("/kaboom/clients")) {
			byte[] data = curator.getData().forPath("/kaboom/clients/" + client);
			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			KaBoomNodeInfo nodeInfo = (KaBoomNodeInfo) yaml.load(bais);
			LOG.debug("Found active client {} ({})", client, nodeInfo);
			clients.put(client, nodeInfo);
		}
	}

	public static void calculateLoad(Map<String, String> partitionToHost,
			Map<String, KaBoomNodeInfo> clients,
			Map<String, List<String>> clientToPartitions) {
		// For each node, figure out its target load, and current load.
		int totalPartitions = partitionToHost.size();
		int totalWeight = 0;
		for (Entry<String, KaBoomNodeInfo> e : clients.entrySet()) {
			totalWeight += e.getValue().getWeight();
		}
		for (Entry<String, KaBoomNodeInfo> e : clients.entrySet()) {
			String client = e.getKey();
			KaBoomNodeInfo info = e.getValue();

			info.setTargetLoad(totalPartitions
					* (1.0 * info.getWeight() / totalWeight));

			List<String> parts = clientToPartitions.get(client);
			if (parts == null) {
				info.setLoad(0);
			} else {
				info.setLoad(parts.size());
			}
		}
	}

	public static List<String> readTopicsFromZooKeeper(
			String kafkaZkConnectionString, List<String> topics) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curator = CuratorFrameworkFactory.newClient(
				kafkaZkConnectionString, retryPolicy);
		try {
			curator.start();

			for (String node : curator.getChildren().forPath("/brokers/topics")) {
				LOG.debug("Got topic: {}", node);
				topics.add(node);
			}

		} finally {
			curator.close();
		}

		return topics;
	}
}
