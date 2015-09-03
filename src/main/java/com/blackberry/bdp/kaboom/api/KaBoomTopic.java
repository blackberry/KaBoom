/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.blackberry.bdp.kaboom.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import com.blackberry.bdp.common.versioned.Util;
import com.blackberry.bdp.common.conversion.Converter;
import java.nio.charset.Charset;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaBoomTopic {

	private static final Logger LOG = LoggerFactory.getLogger(KaBoomTopic.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");

	private final KafkaTopic kafkaTopic;
	private final List<KaBoomPartition> partitions = new ArrayList<>();
	private KaBoomClient assignedFlagPropagator;
	private KaBoomTopicConfig config;

	public KaBoomTopic(KafkaTopic kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}
	
	public KaBoomPartition getKaBoomPartition(int partitionId) {
		KaBoomPartition partition = null;
		for (KaBoomPartition p : partitions) {
			if (p.getKafkaPartition().getPartitionId() == partitionId)
				 partition = p;
		}
		return partition;
	}

	/**
	 * @param kaboomClients	 
	 * @param kafkaTopics
	 * @param curator
	 * @param zkPathTopics
	 * @param zkPathPartitionAssignments
	 * @param zkPathFlagAssignments
	 * @return
	 * @throws java.lang.Exception
	 */
	public static List<KaBoomTopic> getAll(
		 List<KaBoomClient> kaboomClients,
		 List<KafkaTopic> kafkaTopics,
		 CuratorFramework curator,
		 String zkPathTopics,
		 String zkPathPartitionAssignments,
		 String zkPathFlagAssignments) throws Exception {

		HashMap<Integer, KaBoomClient> idToKaBoomClient = new HashMap<>();
		for (KaBoomClient client : kaboomClients) {
			idToKaBoomClient.put(client.getId(), client);
		}

		HashMap<String, KafkaTopic> nameToKafkaTopics = new HashMap<>();
		for (KafkaTopic topic : kafkaTopics) {
			nameToKafkaTopics.put(topic.getName(), topic);
		}

		// The list we'll eventually be returning
		List<KaBoomTopic> kaboomTopics = new ArrayList<>();

		for (String topicName : Util.childrenInZkPath(curator, zkPathTopics)) {
			try {
				// The initial KaBoomTopic is built with a KafkaTopic

				KaBoomTopic topic = new KaBoomTopic(nameToKafkaTopics.get(topicName));

				if (topic.getKafkaTopic() == null) {
					LOG.error("the Kafka topic for {} is null", topicName);
					continue;
				}

				// Let's figure out whom is propagating ready flags for this topic
				String flagPath = String.format("%s/%s", zkPathFlagAssignments, topicName);
				if (curator.checkExists().forPath(flagPath) != null) {
					topic.assignedFlagPropagator = idToKaBoomClient.get(
						 Converter.intFromBytes(curator.getData().forPath(flagPath)));
				}

				String topicZkPath = String.format("%s/%s", zkPathTopics, topicName);				
				if (curator.getData().forPath(topicZkPath).length == 0) {
					LOG.warn("The length byte[] at {} is 0 therefore topic {} isn't configured", topicName);
					continue;
				}
				topic.config = KaBoomTopicConfig.get(KaBoomTopicConfig.class, curator, topicZkPath);
				if (topic.config == null) {
					LOG.error("the configuration for topic for {} is null", topicName);
					continue;
				}

				// With a configured KaBoom topic, we then look in Kafka for the partitions
				for (KafkaPartition kafkaPartition : nameToKafkaTopics.get(topicName).getPartitions()) {
					//The initial KaBoom Partition is built with a Kafka Partitons
					KaBoomPartition partition = new KaBoomPartition(kafkaPartition);
					partition.setTopic(topic);
					// Set the offset if there's one stored in ZK
					String offsetPath = String.format("%s/%d", topicZkPath, kafkaPartition.getPartitionId());
					if (curator.checkExists().forPath(offsetPath) != null) {
						partition.setOffset(Converter.longFromBytes(curator.getData().forPath(offsetPath)));
					}

					// Set the offset timestamp if there's one stored in ZK
					String offsetTsPath = String.format("%s/%s", offsetPath, "offset_timestamp");
					if (curator.checkExists().forPath(offsetTsPath) != null) {
						partition.setOffsetTimestamp(Converter.longFromBytes(curator.getData().forPath(offsetTsPath)));
					}

					// Set the partition assignee if there's one stored in ZK
					String partitionAssignmentPath = String.format("%s/%s-%d",
						 zkPathPartitionAssignments, topicName, kafkaPartition.getPartitionId());
					if (curator.checkExists().forPath(partitionAssignmentPath) != null) {
						// Why we stored them as  strings and not Ints as bytes is before my time
						partition.setAssignedClient(idToKaBoomClient.get(Integer.parseInt(
							 new String(curator.getData().forPath(partitionAssignmentPath), UTF8))));
					}
					topic.partitions.add(partition);
				}
				kaboomTopics.add(topic);
				LOG.debug("KaBoom topic {} found", topic.getKafkaTopic().getName());
			} catch (Exception e) {
				LOG.warn("Failed to build a KaBoomTopic for {} topic could be a deprecated or legacy topic: ", topicName, e);
			}
		}

		return kaboomTopics;
	}

	public String getName() {
		return kafkaTopic.getName();
	}

	public Long oldestPartitionOffset() throws Exception {
		Long oldestTimestamp = null;
		for (KaBoomPartition partiton : partitions) {
			if (oldestTimestamp == null || partiton.getOffsetTimestamp() < oldestTimestamp) {
				oldestTimestamp = partiton.getOffsetTimestamp();
			}
		}
		return oldestTimestamp;
	}

	public static int getTotalPartitonCount(List<KaBoomTopic> kaboomTopics) {
		int totalPartitions = 0;
		for (KaBoomTopic kaboomTopic : kaboomTopics) {
			totalPartitions += kaboomTopic.getKafkaTopic().getPartitions().size();
		}
		return totalPartitions;
	}

	/**
	 * @return the partitions
	 */
	public List<KaBoomPartition> getPartitions() {
		return partitions;
	}

	/**
	 * @return the config
	 */
	public KaBoomTopicConfig getConfig() {
		return config;
	}

	/**
	 * @return the kafkaTopic
	 */
	public KafkaTopic getKafkaTopic() {
		return kafkaTopic;
	}

	/**
	 * @return the assignedFlagPropagator
	 */
	public KaBoomClient getAssignedFlagPropagator() {
		return assignedFlagPropagator;
	}

}
