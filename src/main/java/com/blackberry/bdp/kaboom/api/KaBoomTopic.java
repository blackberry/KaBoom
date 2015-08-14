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
	private KaBoomClient  assignedFlagPropagator;
	private KaBoomTopicConfig config;

	public KaBoomTopic(KafkaTopic kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
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
		for (KaBoomClient client : kaboomClients)  idToKaBoomClient.put(client.getId(), client); 
		
		HashMap<String, KafkaTopic> nameToKafkaTopics = new HashMap<>();
		
		//HashMap<String, KafkaPartition> partitionIdToKafkaPartiton = new HashMap<>();		
		
		for (KafkaTopic topic : kafkaTopics) {
			nameToKafkaTopics.put(topic.getName(), topic);
			//for (KafkaPartition kafkaPartition : topic.getPartitions()) {
			//	String partitionId = String.format("%s-%d", topic.getName(), kafkaPartition.getPartitionId());
			//	partitionIdToKafkaPartiton.put(partitionId, kafkaPartition);
			//}
		}
				
		// The list we'll eventually be returning
		List<KaBoomTopic> kaboomTopics = new ArrayList<>();

		for (String topicName : Util.childrenInZkPath(curator, zkPathTopics)) {
			// The initla KaBoomTopic is built from a KafkaTopic
			KaBoomTopic topic = new KaBoomTopic(nameToKafkaTopics.get(topicName));
			kaboomTopics.add(topic);
			
			// Let's figure out whom is propagating ready flags for this topic
			String flagPath = String.format("%s/%s", zkPathFlagAssignments, topicName);
			if (curator.checkExists().forPath(flagPath) != null) {
				topic.assignedFlagPropagator = idToKaBoomClient.get(
					 Converter.intFromBytes(curator.getData().forPath(flagPath)));
			}
			
			// Now we create all the partitions
			String topicZkPath = String.format("%s/%s", zkPathTopics, topicName);
			for (String partitionId : Util.childrenInZkPath(curator, topicZkPath)) {				
				
				KaBoomPartition partition = new KaBoomPartition(
					 nameToKafkaTopics.get(topicName).getPartition(Integer.parseInt(partitionId)));
				
				partition.setTopic(topic);
				
				// Set the offset if there's one stored in ZK
				String offsetPath = String.format("%s/%s", topicZkPath, partitionId);								
				if (curator.checkExists().forPath(offsetPath) != null) {
					partition.setOffset(Converter.longFromBytes(curator.getData().forPath(offsetPath)));
				}				
				
				// Set the offset timestamp if there's one stored in ZK
				String offsetTsPath = String.format("%s/%s", topicZkPath, "/offset_timestamp");
				if (curator.checkExists().forPath(offsetTsPath) != null) {
					partition.setOffsetTimestamp(Converter.longFromBytes(curator.getData().forPath(offsetTsPath)));
				}
				
				// Set the partition assignee if there's one stored in ZK
				String partitionAssignmentPath = String.format("%s/%s-%s", 
					 zkPathPartitionAssignments, topicName, partitionId);
				if (curator.checkExists().forPath(partitionAssignmentPath) != null) {
					// Why we stored the them as  strings and not Ints as bytes is before my time
					partition.setAssignedClient(idToKaBoomClient.get(Integer.parseInt(
						 new String(curator.getData().forPath(partitionAssignmentPath), UTF8))));
				}
				
				topic.partitions.add(partition);
				topic.config = KaBoomTopicConfig.get(KaBoomTopicConfig.class, curator, topicZkPath);
			}
		}
		return kaboomTopics;
	}

	public long oldestPartitionOffset() throws Exception {
		long oldestTimestamp = -1;
		for (KaBoomPartition partiton : partitions) {
			if (partiton.getOffsetTimestamp()< oldestTimestamp || oldestTimestamp == -1) {
				oldestTimestamp = partiton.getOffsetTimestamp();
			}
		}
		if (oldestTimestamp == -1) {
			throw new Exception("Failed to get oldest partition timestamp for topic: " + kafkaTopic.getName());
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
