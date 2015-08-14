/*
 * Copyright 2015 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.blackberry.bdp.kaboom.api;

import java.util.ArrayList;
import java.util.List;

public class KaBoomPartition {

	private KaBoomTopic topic;
	private KafkaPartition kafkaPartition;
	private KaBoomClient  assignedClient;
	private long offset;
	private long offsetTimestamp;	

	public KaBoomPartition(KafkaPartition kafkaPartition) {
		this.kafkaPartition = kafkaPartition;			 
	}
	
	public static List<KaBoomPartition> unassignedPartitions(List<KaBoomTopic> kaboomTopics) {
		// Build a list of all the KafkaPartiton's we have assigned already
		List<KafkaPartition> assignedPartitions = new ArrayList<>();
		// For all KaBoomTopic's
		for (KaBoomTopic kaboomTopic : kaboomTopics) {
			// For all the KaBoomPartition's
			for (KaBoomPartition kaboomPartition :  kaboomTopic.getPartitions()) {
				// Add the corresponding KafkaPartition
				assignedPartitions.add(kaboomPartition.getKafkaPartition());
			}
		}
		
		// Now build a list of KaBoomPartition that are not already assigned
		List<KaBoomPartition> unassignedPartitions = new ArrayList<>();		
		// For every KaBoomTopic
		for (KaBoomTopic kaboomTopic : kaboomTopics) {
			// For every KafkaPartition (note: we switch to iteating from the KafkaTopic)
			for (KafkaPartition kafkaPartition : kaboomTopic.getKafkaTopic().getPartitions()) {
				// That isn't already assigned
				if (assignedPartitions.contains(kafkaPartition)) continue;
				KaBoomPartition unassignedPartition = new KaBoomPartition(kafkaPartition);
				unassignedPartition.setTopic(kaboomTopic);				
			}
		}
		return unassignedPartitions;
	}

	public String getTopicPartitionString() {
		return String.format("%s-%s", topic.getKafkaTopic().getName(), kafkaPartition.getPartitionId());
	}
	
	/**
	 * @return the kafkaPartition
	 */
	public KafkaPartition getKafkaPartition() {
		return kafkaPartition;
	}

	/**
	 * @param kafkaPartition the kafkaPartition to set
	 */
	public void setKafkaPartition(KafkaPartition kafkaPartition) {
		this.kafkaPartition = kafkaPartition;
	}

	/**
	 * @return the assignedClient
	 */
	public KaBoomClient getAssignedClient() {
		return assignedClient;
	}

	/**
	 * @param assignedClient the assignedClient to set
	 */
	public void setAssignedClient(KaBoomClient assignedClient) {
		this.assignedClient = assignedClient;
	}

	/**
	 * @return the offset
	 */
	public long getOffset() {
		return offset;
	}

	/**
	 * @param offset the offset to set
	 */
	public void setOffset(long offset) {
		this.offset = offset;
	}

	/**
	 * @return the offsetTimestamp
	 */
	public long getOffsetTimestamp() {
		return offsetTimestamp;
	}

	/**
	 * @param offsetTimestamp
	 */
	public void setOffsetTimestamp(long offsetTimestamp) {
		this.offsetTimestamp = offsetTimestamp;
	}

	/**
	 * @return the topic
	 */
	public KaBoomTopic getTopic() {
		return topic;
	}

	/**
	 * @param kaboomTopic the topic to set
	 */
	public void setTopic(KaBoomTopic kaboomTopic) {
		this.topic = kaboomTopic;
	}
}
