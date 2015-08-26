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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaBoomPartition {

	@JsonIgnore private KaBoomTopic topic;
	private KafkaPartition kafkaPartition;
	private KaBoomClient assignedClient;
	private long offset;
	private long offsetTimestamp;
	private static final Logger LOG = LoggerFactory.getLogger(KaBoomPartition.class);

	public KaBoomPartition(KafkaPartition kafkaPartition) {
		this.kafkaPartition = kafkaPartition;
	}

	public static List<KaBoomPartition> unassignedPartitions(List<KaBoomTopic> kaboomTopics) {
		LOG.info("There are {} configured KaBoom topics", kaboomTopics.size());
		List<KaBoomPartition> unassignedPartitions = new ArrayList<>();
		// For all KaBoomTopic's		
		for (KaBoomTopic kaboomTopic : kaboomTopics) {
			// For all the KaBoomPartition's
			LOG.debug("KaBoom topic {} has {} partitons", kaboomTopic.getKafkaTopic().getName(),
				 kaboomTopic.getPartitions().size());
			for (KaBoomPartition kaboomPartition : kaboomTopic.getPartitions()) {
				// Add the corresponding KafkaPartition if not assigned
				if (kaboomPartition.assignedClient != null) {
					LOG.debug("Topic {} partition {} is assigned to {}",
						 kaboomTopic.getKafkaTopic().getName(),
						 kaboomPartition.getKafkaPartition().getPartitionId(),
						 kaboomPartition.getAssignedClient().getId());
				} else {
					unassignedPartitions.add(kaboomPartition);
					LOG.info("Topic {} partition {} is not assigned",
						 kaboomTopic.getKafkaTopic().getName(),
						 kaboomPartition.getKafkaPartition().getPartitionId());
				}
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
