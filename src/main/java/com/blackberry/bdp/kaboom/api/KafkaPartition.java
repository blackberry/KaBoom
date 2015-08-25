/*
 * Copyright 2015 dariens.
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
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;

public class KafkaPartition {

	@JsonIgnore private KafkaTopic topic;
	private final int partitionId;
	private final int leaderBrokerId;
	private KafkaBroker leader;
	private final short errorCode;
	private final int sizeInBytes;	
	private final List<Integer> replicas = new ArrayList<>();
	private final List<Integer> inSyncReplicas = new ArrayList<>();	
	private Long earliestOffset = null;
	private Long latestOffset = null;

	public KafkaPartition(PartitionMetadata metadata) {		
		this.partitionId = metadata.partitionId();
		this.leaderBrokerId = metadata.leader().id();
		this.errorCode = metadata.errorCode();
		this.sizeInBytes = metadata.sizeInBytes();
		
		for (Broker b : metadata.replicas()) {
			replicas.add(b.id());
		}
		
		for (Broker b : metadata.isr()) {
			inSyncReplicas.add(b.id());
		}
	}

	public String getTopicPartitionString() {
		return String.format("%s-%s", topic.getName(), partitionId);
	}
	
	/**
	 * @return the partitionId
	 */
	public int getPartitionId() {
		return partitionId;
	}

	/**
	 * @return the errorCode
	 */
	public short getErrorCode() {
		return errorCode;
	}

	/**
	 * @return the sizeInBytes
	 */
	public int getSizeInBytes() {
		return sizeInBytes;
	}

	/**
	 * @return the replicas
	 */
	public List<Integer> getReplicas() {
		return replicas;
	}

	/**
	 * @return the inSyncReplicas
	 */
	public List<Integer> getInSyncReplicas() {
		return inSyncReplicas;
	}

	/**
	 * @return the earliestOffset
	 */
	public Long getEarliestOffset() {
		return earliestOffset;
	}

	/**
	 * @param earliestOffset the earliestOffset to set
	 */
	public void setEarliestOffset(Long earliestOffset) {
		this.earliestOffset = earliestOffset;
	}

	/**
	 * @return the latestOffset
	 */
	public Long getLatestOffset() {
		return latestOffset;
	}

	/**
	 * @param latestOffset the latestOffset to set
	 */
	public void setLatestOffset(Long latestOffset) {
		this.latestOffset = latestOffset;
	}

	/**
	 * @return the leaderBrokerId
	 */
	public int getLeaderBrokerId() {
		return leaderBrokerId;
	}

	/**
	 * @return the leader
	 */
	public KafkaBroker getLeader() {
		return leader;
	}

	/**
	 * @param leader the leader to set
	 */
	public void setLeader(KafkaBroker leader) {
		this.leader = leader;
	}

	/**
	 * @return the topic
	 */
	public KafkaTopic getTopic() {
		return topic;
	}

	/**
	 * @param topic the topic to set
	 */
	public void setTopic(KafkaTopic topic) {
		this.topic = topic;
	}
	
}