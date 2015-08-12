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
	private final String topicName;
	private final List<KaBoomPartitionDetails> partitionDetails = new ArrayList<>();
	private KaBoomTopicConfig config;

	public KaBoomTopic(String topicName) {
		this.topicName = topicName;
	}

	/**
	 * @param curator
	 * @param zkPathTopics
	 * @param zkPathAssignments
	 * @return
	 * @throws java.lang.Exception
	 */
	public static List<KaBoomTopic> getAll(CuratorFramework curator,
		 String zkPathTopics,
		 String zkPathAssignments) throws Exception {

		List<KaBoomTopic> topics = new ArrayList<>();

		for (String topicName : Util.childrenInZkPath(curator, zkPathTopics)) {
			KaBoomTopic topic = new KaBoomTopic(topicName);
			topics.add(topic);
			String topicZkPath = String.format("%s/%s", zkPathTopics, topicName);
			for (String partition : Util.childrenInZkPath(curator, topicZkPath)) {
				int partitionId = Integer.parseInt(partition);
				long offset = 0;
				long offsetTimestamp = 0;
				int assignedKaBoomClientId = 0;
				String path = topicZkPath + "/" + partition;

				offset = Converter.longFromBytes(curator.getData().forPath(path));

				if (curator.checkExists().forPath(path + "/offset_timestamp") != null) {
					offsetTimestamp = Converter.longFromBytes(curator.getData().forPath(path + "/offset_timestamp"));
				}
				String assignmentPath = String.format("%s/%s-%s", zkPathAssignments, topicName, partition);
				if (curator.checkExists().forPath(assignmentPath) != null) {
					assignedKaBoomClientId = Integer.parseInt(new String(curator.getData().forPath(assignmentPath), UTF8));
				}

				topic.partitionDetails.add(new KaBoomPartitionDetails(partitionId, offset, offsetTimestamp, assignedKaBoomClientId));
				topic.config = KaBoomTopicConfig.get(KaBoomTopicConfig.class, curator, topicZkPath);
			}
		}
		return topics;
	}

	public static HashMap<String, KaBoomTopic> getAllMap(CuratorFramework curator,
		 String zkPathTopics,
		 String zkPathAssignments) throws Exception{		
		HashMap<String, KaBoomTopic> map = new HashMap<>();
		for (KaBoomTopic topic : getAll(curator, zkPathTopics, zkPathAssignments)) {
			map.put(topic.topicName, topic);
		}
		return map;
	}
	
	public long oldestPartitionOffset() throws Exception {
		long oldestTimestamp = -1;
		for (KaBoomPartitionDetails partitonDetail : partitionDetails) {
			if (partitonDetail.getOffsetTimestamp() < oldestTimestamp || oldestTimestamp == -1) {
				oldestTimestamp = partitonDetail.getOffsetTimestamp();
			}
		}
		if (oldestTimestamp == -1) {
			throw new Exception("Failed to get oldest partition timestamp for topic: " + topicName);
		}
		return oldestTimestamp;
	}

	/**
	 * @return the topicName
	 */
	public String getTopicName() {
		return topicName;
	}

	/**
	 * @return the partitionDetails
	 */
	public List<KaBoomPartitionDetails> getPartitionDetails() {
		return partitionDetails;
	}

	/**
	 * @return the config
	 */
	public KaBoomTopicConfig getConfig() {
		return config;
	}

}
