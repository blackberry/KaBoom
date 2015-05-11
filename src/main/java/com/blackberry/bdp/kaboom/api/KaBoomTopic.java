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

import com.blackberry.bdp.common.conversion.Converter;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaBoomTopic {

	private static final Logger LOG = LoggerFactory.getLogger(KaBoomTopic.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private final String topicName;
	private final List<PartitionDetails> partitionDetails = new ArrayList<>();
	
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
			for (String partition :  Util.childrenInZkPath(curator, zkPathTopics + "/" + topicName)) {
				int partitionId = Integer.parseInt(partition);
				long offset = 0;
				long offsetTimestamp = 0;
				int assignedKaBoomClientId = 0;
				String path = zkPathTopics + "/" + topicName + "/" + partition;
				
				offset = Converter.longFromBytes(curator.getData().forPath(path));
				
				if (curator.checkExists().forPath(path + "/offset_timestamp") != null) {
					offsetTimestamp = Converter.longFromBytes(curator.getData().forPath(path + "/offset_timestamp"));
				}
				String assignmentPath = String.format("%s/%s-%s", zkPathAssignments, topicName, partition);
				if (curator.checkExists().forPath(assignmentPath) != null) {
					assignedKaBoomClientId = Integer.parseInt(new String(curator.getData().forPath(assignmentPath), UTF8));
				}
				
				topic.partitionDetails.add(new PartitionDetails(partitionId, offset, offsetTimestamp, assignedKaBoomClientId));
			}
		}
		return topics;
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
	public List<PartitionDetails> getPartitionDetails() {
		return partitionDetails;
	}
	
	private static class PartitionDetails
	{
		private final int partitionId;
		private final long offset;
		private final long offset_timestamp;
		private final int assignedKaBoomClientId;
		
		private PartitionDetails(int partitionId,
			 long offset,
			 long offset_timestamp,
			 int assignedKaBoomClientId) {
			this.partitionId = partitionId;
			this.offset = offset;
			this.offset_timestamp = offset_timestamp;
			this.assignedKaBoomClientId = assignedKaBoomClientId;
		}
			 		
		public int getPartitionId() {			
			return this.partitionId;
		}
		
		public long getOffset() {
			return this.offset;			
		}
		
		public long getOffsetTimestamp() {
			return this.offset_timestamp;
		}
		
		public int getAssignedKaBoomClientId() {
			return this.assignedKaBoomClientId;
		}			  
	}
}
