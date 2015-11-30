/*
 * Copyright 2015 BlackBerry Limited.
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
package com.blackberry.bdp.kaboom;

import static com.blackberry.bdp.common.conversion.Converter.longFromBytes;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.kaboom.api.KaBoomTopic;
import com.blackberry.bdp.common.zk.ZkUtils;
import com.blackberry.bdp.kaboom.api.KaBoomClient;
import com.blackberry.bdp.kaboom.api.KafkaBroker;
import com.blackberry.bdp.kaboom.api.KafkaPartition;

import com.blackberry.bdp.kaboom.api.KafkaTopic;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.zookeeper.CreateMode;

public abstract class Leader extends LeaderSelectorListenerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
	protected static final Charset UTF8 = Charset.forName("UTF-8");
	protected static final Random rand = new Random();

	final protected StartupConfig config;
	protected CuratorFramework curator;

	private List<KafkaBroker> kafkaBrokers;
	private List<KaBoomTopic> kaboomTopics;
	private List<KafkaTopic> kafkaTopics;
	private List<KaBoomClient> kaboomClients;

	private final HashMap<Integer, KaBoomClient> idToKaBoomClient = new HashMap<>();
	private final HashMap<String, KaBoomTopic> nameToKaBoomTopic = new HashMap<>();
	private final HashMap<String, KafkaPartition> kafkaPartitionIdToPartition = new HashMap<>();
	
	private int totalPartitions;			
	private int totalWeight;

	public Leader(StartupConfig config) {
		this.config = config;
	}

	protected abstract void run_balancer(
		 List<KafkaBroker> kafkaBrokers,
		 List<KaBoomClient> kaboomClients,
		 List<KaBoomTopic> kaboomTopics,
		 List<KafkaTopic> kafkaTopics)
		 throws Exception;

	private void deleteAssignment(String reason, String zkPath) throws Exception {
		curator.delete().forPath(zkPath);
		LOG.info("Assignment {} deleted {}", zkPath, reason);
	}
	
	private void refreshMetadata() throws Exception {
		idToKaBoomClient.clear();
		nameToKaBoomTopic.clear();
		kafkaPartitionIdToPartition.clear();

		kafkaBrokers = KafkaBroker.getAll(config.getKafkaCurator(), config.getZkRootPathKafkaBrokers());
		kafkaTopics = KafkaTopic.getAll(config.getKafkaSeedBrokers(), "leaderLookup", kafkaBrokers);
		kaboomClients = KaBoomClient.getAll(KaBoomClient.class, curator, config.getZkRootPathClients());
		kaboomTopics = KaBoomTopic.getAll(kaboomClients, kafkaTopics,
			 config.getKaBoomCurator(),
			 config.getZkRootPathTopicConfigs(),
			 config.getZkRootPathPartitionAssignments(),
			 config.getZkRootPathFlagAssignments());
		
		totalPartitions = KaBoomTopic.getTotalPartitonCount(kaboomTopics);
		totalWeight = 0;
		
		for (KaBoomClient kaboomClient : kaboomClients) {
			totalWeight += kaboomClient.getWeight();
			idToKaBoomClient.put(kaboomClient.getId(), kaboomClient);
		}
		LOG.debug("The total weight of the KaBoom cluster is {}", totalWeight);

		for (KaBoomTopic kaboomTopic : kaboomTopics) {
			nameToKaBoomTopic.put(kaboomTopic.getKafkaTopic().getName(), kaboomTopic);
		}

		for (KafkaTopic kafkaTopic : kafkaTopics) {
			for (KafkaPartition kafkaPartition : kafkaTopic.getPartitions()) {
				kafkaPartitionIdToPartition.put(kafkaPartition.getTopicPartitionString(), kafkaPartition);
			}
		}
		
		LOG.info("metadata refreshed");
	}
	
	private void pauseOnFirstDisconnectedAssignee() throws Exception {		
		for (String partitionId : curator.getChildren().
			 forPath(config.getZkRootPathPartitionAssignments())) {
			long sleepTime = config.getRunningConfig().getLeaderNodeDisconnectionWaittimeSeconds();
			try {
			String assignedClientId = new String(curator.getData()
				 .forPath(String.format("%s/%s", 
					 config.getZkRootPathPartitionAssignments(), partitionId)), UTF8);				
				if (!idToKaBoomClient.containsKey(Integer.parseInt(assignedClientId))) {
					LOG.warn("disconnected client detected forcing {} second sleep",
						 sleepTime);
					Thread.sleep(sleepTime * 1000);
					refreshMetadata();
					return;
				}				
			} catch (Exception e) {
				LOG.error("error while fetching unique client IDs: ", e);
			}
		}
		
	}
	
	@Override
	public void takeLeadership(CuratorFramework curator) throws Exception {
		this.curator = curator;
		ZkUtils.writeToPath(curator, config.getZkPathLeaderClientId(), config.getKaboomId(), true, CreateMode.EPHEMERAL);
		LOG.info("KaBoom client ID {} is the new leader, entering the {} calm down",
			 config.getKaboomId(), config.getRunningConfig().getNewLeaderCalmDownDelay());
		Thread.sleep(config.getRunningConfig().getNewLeaderCalmDownDelay());

		while (true) {

			refreshMetadata();
			pauseOnFirstDisconnectedAssignee();			

			// Delete an assignemnts if the kaboom client isn't connected or the topic is not configured
			try {
				for (String partitionId : curator.getChildren().forPath(config.getZkRootPathPartitionAssignments())) {
					try {
						Pattern topicPartitionPattern = Pattern.compile("^(.*)-(\\d+)$");
						Matcher m = topicPartitionPattern.matcher(partitionId);
						if (m.matches()) {
							String assignmentZkPath = String.format("%s/%s", config.getZkRootPathPartitionAssignments(), partitionId);
							String clientId = new String(curator.getData().forPath(assignmentZkPath), UTF8);
							String topicName = m.group(1);
							int partitonId = Integer.parseInt(m.group(2));
							int assignedClientId = new Integer(clientId);							
							
							// Check for all the reasons to delete an invalid assignment
							
							if (!nameToKaBoomTopic.containsKey(topicName)) {
								deleteAssignment("because of missing topic configuration", assignmentZkPath);
							} else if (!idToKaBoomClient.containsKey(assignedClientId)) {
								deleteAssignment(String.format("because client %s is not connected", assignedClientId), 
									 assignmentZkPath);
							} else if (!kafkaPartitionIdToPartition.containsKey(partitionId)) {
								deleteAssignment(String.format("because %s is not a valid Kafka partition", partitionId),
									 assignmentZkPath);
							} else {								
								idToKaBoomClient.get(assignedClientId).getAssignedPartitions().add(
									 nameToKaBoomTopic.get(topicName).getKaBoomPartition(partitonId));
								LOG.info("Pre-balanced found  {} assigned to {}", partitionId, assignedClientId);
							}
						}
					} catch (Exception e) {
						LOG.error("There was a problem pruning the assignments of unsupported topic {}", partitionId, e);
					}
				}
			} catch (Exception e) {
				LOG.error("There was a problem pruning the assignments of unsupported topics", e);
			}

			/**
			 * By now we have cleaned up invalid partition assignments 
			 * and we know our total weight and partition count as well
			 * as how much work each client is currently being assigned
			 * so we can calculate each client's target load
			 */
			for (KaBoomClient kaboomClient : kaboomClients) {
				kaboomClient.calculateTargetPartitionLoad(totalPartitions, totalWeight);
			}

			// With that done then we can call the balance method...			
			try {
				run_balancer(kafkaBrokers, kaboomClients, kaboomTopics, kafkaTopics);
			} catch (Exception e) {
				LOG.error("The load balancer raised an exception: ", e);
			}

			Thread.sleep(config.getRunningConfig().getLeaderSleepDurationMs());
		}
	}

}
