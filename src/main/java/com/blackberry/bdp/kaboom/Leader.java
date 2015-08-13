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

import static com.blackberry.bdp.common.conversion.Converter.intFromBytes;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.kaboom.api.KaBoomTopic;
import com.blackberry.bdp.common.zk.ZkUtils;
import com.blackberry.bdp.common.threads.NotifyingThread;
import com.blackberry.bdp.common.threads.ThreadCompleteListener;
import com.blackberry.bdp.kaboom.api.KaBoomClient;
import com.blackberry.bdp.kaboom.api.KafkaBroker;
import com.blackberry.bdp.kaboom.api.KafkaTopic;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Leader extends LeaderSelectorListenerAdapter implements ThreadCompleteListener {

	private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
	protected static final Charset UTF8 = Charset.forName("UTF-8");
	protected static final Random rand = new Random();

	final protected StartupConfig config;	
	private ReadyFlagWriter readyFlagWriter;
	private CuratorFramework curator;
	private Thread readyFlagWriterThread;	
	
	private List<KafkaBroker> kafkaBrokers;
	private List<KaBoomTopic> kaboomTopics;
	private List<KafkaTopic> kafkaTopics;
	private List<KaBoomClient> kaboomClients;
	
	private HashMap<Integer, KaBoomClient> kaboomClientMap;
	private HashMap<String, KaBoomTopic> kaboomTopicMap;

	protected ReadyFlagController readyFlagController;

	public Leader(StartupConfig config) {
		this.config = config;
	}

	protected abstract void run_balancer(
		 List<KafkaBroker> kafkaBrokers, 
		 List<KaBoomClient> kaboomClients, 
		 List<KaBoomTopic> kaboomTopics, 
		 List<KafkaTopic> kafkaTopics) 
		 throws Exception;

	@Override
	public void takeLeadership(CuratorFramework curator) throws Exception {
		this.curator = curator;
		ZkUtils.writeToPath(curator, config.getZkPathLeaderClientId(), config.getKaboomId(), true);
		LOG.info("KaBoom client ID {} is the new leader, entering the 30s calm down", config.getKaboomId());		
		Thread.sleep(30 * 1000);

		while (true) {
			kafkaBrokers = KafkaBroker.getAll(config.getKafkaCurator(), config.getZkRootPathKafkaBrokers());
			kafkaTopics = KafkaTopic.getAll(config.getKafkaSeedBrokers(), "leaderLookup");
			kaboomClients = KaBoomClient.getAll(KaBoomClient.class, curator, config.getZkRootPathClients());						
			kaboomTopics = KaBoomTopic.getAll(config.getKaBoomCurator(), config.getZkRootPathTopicConfigs(),
				 config.getZkRootPathPartitionAssignments());			

			int totalPartitions = 0;			
			for (KafkaTopic kafkaTopic : kafkaTopics) {
				totalPartitions += kafkaTopic.getPartitions().size();
			}

			int totalWeight = 0;			
			for (KaBoomClient kaboomClient : kaboomClients) {
				kaboomClient.getAssignedPartitionIds().clear();
				totalWeight += kaboomClient.getWeight();
				kaboomClientMap.put(kaboomClient.getId(), kaboomClient);
			}
			
			for (KaBoomTopic kaboomTopic : kaboomTopics) {
				kaboomTopicMap.put(kaboomTopic.getTopicName(), kaboomTopic);
			}
			
			/**
			 * This an an abstract KaBoom leader that must be extended and implemented 
			 * by an actual load balancer. There are, however, tasks that must always be 
			 * performed regardless of a balancer implementation. Tasks such as removing 
			 * assignments for non-existent clients and running the Kafka ready flag writer,
			 * etc. Let's do those, and then call the balancer...
			 */
			try {
				for (String partitionId : curator.getChildren().forPath(config.getZkRootPathPartitionAssignments())) {
					try {
						Pattern topicPartitionPattern = Pattern.compile("^(.*)-(\\d+)$");
						Matcher m = topicPartitionPattern.matcher(partitionId);
						if (m.matches()) {
							String topic = m.group(1);
							String assignmentZkPath = String.format("%s/%s", config.getZkRootPathPartitionAssignments(), partitionId);
							int assignedClientId = intFromBytes(curator.getData().forPath(assignmentZkPath));
							String deletedReason = null;
							if (!kaboomTopicMap.containsKey(topic)) {
								deletedReason = "because of missing topic configuration";
							} else {
								if (!KaBoomClient.isConnected(curator, config.getZkRootPathClients(), assignedClientId)) {
									deletedReason = String.format("because client %s is not connected", assignedClientId);
								}
							}
							if (deletedReason != null) {
								curator.delete().forPath(assignmentZkPath);
								LOG.info("Assignment of {} to {} deleted from {} {}",
									 partitionId, assignedClientId, assignmentZkPath, deletedReason);
							} else {
								kaboomClientMap.get(assignedClientId).getAssignedPartitionIds().add(partitionId);
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

			try {
				readyFlagController = new ReadyFlagController(config);
				readyFlagController.balance(totalWeight, kaboomClientMap);
			} catch (Exception e) {
				LOG.error("There was an error running the ready flag controller's balancer", e);
			}
			
			/*
			 * All the operations that the leader performs that involve modifications 
			 * to the KaBoomClient objects should now be completed.  Let's iterate 
			 * over them and save() to persist their attributes in Zk
			 */
			

			/*
			 *  Check to see if the kafka_ready flag writer thread exists and is alive:
			 *  
			 *  If it doesn't exist or isn't running, start it.  This is designed to 
			 *  work well when the load balancer sleeps for 10 minutes after assigning 
			 *  work.  If that behavior changes then additional logic will be required
			 *  to ensure this isn't executed too often  
			 */
			if (readyFlagWriterThread == null || !readyFlagWriterThread.isAlive()) {
				LOG.info("[ready flag writer] thread doesn't exist or is not running");
				readyFlagWriter = new ReadyFlagWriter(config);
				readyFlagWriter.addListener(this);
				readyFlagWriterThread = new Thread(readyFlagWriter);
				readyFlagWriterThread.start();
				LOG.info("[ready flag writer] thread created and started");
			} else {
				LOG.warn("[ready flag writer] is either not null or is still alive (could it be hung?)");
			}

			Thread.sleep(config.getRunningConfig().getLeaderSleepDurationMs());
		}
	}

	/*
	 * This method is called when the ready flag writer thread finishes.  There is 
	 * currently nothing special that needs to happen, but later we may wish to have 
	 * greater control over when threads start/stop and when they need to be ran 
	 * again
	 */
	@Override
	public void notifyOfThreadComplete(NotifyingThread notifyingThread, Exception e) {
		if (e != null) {
			LOG.error("[ready flag writer] Exception raised in thread: {}", e);
		}
	}

}
