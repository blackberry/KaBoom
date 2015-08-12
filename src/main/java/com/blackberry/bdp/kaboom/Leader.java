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
import java.util.ArrayList;
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
import com.blackberry.bdp.kaboom.api.KaBoomTopicConfig;
import com.blackberry.bdp.kaboom.api.KafkaTopic;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Leader extends LeaderSelectorListenerAdapter implements ThreadCompleteListener {

	private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
	protected static final Charset UTF8 = Charset.forName("UTF-8");
	protected static final Random rand = new Random();

	private ReadyFlagWriter readyFlagWriter;
	private CuratorFramework curator;
	private Thread readyFlagWriterThread;
	final protected StartupConfig config;	
	private HashMap<String, KaBoomTopic> kaboomTopics;
	private List<KafkaTopic> kafkaTopics;
	private List<KaBoomClient> kaboomClients;
	HashMap<Integer, KaBoomClient> kaboomClientMap;

	protected ReadyFlagController readyFlagController;

	public Leader(StartupConfig config) {
		this.config = config;
	}

	protected abstract void run_balancer() throws Exception;

	@Override
	public void takeLeadership(CuratorFramework curator) throws Exception {
		this.curator = curator;
		ZkUtils.writeToPath(curator, config.getZkPathLeaderClientId(), config.getKaboomId(), true);
		LOG.info("A new leader has been elected: kaboom.id={}", config.getKaboomId());
		// Chill for 30 seconds after the election, give whatever caused it a few moments to potentially subside
		Thread.sleep(30 * 1000);

		while (true) {
			kafkaTopics = KafkaTopic.getAll(config.getKafkaSeedBrokers(), "leaderLookup");
			kaboomClients = KaBoomClient.getAll(KaBoomClient.class, curator, config.getZkRootPathClients());						
			kaboomTopics = KaBoomTopic.getAllMap(config.getCurator(), config.getZkRootPathTopicConfigs(),
				 config.getZkRootPathPartitionAssignments());			

			int totalPartitions = 0;			
			for (KafkaTopic kafkaTopic : kafkaTopics) {
				totalPartitions += kafkaTopic.getPartitions().size();
			}

			int totalWeight = 0;			
			for (KaBoomClient kaboomClient : kaboomClients) {
				kaboomClient.setPartitionLoad(0);
				totalWeight += kaboomClient.getWeight();
				kaboomClientMap.put(kaboomClient.getId(), kaboomClient);
			}
			
			LOG.info("Found a total of {} configured topics in ZooKeeper", kaboomTopics.size());
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
							if (!kaboomTopics.containsKey(topic)) {
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
								kaboomClientMap.get(assignedClientId).incrementPartitionLoad(1);
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
				kaboomClient.calculateTargetLoad(totalPartitions, totalWeight);
			}
			
			// With that done then we can call the balance method...			

			try {
				run_balancer();
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
