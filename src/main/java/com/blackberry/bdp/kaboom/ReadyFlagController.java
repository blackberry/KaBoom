/*
 * Copyright 2015 BlackBerry, Inc.
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

import com.blackberry.bdp.kaboom.api.KaBoomClient;
import com.blackberry.bdp.kaboom.api.KaBoomTopic;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadyFlagController {

	private static final Logger LOG = LoggerFactory.getLogger(ReadyFlagController.class);
	protected static final Random rand = new Random();
	protected static final Charset UTF8 = Charset.forName("UTF-8");

	private final CuratorFramework curator;
	private final String flagAssignmentsPath;
	private final List<KaBoomTopic> unassignedTopics;
	private final List<KaBoomTopic> kaboomTopics;

	public ReadyFlagController(StartupConfig config, List<KaBoomTopic> kaboomTopics) throws Exception {
		this.curator = config.getKaBoomCurator();
		this.unassignedTopics = new ArrayList<>();
		this.kaboomTopics = kaboomTopics;
		this.flagAssignmentsPath = config.getZkRootPathFlagAssignments();
	}

	public static Path getRootFromPathTemplate(String pathTemplate) throws Exception {
		Pattern hdfsRootPattern = Pattern.compile("(.*?)/%y%M%d/%H/.*?");
		Matcher hdfsRootMatcher = hdfsRootPattern.matcher(pathTemplate);
		if (hdfsRootMatcher.matches()) {
			return new Path(hdfsRootMatcher.group(1));
		} else {
			throw new Exception("Failed to parse hdfs root from path template: " + pathTemplate);
		}
	}

	public void balance(int totalWeight, HashMap<Integer, KaBoomClient> kaboomClientMap) throws Exception {		
		/**
		 * Our topics for a single service are generally within the same HDFS root path
		 * and we don't want to create a flag propagator for each one.  Let's get a unique 
		 * list of HDFS root paths from all the topics and ensure we only load balance those
		 */
		unassignedTopics.clear();
		
		Map<Path, String> tempHdfsPathToTopic = new HashMap<>();		
		Map<String, KaBoomTopic> nameToKaBoomTopic = new HashMap<>();		
		for (KaBoomTopic topic : this.kaboomTopics) {
			Path tempPath = getRootFromPathTemplate(topic.getConfig().getHdfsRootDir());
			if (!tempHdfsPathToTopic.containsKey(tempPath)) {
				LOG.debug("Topic {} is the first instance with a unique HDFS root path {}", 
					 topic.getName(), tempPath);
				tempHdfsPathToTopic.put(tempPath, topic.getName());
				unassignedTopics.add(topic);
				nameToKaBoomTopic.put(topic.getName(), topic);
			}
		}

		// Build a new List for KaBoomClient's since we'll be sorting
		List<KaBoomClient> kaboomClientList = new ArrayList<>();		
		
		// Clear the existing flag assignments collection, and re-caculate our target loads		
		for (Entry<Integer, KaBoomClient> entry : kaboomClientMap.entrySet()) {
			entry.getValue().getAssignedFlagPropagatorTopics().clear();
			entry.getValue().calculateFlagPropagatorTargetLoad(unassignedTopics.size(), totalWeight);
			kaboomClientList.add(entry.getValue());
		}

		// Delete any flag assignemnts for disconnected clients or for topics that are no longer configured
		try {
			for (String topic : curator.getChildren().forPath(flagAssignmentsPath)) {								
				try {
					String zkPathAssignment = String.format("%s/%s", flagAssignmentsPath, topic);
					int assignedClientId = Integer.parseInt(new String(curator.getData().forPath(zkPathAssignment), UTF8));
					String reasonToDelete = null;
					if (!nameToKaBoomTopic.containsKey(topic)) {
						reasonToDelete = String.format("becaue the topic {} is no longer configured in KaBoom", topic);
					} else if (!kaboomClientMap.containsKey(assignedClientId)) {
						reasonToDelete = String.format("becaue assigned KaBoomClient is no longer connected");
					}
					if (reasonToDelete != null) {
						curator.delete().forPath(zkPathAssignment);
						LOG.info("Flag propagator assignment {} for {} removed {}",
							 zkPathAssignment, assignedClientId, reasonToDelete);
					} else {
						kaboomClientMap.get(assignedClientId).getAssignedFlagPropagatorTopics().add(
							 nameToKaBoomTopic.get(topic));
						unassignedTopics.remove(nameToKaBoomTopic.get(topic));
					}
				} catch (Exception e) {
					LOG.error("[{}] Failed to grab current client flag propagator assignment for topic", topic);
				}
			}
		} catch (Exception ex) {
			LOG.error("There was a failure iterating over the children of the flagAssignmentPath", ex);
			return;
		}

		// Delete flag assignments for clients that are assigned too many topics
		for (Map.Entry<Integer, KaBoomClient> e : kaboomClientMap.entrySet()) {
			int clientId = e.getKey();
			KaBoomClient client = e.getValue();
			// delete assignments for clients that are over-assigned their target load 
			if (client.tooManyAssignedFlags()) {
				LOG.info("Client {}'s flag propagator load is {} and target load is {}, need to  unassign topics",
					 clientId, client.getAssignedFlagPropagatorTopics().size(), client.getTargetFlagPropagatorLoad());				
				int numTotalFails = 0;
				while (client.tooManyAssignedFlags()) {
					// Find a random topic in the assigned unassignedTopics and delete it										
					KaBoomTopic topicToDelete = client.getAssignedFlagPropagatorTopics().get(
						 rand.nextInt(client.getAssignedFlagPropagatorTopics().size()));
					String deletePath = String.format("%s/%s", flagAssignmentsPath, topicToDelete.getKafkaTopic().getName());
					try {
						curator.delete().forPath(deletePath);
						LOG.info("Deleted flag propagator assignment ZK path {} for clientId {}:", deletePath, clientId);
						client.getAssignedFlagPropagatorTopics().remove(topicToDelete);						
					} catch (Exception ex) {
						LOG.error("Failed to delete flag propagator assignment ZK path {} for clientId {}:", deletePath, clientId, ex);
						numTotalFails++;
						if (numTotalFails > 4) {
							LOG.error("Failed to delete {} {} times, giving up!", deletePath, numTotalFails);
							throw ex;
						}
						Thread.sleep(500);
					}
				}
			}
		}

		// Sort the clients based on their number of flag propagator assignments
		Comparator<KaBoomClient> comparator = new Comparator<KaBoomClient>() {
			@Override
			public int compare(KaBoomClient clientA, KaBoomClient clientB) {
				double valA = clientA.getAssignedFlagPropagatorTopics().size() / clientA.getTargetFlagPropagatorLoad();
				double valB = clientB.getAssignedFlagPropagatorTopics().size() / clientB.getTargetFlagPropagatorLoad();
				if (valA == valB) {
					return 0;
				} else {
					if (valA > valB) {
						return 1;
					} else {
						return -1;
					}
				}
			}
		};

		LOG.info("[ready flag controller] there are {} unassigned topics for flag propagation", unassignedTopics.size());
		
		// Assign unassigned topics to the least loaded client sorting it on each iteration
		int numFailures = 0;		
		int maxRetires = 5;		
		Iterator<KaBoomTopic> iter = unassignedTopics.iterator();		
		while (iter.hasNext()) {
			KaBoomTopic topic = iter.next();
			Collections.sort(kaboomClientList, comparator);
			KaBoomClient leastLoadedClient = kaboomClientList.get(0);
			String assignmentPath = String.format("%s/%s", flagAssignmentsPath, topic.getName());
			try {
				if (curator.checkExists().forPath(assignmentPath) != null) {
					curator.setData().forPath(assignmentPath, 
						 String.valueOf(leastLoadedClient.getId()).getBytes(UTF8));
				} else {
					curator.create().withMode(CreateMode.PERSISTENT).forPath(assignmentPath, 
						 String.valueOf(leastLoadedClient.getId()).getBytes(UTF8));							  
				}
				leastLoadedClient.getAssignedFlagPropagatorTopics().add(topic);
				LOG.info("Flag propagation assigned {} to {}", topic.getName(), leastLoadedClient.getId());
				iter.remove();
			} catch (Exception e) {
				numFailures++;
				LOG.error("Failed {}/{} attempts to create {} for clientId {}", 
					 numFailures, maxRetires, assignmentPath, leastLoadedClient.getId(), e);
				if (numFailures == maxRetires) {
					LOG.error("Number of retries exhausted, re-throwing exception upwards");
					throw e;
				}								
			}
		}
	}
}
