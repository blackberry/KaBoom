/*
 * Copyright 2016 BlackBerry Limited.
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
package com.blackberry.bdp.cli;

import com.blackberry.bdp.common.props.Parser;
import com.blackberry.bdp.kaboom.StartupConfig;
import com.blackberry.bdp.kaboom.api.KaBoomClient;
import com.blackberry.bdp.kaboom.api.KaBoomPartition;
import com.blackberry.bdp.kaboom.api.KaBoomTopic;
import com.blackberry.bdp.krackle.meta.MetaData;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignmentStats {

	private static final Logger LOG = LoggerFactory.getLogger(AssignmentStats.class);
	protected static final Charset UTF8 = Charset.forName("UTF-8");
	private StartupConfig config;
	private MetaData kafkaMetaData;
	private List<KaBoomClient> kaboomClients;
	private List<KaBoomTopic> kaboomTopics;

	private final HashMap<Integer, KaBoomClient> idToKaBoomClient = new HashMap<>();
	private final HashMap<String, KaBoomTopic> nameToKaBoomTopic = new HashMap<>();
	private int totalPartitions;

	public static void main(String args[]) {
		AssignmentStats instance = new AssignmentStats();
		instance.report();
	}

	private AssignmentStats() {
		try {
			Properties props = StartupConfig.getProperties();
			Parser propsParser = new Parser(props);
			if (propsParser.parseBoolean("configuration.authority.zk", false)) {
				// TODO: ZK
			} else {
				LOG.info("Configuration authority is file based");
				config = new StartupConfig(props);
			}

			config.logConfiguraton();
		} catch (Exception e) {
			LOG.error("an error occured while building configuration object: ", e);
			System.exit(1);
		}

	}

	private void refreshMetadata() throws Exception {
		idToKaBoomClient.clear();
		nameToKaBoomTopic.clear();
		kafkaMetaData = MetaData.getMetaData(config.getKafkaSeedBrokers(), "kaboom");
		kaboomClients = KaBoomClient.getAll(KaBoomClient.class, config.getKaBoomCurator(), config.getZkRootPathClients());
		kaboomTopics = KaBoomTopic.getAll(kaboomClients,
			 kafkaMetaData,
			 config.getKaBoomCurator(),
			 config.getZkRootPathTopicConfigs(),
			 config.getZkRootPathPartitionAssignments(),
			 config.getZkRootPathFlagAssignments());

		for (KaBoomClient kaboomClient : kaboomClients) {
			idToKaBoomClient.put(kaboomClient.getId(), kaboomClient);
		}

		for (KaBoomTopic kaboomTopic : kaboomTopics) {
			nameToKaBoomTopic.put(kaboomTopic.getKafkaTopic().getName(), kaboomTopic);
		}
		totalPartitions = KaBoomTopic.getTotalPartitonCount(kaboomTopics);

		LOG.info("metadata refreshed for {} partitions", totalPartitions);
	}

	private void updatePartitonMap(HashMap<String, HashMap<String, List<String>>> counts,
		 String host,
		 String identifier,
		 KaBoomPartition partition) {
		HashMap<String, List<String>> hostCounts = counts.get(host);
		if (hostCounts == null) {
			hostCounts = new HashMap<>();
			counts.put(host, hostCounts);
		}
		List<String> partitions = hostCounts.get(identifier);
		if (partitions == null) {
			partitions = new ArrayList<>();
			partitions.add(partition.getTopicPartitionString());
			hostCounts.put(identifier, partitions);
		} else {
			partitions.add(partition.getTopicPartitionString());
			hostCounts.put(identifier, partitions);
		}

	}

	private void report() {
		try {
			int unassigned = 0;
			refreshMetadata();
			HashMap<String, List<String>> remoteSenders = new HashMap<>();
			HashMap<String, String> partitionIdToBroker = new HashMap<>();
			HashMap<String, HashMap<String, List<String>>> counts = new HashMap<>();
			for (KaBoomTopic topic : kaboomTopics) {
				for (KaBoomPartition partition : topic.getPartitions()) {
					partitionIdToBroker.put(partition.getTopicPartitionString(),
						 partition.getKafkaPartition().getLeader().getHost());
					if (partition.getAssignedClient() != null) {
						String kaboomHost = partition.getAssignedClient().getHostname();
						System.out.println(String.format("partition %s is assigned to %s",
							 partition.getTopicPartitionString(), kaboomHost));
						if (kaboomHost.equals(partition.getKafkaPartition().getLeader().getHost())) {
							updatePartitonMap(counts, kaboomHost, "local", partition);
						} else {
							updatePartitonMap(counts, kaboomHost, "remote", partition);
							String ownerHost = partition.getKafkaPartition().getLeader().getHost();
							List<String> remoteSendersPartitions = remoteSenders.get(ownerHost);
							if (remoteSendersPartitions == null) {
								remoteSendersPartitions = new ArrayList<>();
							}
							remoteSendersPartitions.add(partition.getTopicPartitionString());
							remoteSenders.put(ownerHost, remoteSendersPartitions);
						}
					} else {
						unassigned++;
						System.out.println(String.format("partion %s is not assigned", partition.getTopicPartitionString()));
					}
				}
			}
			DecimalFormat df = new DecimalFormat("0.0000");
			int totalLocal = 0;
			int totalRemote = 0;
			for (String hostname : counts.keySet()) {
				List<String> remotePartitions = counts.get(hostname).get("remote") != null
					 ? counts.get(hostname).get("remote") : new ArrayList<String>();
				List<String> localPartitions = counts.get(hostname).get("local") != null
					 ? counts.get(hostname).get("local") : new ArrayList<String>();
				totalRemote += remotePartitions.size();
				totalLocal += localPartitions.size();
				int remoteProducers = remoteSenders.get(hostname) != null ? remoteSenders.get(hostname).size() : 0;
				System.out.println(String.format("host: %s consuming: %d local / %d remote (%s percent local), producing: " + remoteProducers,
					 hostname,
					 localPartitions.size(),
					 remotePartitions.size(),
					 df.format(100.0 * ((float) localPartitions.size() / ((float) localPartitions.size() + (float) remotePartitions.size()))))
					 );
				for (String partitionId : remotePartitions) {
					System.out.println(String.format("\tremote partition %s lead broker is %s: ",
						 partitionId,
						 partitionIdToBroker.get(partitionId)));
				}

			}
			System.out.println(String.format("totals: %d total local, %d totalremote (%s total percent local)",
				 totalLocal,
				 totalRemote,
				 df.format(100.0 * ((float) totalLocal / ((float) totalLocal + (float) totalRemote)))));

			System.out.println(String.format("there are %s unassigned partitions", unassigned));

		} catch (Exception e) {
			LOG.error("There was a problem pruning the assignments of unsupported topics", e);
		}

	}

}
