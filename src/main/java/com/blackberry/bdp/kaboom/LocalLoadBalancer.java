/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blackberry.bdp.kaboom;

import static com.blackberry.bdp.common.conversion.Converter.intFromBytes;
import static com.blackberry.bdp.kaboom.Leader.UTF8;
import com.blackberry.bdp.kaboom.api.KaBoomClient;
import com.blackberry.bdp.kaboom.api.KaBoomPartition;
import com.blackberry.bdp.kaboom.api.KaBoomTopic;
import com.blackberry.bdp.krackle.meta.Broker;
import java.util.HashMap;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assigns partitions to whomever their local client is
 */
public class LocalLoadBalancer extends Leader {

	private static final Logger LOG = LoggerFactory.getLogger(EvenLoadBalancer.class);

	public LocalLoadBalancer(StartupConfig config) {
		super(config);
	}

	/**
	 * Unassign non-local assignments and assigns unassigned partitions to the local client
	 * @param kaboomClients
	 * @param kaboomTopics
	 * @throws java.lang.Exception
	 */
	@Override
	protected void run_balancer(
		 List<KaBoomClient> kaboomClients,
		 List<KaBoomTopic> kaboomTopics) throws Exception {

		HashMap<String, KaBoomPartition> idToPartition = new HashMap<>();
		for (KaBoomTopic topic : kaboomTopics) {
			for (KaBoomPartition partition : topic.getPartitions()) {
				idToPartition.put(partition.getTopicPartitionString(), partition);
			}
		}

		HashMap<String, KaBoomClient> hostToKaBoomClient = new HashMap<>();
		HashMap<Integer, KaBoomClient> idToKaBoomClient = new HashMap<>();
		for (KaBoomClient client : kaboomClients) {
			hostToKaBoomClient.put(client.getHostname(), client);
			idToKaBoomClient.put(client.getId(), client);
		}

		// Delete an assignemnts if the kaboom client isn't local
		try {
			for (String partitionId : curator.getChildren().forPath(config.getZkRootPathPartitionAssignments())) {
				String assignmentZkPath = String.format("%s/%s", config.getZkRootPathPartitionAssignments(), partitionId);
				KaBoomClient assignedClient = idToKaBoomClient.get(intFromBytes(curator.getData().forPath(assignmentZkPath)));
				Broker leader = idToPartition.get(partitionId).getKafkaPartition().getLeader();
				if (!leader.getHost().equals(assignedClient.getHostname())) {
					curator.delete().forPath(assignmentZkPath);
					LOG.info("Non-local assignment {} to client {} (hostname: {}) deleted because leader's  hostname is{}",
						 assignmentZkPath, assignedClient.getId(), assignedClient.getHostname(), leader.getHost());
				}
			}
		} catch (Exception e) {
			LOG.error("There was a problem pruning the assignments of unsupported topics", e);
		}

		for (KaBoomPartition partition : KaBoomPartition.unassignedPartitions(kaboomTopics)) {
			String leaderHostname = partition.getKafkaPartition().getLeader().getHost();
			KaBoomClient localClient = hostToKaBoomClient.get(leaderHostname);
			if (localClient != null) {
				String zkPath = String.format("%s/%s", config.getZkRootPathPartitionAssignments(),
					 partition.getTopicPartitionString());
				curator.create().withMode(CreateMode.PERSISTENT).forPath(zkPath,
					 String.valueOf(localClient.getId()).getBytes(UTF8));
				partition.setAssignedClient(localClient);
				localClient.getAssignedPartitions().add(partition);

			}

		}
	}

}
