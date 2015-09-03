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

import com.blackberry.bdp.kaboom.api.KaBoomClient;
import com.blackberry.bdp.kaboom.api.KaBoomPartition;
import com.blackberry.bdp.kaboom.api.KaBoomTopic;
import com.blackberry.bdp.kaboom.api.KafkaBroker;
import com.blackberry.bdp.kaboom.api.KafkaTopic;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assigns partitions based off a weighted workload.  The default 
 * weighting is based on how many cores each client has. There will 
 * be a preference for local work while the client is under-loaded.
 * 
 * Work is assigned to least loaded once all clients are over-loaded.
 */
public class EvenLoadBalancer extends Leader {

	private static final Logger LOG = LoggerFactory.getLogger(EvenLoadBalancer.class);

	public EvenLoadBalancer(StartupConfig config) {
		super(config);
		LOG.info("The even load balancer has been instantiated");
	}

	@Override
	protected void run_balancer(
		 List<KafkaBroker> kafkaBrokers,
		 List<KaBoomClient> kaboomClients,
		 List<KaBoomTopic> kaboomTopics,
		 List<KafkaTopic> kafkaTopics) throws Exception {

		// Overloaded?  
		for (KaBoomClient client : kaboomClients) {
			LOG.info("Client {} has {} assigned partitons", client.getId(), client.getAssignedPartitions().size());
			if (client.tooManyAssignedPartitions() == false) {
				continue;
			}
			LOG.info("KaBoom client {} is overloaded and needs to shed assignments", client.getId());
			// Build up our lists of local and remote partitions...
			List<KaBoomPartition> localPartitions = new ArrayList<>();
			List<KaBoomPartition> remotePartitions = new ArrayList<>();
			for (KaBoomPartition partition : client.getAssignedPartitions()) {
				if (partition.getKafkaPartition().getLeader().getHost().equals(
					 client.getHostname())) {
					localPartitions.add(partition);
				} else {
					remotePartitions.add(partition);
				}
			}
			// ...then unassign partitions, remote ones first until we're even
			while (client.tooManyAssignedPartitions()) {
				KaBoomPartition partitionToDelete;
				LOG.info("client {} target partiton load is {} and assigned partitons count is {}",
					 client.getId(), client.getTargetPartitionLoad(), client.getAssignedPartitions().size());
				if (remotePartitions.size() > 0) {
					partitionToDelete = remotePartitions.remove(rand.nextInt(remotePartitions.size()));
				} else {
					partitionToDelete = localPartitions.remove(rand.nextInt(localPartitions.size()));
				}
				String deletePath = config.zkPathPartitionAssignment(partitionToDelete.getTopicPartitionString());
				try {
					curator.delete().forPath(deletePath);
					client.getAssignedPartitions().remove(partitionToDelete);
					LOG.info("Deleted assignment {} to relieve overloaded client {}",
						 deletePath, client.getId());
				} catch (Exception e) {
					LOG.error("Failed to delete assignment {} to relieve overloaded client {}",
						 deletePath, client.getId(), e);
				}
			}
		}

		// Sorts the clients based on their assigned partition load
		Comparator<KaBoomClient> leastLoadedSorter = new Comparator<KaBoomClient>() {
			@Override
			public int compare(KaBoomClient clientA, KaBoomClient clientB) {
				double valA = clientA.getAssignedPartitions().size() / clientA.getTargetPartitionLoad();
				double valB = clientB.getAssignedPartitions().size() / clientB.getTargetPartitionLoad();
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

		// Find clients for any unassigned partitions
		List<KaBoomPartition> unassignedPartitions = KaBoomPartition.unassignedPartitions(kaboomTopics);
		LOG.info("[even load balancer] there are {} unassigned partitons", unassignedPartitions.size());
		for (KaBoomPartition partition : unassignedPartitions) {
			Collections.sort(kaboomClients, leastLoadedSorter);
			// Grab the least loaded client  in case we  cannot find an underloaded local client
			KaBoomClient chosenClient = kaboomClients.get(0);
			for (KaBoomClient client : kaboomClients) {
				if (!client.tooManyAssignedPartitions() && client.getHostname().equals(
					 partition.getKafkaPartition().getLeader().getHost())) {
					chosenClient = client;
					break;
				}
			}
			String zkPath = config.zkPathPartitionAssignment(partition.getTopicPartitionString());			
			try {				
				if (curator.checkExists().forPath(zkPath) != null) {
					curator.setData().forPath(zkPath, 
						 String.valueOf(chosenClient.getId()).getBytes(UTF8));
				} else {
					curator.create().withMode(CreateMode.PERSISTENT).forPath(zkPath,
						 String.valueOf(chosenClient.getId()).getBytes(UTF8));
				}
				partition.setAssignedClient(chosenClient);
				chosenClient.getAssignedPartitions().add(partition);
				LOG.info("Assigned {} to client ID {}", partition.getTopicPartitionString(), chosenClient.getId());
			} catch (Exception e) {
				LOG.error("Failed to create assignment {} for {}", zkPath, chosenClient.getId(), e);
			}
		}
	}

}
