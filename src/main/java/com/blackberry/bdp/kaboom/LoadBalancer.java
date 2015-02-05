/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blackberry.bdp.kaboom;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.common.utils.threads.NotifyingThread;
import com.blackberry.bdp.common.utils.threads.ThreadCompleteListener;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;

public class LoadBalancer extends LeaderSelectorListenerAdapter implements ThreadCompleteListener
{
	private static final Logger LOG = LoggerFactory.getLogger(LoadBalancer.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private static final Random rand = new Random();

	private ReadyFlagWriter readyFlagWriter;
	private Thread readyFlagThread;
	final private KaboomConfiguration config;

	public LoadBalancer(KaboomConfiguration config)
	{
		this.config = config;
	}

	@Override
	public void takeLeadership(CuratorFramework curator) throws Exception
	{
		/**
		 * Chill for 30 seconds after the election, give whatever caused it a few moments to potentially subside
		 */

		LOG.info("a new leader has been elected: kaboom.id={}", config.getKaboomId());
	
		Thread.sleep(30 * 1000);

		while (true)
		{
			Map<String, String> partitionToHost = new HashMap<>();
			Map<String, List<String>> hostToPartition = new HashMap<>();
			final Map<String, KaBoomNodeInfo> clients = new HashMap<>();
			Map<String, List<String>> clientToPartitions = new HashMap<>();
			Map<String, String> partitionToClient = new HashMap<>();
			List<String> topics = new ArrayList<>();
			List<String> unsupportedTopics = new ArrayList<>();

			// Get a full set of metadata from Kafka
			StateUtils.readTopicsFromZooKeeper(config.getKafkaZkConnectionString(), topics);
			
			LOG.info("Found a total of {} topics in ZooKeeper", topics.size());
			
			for (int i = 0; i < topics.size(); i++)
			{
				if (false == config.getTopicToSupportedStatus().get(topics.get(i)))
				{					
					unsupportedTopics.add(topics.get(i));
					topics.remove(i);
				}
			}
			
			LOG.info("Of the all the topics in ZooKeeper there are {} that are unsupported because there were no HDFS paths configured: {}", 
				 unsupportedTopics.size(), StringUtils.join(unsupportedTopics, ", "));

			// Map partitionId to leader/host and leader/host to partitionId
			StateUtils.getPartitionHosts(config.getKafkaSeedBrokers(), topics, partitionToHost, hostToPartition);

			// Get a list of active clients from zookeeper
			StateUtils.getActiveClients(curator, clients);
			
			/**
			 * Determine if there are any assignments for partitions on topics that are no longer supported (i.e. have missing HDFS output paths)
			 * Prune them by removing the assignment in ZooKeeper when found
			 */
			
			try
			{
				for (String partitionId : curator.getChildren().forPath("/kaboom/assignments")) 
				{
					try
					{
						Pattern topicPartitionPattern = Pattern.compile("^(.*)-(\\d+)$");
						Matcher m = topicPartitionPattern.matcher(partitionId);

						if (m.matches())
						{
							String topic = m.group(1);
							if (false == config.getTopicToSupportedStatus().get(topic))
							{
								String assignedClient = new String(curator.getData().forPath("/kaboom/assignments/" + partitionId), UTF8);
								LOG.info("Deleted assignment for unsupported topic partiton {} previously assigned to {}", partitionId, assignedClient);
								curator.delete().forPath("/kaboom/assignments/" + partitionId);
							}
						}
					}
					catch (Exception e)
					{
						LOG.error("There was a problem pruning the assignments of unsupported topic {}", partitionId, e);
					}
				}
			}
			catch (Exception e)
			{
				LOG.error("There was a problem pruning the assignments of unsupported topics", e);
			}			

			// Get a list of current assignments
			
			for (String partition : partitionToHost.keySet())
			{
				Stat stat = curator.checkExists().forPath("/kaboom/assignments/" + partition);
				
				if (stat != null)
				{
					// check if the client is still connected, and delete node if it is not.
					
					String client = new String(curator.getData().forPath("/kaboom/assignments/" + partition), UTF8);
					
					if (clients.containsKey(client))
					{
						LOG.debug("Partition {} : client {} is connected", partition, client);
						
						partitionToClient.put(partition, client);						
						List<String> parts = clientToPartitions.get(client);
						
						if (parts == null)
						{
							parts = new ArrayList<>();
							clientToPartitions.put(client, parts);
						}
						parts.add(partition);
					} 
					else
					{
						LOG.debug("Partition {} : client {} is not connected", partition, client);
						curator.delete().forPath("/kaboom/assignments/" + partition);
						stat = null;
					}
				}
			}

			StateUtils.calculateLoad(partitionToHost, clients, clientToPartitions);

			/**
			 * For every client, determine if it's doing too much work and remove assignments (remote ones first)
			 */			
			for (Entry<String, KaBoomNodeInfo> e : clients.entrySet())
			{
				String client = e.getKey();
				KaBoomNodeInfo info = e.getValue();

				if (info.getLoad() >= info.getTargetLoad() + 1)
				{
					List<String> localPartitions = new ArrayList<>();
					List<String> remotePartitions = new ArrayList<>();

					for (String partition : clientToPartitions.get(client))
					{
						if (partitionToHost.get(partition).equals(info.getHostname()))
						{
							localPartitions.add(partition);
						} 
						else
						{
							remotePartitions.add(partition);
						}
					}

					while (info.getLoad() > info.getTargetLoad())
					{
						String partitionToDelete;
						if (remotePartitions.size() > 0)
						{
							partitionToDelete = remotePartitions.remove(rand.nextInt(remotePartitions.size()));
						} 
						else
						{
							partitionToDelete = localPartitions.remove(rand.nextInt(localPartitions.size()));
						}

						LOG.info("Unassgning {} from overloaded client {}", partitionToDelete, client);
						partitionToClient.remove(partitionToDelete);
						clientToPartitions.get(client).remove(partitionToDelete);
						info.setLoad(info.getLoad() - 1);

						curator.delete().forPath("/kaboom/assignments/" + partitionToDelete);
					}
				}
			}

			// Sort the clients by percent load, then add unassigned clients to the lowest loaded client			
			{
				List<String> sortedClients = new ArrayList<>();
				Comparator<String> comparator = new Comparator<String>()
				{
					@Override
					public int compare(String a, String b)
					{
						KaBoomNodeInfo infoA = clients.get(a);
						double valA = infoA.getLoad() / infoA.getTargetLoad();

						KaBoomNodeInfo infoB = clients.get(b);
						double valB = infoB.getLoad() / infoB.getTargetLoad();

						if (valA == valB)
						{
							return 0;
						} 
						else
						{
							if (valA > valB)
							{
								return 1;
							} 
							else
							{
								return -1;
							}
						}
					}
				};
				
				sortedClients.addAll(clients.keySet());

				for (String partition : partitionToHost.keySet())
				{
					// If it's already assigned, skip it
					
					if (partitionToClient.containsKey(partition))
					{
						continue;
					}
					
					Collections.sort(sortedClients, comparator);

					/**
					 * 
					 * Iterate through the list until we find either a local client below capacity, or we reach the ones that are 
					 * above capacity.  If we reach clients above capacity, then just assign it to the first node.
					 */
					
					LOG.info("Going to assign {}", partition);
					String chosenClient = null;
					
					for (String client : sortedClients)
					{
						LOG.info("- Checking {}", client);						
						KaBoomNodeInfo info = clients.get(client);						
						LOG.info("- Current load = {}, Target load =  {}", info.getLoad(), info.getTargetLoad());
						
						if (info.getLoad() >= info.getTargetLoad())
						{
							chosenClient = sortedClients.get(0);
							break;
						} 
						else
						{
							if (clients.get(client).getHostname().equals(partitionToHost.get(partition)))
							{
								chosenClient = client;
								break;
							} 
							else
							{
								continue;
							}
						}
					}
					
					if (chosenClient == null)
					{
						chosenClient = sortedClients.get(0);
					}

					LOG.info("Assigning partition {} to client {}", partition, chosenClient);

					curator
						 .create()
						 .withMode(CreateMode.PERSISTENT)
						 .forPath("/kaboom/assignments/" + partition,
							  chosenClient.getBytes(UTF8));

					List<String> parts = clientToPartitions.get(chosenClient);
					
					if (parts == null)
					{
						parts = new ArrayList<String>();
						clientToPartitions.put(chosenClient, parts);
					}
					
					parts.add(partition);

					partitionToClient.put(partition, chosenClient);

					clients.get(chosenClient).setLoad(clients.get(chosenClient).getLoad() + 1);
				}
			}

			/*
			 *  Check to see if the kafka_ready flag writer thread exists and is alive:
			 *  
			 *  If it doesn't exist or isn't running, start it.  This is designed to 
			 *  work well when the load balancer sleeps for 10 minutes after assigning 
			 *  work.  If that behavior changes then additional logic will be required
			 *  to ensure this isn't executed too often  
			 */
			
			if (readyFlagThread == null || !readyFlagThread.isAlive())
			{
				LOG.info("[ready flag writer] thread doesn't exist or is not running");				
				readyFlagWriter = new ReadyFlagWriter(config, curator);
				readyFlagWriter.addListener(this);
				readyFlagThread = new Thread(readyFlagWriter);
				readyFlagThread.start();
				LOG.info("[ready flag writer] thread created and started");
			}

			Thread.sleep(10 * 60 * 1000);
		}
	}

	/*
	 * This method is called when the ready flag writer thread finishes.  There is 
	 * currently nothing special that needs to happen, but later we may wish to have 
	 * greater control over when threads start/stop and when they need to be ran 
	 * again
	 */
	
	@Override
	public void notifyOfThreadComplete(NotifyingThread notifyingThread, Exception e)
	{
		LOG.info("[ready flag writer] thread finished");

		if (e != null)
		{
			LOG.error("[ready flag writer] Exception raised in thread: {}", e);
		}

	}

}
