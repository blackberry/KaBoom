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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.common.threads.NotifyingThread;
import com.blackberry.bdp.common.threads.ThreadCompleteListener;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Leader extends LeaderSelectorListenerAdapter implements ThreadCompleteListener
{
	private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
	protected static final Charset UTF8 = Charset.forName("UTF-8");
	protected static final Random rand = new Random();

	private ReadyFlagWriter readyFlagWriter;
	private Thread readyFlagThread;
	final protected KaboomConfiguration config;
	CuratorFramework curator;
	
	Map<String, String> partitionToHost = new HashMap<>();
	Map<String, List<String>> hostToPartition = new HashMap<>();
	Map<String, KaBoomNodeInfo> clientIdToNodeInfo = new HashMap<>();
	Map<String, String> hostnameToClientId = new HashMap<>();			
	Map<String, List<String>> clientToPartitions = new HashMap<>();
	Map<String, String> partitionToClient = new HashMap<>();			
	List<String> topics = new ArrayList<>();	
	
	public Leader(KaboomConfiguration config)
	{
		this.config = config;
	}
	
	protected abstract void run_balancer() throws Exception;

	@Override
	public void takeLeadership(CuratorFramework curator) throws Exception
	{
		this.curator = curator;
		
		/**
		 * Chill for 30 seconds after the election, give whatever caused it a few moments to potentially subside
		 */

		LOG.info("A new leader has been elected: kaboom.id={}", config.getKaboomId());
	
		Thread.sleep(30 * 1000);

		while (true)
		{			
			// (Re-)build the data structures and maps that our leader needs to make balancing decisions
			
			topics = new ArrayList<>();			
			StateUtils.readTopicsFromZooKeeper(config.getKafkaZkConnectionString(), topics, config.getTopicToSupportedStatus());
			
			partitionToHost = new HashMap<>();
			hostToPartition = new HashMap<>();
			StateUtils.getPartitionHosts(config.getKafkaSeedBrokers(), topics, partitionToHost, hostToPartition);
			
			clientIdToNodeInfo = new HashMap<>();
			hostnameToClientId = new HashMap<>();
			StateUtils.getActiveClients(curator, clientIdToNodeInfo, hostnameToClientId);
			
			clientToPartitions = new HashMap<>();
			StateUtils.calculateLoad(partitionToHost, clientIdToNodeInfo, clientToPartitions);
			
			LOG.info("Found a total of {} supported topics in ZooKeeper", topics.size());
			
			/**
			 * This an an abstract KaBoom leader that must be extended and implemented by an actual 
			 * load balancer.  There are, however, tasks that must always be performed regardless of 
			 * a balancer implementation.  Tasks such as removing assignments for non-existent clients
			 * and running the Kafka ready flag writer, etc.  Let's do those, and then call the balancer...
			 */
			
			// Delete any assignments for topics that are not supported (configured) for KaBoom
			
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
							if (!config.getTopicToSupportedStatus().containsKey(topic)
								 || config.getTopicToSupportedStatus().get(topic) == false)
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

			// Build up our clientToPartitions and partitionsToClients, while 
			// deleting any assignments for clientIdToNodeInfo that are not connected
			
			partitionToClient = new HashMap<>();			
			
			for (String partition : partitionToHost.keySet())
			{
				Stat stat = curator.checkExists().forPath("/kaboom/assignments/" + partition);
				
				if (stat != null)
				{	
					String client = new String(curator.getData().forPath("/kaboom/assignments/" + partition), UTF8);
					
					if (clientIdToNodeInfo.containsKey(client))
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
					}
				}
			}
			
			// Call the load balancer's implementation of run_balancer()
			
			try
			{
				run_balancer();
			}
			catch (Exception e)
			{
				LOG.error("The load balancer raised an exception: ", e);
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
				readyFlagWriter = new ReadyFlagWriter(config);
				readyFlagWriter.addListener(this);
				readyFlagThread = new Thread(readyFlagWriter);
				readyFlagThread.start();
				LOG.info("[ready flag writer] thread created and started");
			}

			Thread.sleep(config.getLeaderSleepDurationMs());
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
		if (e != null)
		{
			LOG.error("[ready flag writer] Exception raised in thread: {}", e);
		}
	}
}
