package com.blackberry.kaboom;

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

import com.blackberry.common.threads.NotifyingThread;
import com.blackberry.common.threads.ThreadCompleteListener;

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
			Map<String, String> partitionToHost = new HashMap<String, String>();
			Map<String, List<String>> hostToPartition = new HashMap<String, List<String>>();
			final Map<String, KaBoomNodeInfo> clients = new HashMap<String, KaBoomNodeInfo>();
			Map<String, List<String>> clientToPartitions = new HashMap<String, List<String>>();
			Map<String, String> partitionToClient = new HashMap<String, String>();
			List<String> topics = new ArrayList<String>();

			// Get a full set of metadata from Kafka
			StateUtils.readTopicsFromZooKeeper(config.getKafkaZkConnectionString(), topics);

			// Map partition to host and host to partition
			StateUtils.getPartitionHosts(config.getKafkaSeedBrokers(), topics, partitionToHost, hostToPartition);

			// Get a list of active clients from zookeeper
			StateUtils.getActiveClients(curator, clients);

			// Get a list of current assignments
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
							parts = new ArrayList<String>();
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

			// If any node is over its target by at least one, then unassign partitions until it is at or below its target
			
			for (Entry<String, KaBoomNodeInfo> e : clients.entrySet())
			{
				String client = e.getKey();
				KaBoomNodeInfo info = e.getValue();

				if (info.getLoad() >= info.getTargetLoad() + 1)
				{
					List<String> localPartitions = new ArrayList<String>();
					List<String> remotePartitions = new ArrayList<String>();

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
				List<String> sortedClients = new ArrayList<String>();
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
