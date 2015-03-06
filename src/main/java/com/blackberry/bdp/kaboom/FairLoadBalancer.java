/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blackberry.bdp.kaboom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The fair load balancer
 * 
 * Partitions are assigned based off a weighted workload.  
 * 
 * The default weighting is based on how many cores each client has.
 * 
 * There will be a preference for local work while the client is under-loaded.
 * 
 * However, as soon as the client is over-loaded, work is arbitrarily assigned.
 * 
 * @author dariens
 */
public class FairLoadBalancer extends Leader
{
	
	private static final Logger LOG = LoggerFactory.getLogger(FairLoadBalancer.class);

	public FairLoadBalancer(KaboomConfiguration config)
	{		
		super(config);
		LOG.info("The fair load balancer has been instantiated");
	}
	
	@Override
	protected void run_balancer()
	{
		// For every client, determine if it's doing too much work and remove assignments (remote ones first)

		for (Map.Entry<String, KaBoomNodeInfo> e : clients.entrySet())
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

					try
					{
						curator.delete().forPath("/kaboom/assignments/" + partitionToDelete);
					}
					catch (Exception ex)
					{
						LOG.error("Failed to delete assignment {}:", "/kaboom/assignments/" + partitionToDelete, ex);
					}
					
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
					}
				}

				if (chosenClient == null)
				{
					chosenClient = sortedClients.get(0);
				}

				LOG.info("Assigning partition {} to client {}", partition, chosenClient);

				try
				{
					curator
						 .create()
						 .withMode(CreateMode.PERSISTENT)
						 .forPath("/kaboom/assignments/" + partition,chosenClient.getBytes(UTF8));					
				}
				catch (Exception e)
				{
					LOG.error("Failed to create assignment {}", "/kaboom/assignments/" + partition,chosenClient, e);
				}

				List<String> parts = clientToPartitions.get(chosenClient);

				if (parts == null)
				{
					parts = new ArrayList<>();
					clientToPartitions.put(chosenClient, parts);
				}

				parts.add(partition);

				partitionToClient.put(partition, chosenClient);

				clients.get(chosenClient).setLoad(clients.get(chosenClient).getLoad() + 1);
			}
		}		
	}
	
}
