/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blackberry.bdp.kaboom;

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
public class LocalLoadBalancer extends Leader
{
	
	private static final Logger LOG = LoggerFactory.getLogger(EvenLoadBalancer.class);

	public LocalLoadBalancer(StartupConfig config)
	{
		super(config);
		LOG.info("The local load balancer has been instantiated");
	}

	/**
	 * Iterate through all partitions...
	 * 
	 * Unassign non-local assignment and assign unassigned partitions to the local client 
	 */
	
	@Override
	protected void run_balancer()
	{	
		for (String partition : partitionToHost.keySet())
		{
			// Are we assigned?
			
			String leaderClientId = hostnameToClientId.get(partitionToHost.get(partition));
			
			if (partitionToClient.containsKey(partition))
			{
				// Is the assigned client the leader of the partition?
				
				if (partitionToClient.get(partition).equals(leaderClientId))
				{
					LOG.info("{} is already assigned to the partition leader");
				}
				else
				{
					LOG.warn("partition {} is assigned to {} however {} is the leader", partition, partitionToClient.get(partition), leaderClientId);
					
					try
					{					
						curator.delete().forPath("/kaboom/assignments/" + partition);

						LOG.info("{} non-local assignment to {} deleted from ZK ", partition, partitionToClient.get(partition));

						curator
							 .create()
							 .withMode(CreateMode.PERSISTENT)
							 .forPath("/kaboom/assignments/" + partition, leaderClientId.getBytes(UTF8));					
						
						partitionToClient.put(partition, leaderClientId);
						
						LOG.info("{} previously assigned to remote client now assigned to local client: {}", partition, leaderClientId);
					}
					catch (Exception e)
					{
						LOG.error("[{}] error trying to correct non-local assignment", partition);
					}
				}
			}
			
			// Nope, not assigned, just assign it to the local client
			else
			{
				try
				{	
					curator
						 .create()
						 .withMode(CreateMode.PERSISTENT)
						 .forPath("/kaboom/assignments/" + partition, leaderClientId.getBytes(UTF8));					

					partitionToClient.put(partition, leaderClientId);

					LOG.info("{} assigned to local client: {}", partition, leaderClientId);
				}
				catch (Exception e)
				{
					LOG.error("[{}] error trying to assign to {}", partition, leaderClientId);
				}
			}
		}
			
	}
}
