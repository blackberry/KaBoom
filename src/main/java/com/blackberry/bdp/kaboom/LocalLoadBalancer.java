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

	public LocalLoadBalancer(KaboomConfiguration config)
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
			
			if (partitionToClient.containsKey(partition))
			{
				// Is the assigned client already that partitions leader?
				
				if (partitionToClient.get(partition).equals(partitionToHost.get(partition)))
				{
					LOG.info("{} is already assigned to the partition leader");
				}
				else
				{
					LOG.warn("{} is not assigned to the partiton leader", partition);
					
					try
					{					
						curator.delete().forPath("/kaboom/assignments/" + partition);

						LOG.info("{} non-local assignment to {} deleted from ZK ", partition, partitionToClient.get(partition));

						curator
							 .create()
							 .withMode(CreateMode.PERSISTENT)
							 .forPath("/kaboom/assignments/" + partition, partitionToHost.get(partition).getBytes(UTF8));					
						
						partitionToClient.put(partition, partitionToHost.get(partition));
						
						LOG.info("{} previously assigned to remote client now assigned to local client: {}", partition, partitionToClient.get(partition));
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
						 .forPath("/kaboom/assignments/" + partition, partitionToHost.get(partition).getBytes(UTF8));					

					partitionToClient.put(partition, partitionToHost.get(partition));

					LOG.info("{} assigned to local client: {}", partition, partitionToClient.get(partition));
				}
				catch (Exception e)
				{
					LOG.error("[{}] error trying to assign to {}", partition, partitionToHost.get(partition));
				}
			}
		}
			
	}
}
