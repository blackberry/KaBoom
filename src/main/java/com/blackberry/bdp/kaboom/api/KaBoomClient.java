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
package com.blackberry.bdp.kaboom.api;

import com.blackberry.bdp.common.versioned.ZkVersioned;
import org.apache.curator.framework.CuratorFramework;

public class KaBoomClient extends ZkVersioned{

	private int id;
	private String hostname;
	private int weight;
	private int partitionLoad;
	private double targetPartitionLoad;
	private int flagPropagatorLoad = 0;
	private double targetFlagPropagatorLoad = 0.0;
	
	/**
	 * Instantiates a default RunningConfig without any ZK interaction
	 */
	public KaBoomClient() { }
	
	/**
	 * Instantiates a ZkVersioned KaBoomClient from ZK Path
	 * @param curator
	 * @param zkPath
	 * @throws Exception
	 */
	public KaBoomClient(CuratorFramework curator, String zkPath) throws Exception {
		super(curator, zkPath);
	}	
	
	public boolean isConnected(CuratorFramework curator, String zkRootPathClients) 
		 throws Exception {
		String zkPath = String.format("%s/%d", zkRootPathClients, getId());
		return curator.checkExists().forPath(zkPath) != null;
	}
	
	public static boolean isConnected(CuratorFramework curator, String zkRootPathClients,  int clientId) 
		 throws Exception {
		String zkPath = String.format("%s/%d", zkRootPathClients, clientId);
		return curator.checkExists().forPath(zkPath) != null;
	}
	
	public void calculateTargetLoad(int totalPartitions, int totalWeight) {
		targetPartitionLoad = (totalPartitions * (1.0 * weight/ totalWeight));
	}
	
	public void calculateFlagPropagatorTargetLoad(int totalPaths, int totalWeight) {
		targetFlagPropagatorLoad = (totalPaths * (1.0 * weight/ totalWeight));
	}

	public int incrementPartitionLoad(int amount) {
		partitionLoad += amount;
		return partitionLoad;
	}

	public int incrementFlagPropagatorLoad(int amount) {
		flagPropagatorLoad += amount;
		return flagPropagatorLoad;
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the hostname
	 */
	public String getHostname() {
		return hostname;
	}

	/**
	 * @param hostname the hostname to set
	 */
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	/**
	 * @return the weight
	 */
	public int getWeight() {
		return weight;
	}

	/**
	 * @param weight the weight to set
	 */
	public void setWeight(int weight) {
		this.weight = weight;
	}

	/**
	 * @return the partitionLoad
	 */
	public int getPartitionLoad() {
		return partitionLoad;
	}

	/**
	 * @param load the partitionLoad to set
	 */
	public void setPartitionLoad(int load) {
		this.partitionLoad = load;
	}

	/**
	 * @return the targetPartitionLoad
	 */
	public double getTargetLoad() {
		return targetPartitionLoad;
	}

	/**
	 * @param targetLoad the targetPartitionLoad to set
	 */
	public void setTargetLoad(double targetLoad) {
		this.targetPartitionLoad = targetLoad;
	}

	/**
	 * @return the flagPropagatorLoad
	 */
	public int getFlagPropagatorLoad() {
		return flagPropagatorLoad;
	}

	/**
	 * @param flagPropagatorLoad the flagPropagatorLoad to set
	 */
	public void setFlagPropagatorLoad(int flagPropagatorLoad) {
		this.flagPropagatorLoad = flagPropagatorLoad;
	}

	/**
	 * @return the targetFlagPropagatorLoad
	 */
	public double getFlagPropagatorTargetLoad() {
		return targetFlagPropagatorLoad;
	}

	/**
	 * @param flagPropagatorTargetLoad the targetFlagPropagatorLoad to set
	 */
	public void setFlagPropagatorTargetLoad(double flagPropagatorTargetLoad) {
		this.targetFlagPropagatorLoad = flagPropagatorTargetLoad;
	}

}
