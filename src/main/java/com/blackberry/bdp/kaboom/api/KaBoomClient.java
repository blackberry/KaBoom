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
import com.blackberry.bdp.kaboom.StartupConfig;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaBoomClient extends ZkVersioned {

	private int id;
	private String hostname;
	private int weight;
	private int partitionLoad;
	private double targetPartitionLoad;
	private double targetFlagPropagatorLoad = 0.0;
	private final List<String> assignedFlagPropagatorTopics = new ArrayList<>();
	private final List<KaBoomPartition> assignedPartitions = new ArrayList<>();
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private static final Logger LOG = LoggerFactory.getLogger(KaBoomClient.class);

	/**
	 * Instantiates a default RunningConfig without any ZK interaction
	 */
	public KaBoomClient() {
	}

	/**
	 * Instantiates a ZkVersioned KaBoomClient from ZK Path
	 * @param curator
	 * @param zkPath
	 * @throws Exception
	 */
	public KaBoomClient(CuratorFramework curator, String zkPath) throws Exception {
		super(curator, zkPath);
	}

	public void calculateTargetPartitionLoad(int totalPartitions, int totalWeight) {
		targetPartitionLoad = (totalPartitions * (1.0 * weight / totalWeight));
		LOG.info("Client ID {} target partition load is {}", id, targetPartitionLoad);
	}

	public void calculateFlagPropagatorTargetLoad(int totalPaths, int totalWeight) {		
		targetFlagPropagatorLoad = (totalPaths * (1.0 * weight / totalWeight));
		LOG.info("Client ID {} target flag propagator load is {}", id, targetFlagPropagatorLoad);
	}

	public boolean overloaded() {
		return assignedPartitions.size() > targetPartitionLoad + 1;
	}

	public List<String> getAssignments(StartupConfig config, String zkRootPath) throws Exception {
		List<String> assignments = new ArrayList<>();
		for (String assignment : config.getKaBoomCurator().getChildren().forPath(zkRootPath)) {
			String assignee;
			try {
				assignee = new String(config.getKaBoomCurator().getData().forPath(config.zkPathPartitionAssignment(assignment)), UTF8);
				if (assignee.equals(Integer.toString(id))) {
					assignments.add(assignment);
				}
			} catch (KeeperException.NoNodeException nne) {
				LOG.warn("The weird 'NoNodeException' has been raised, let's just continue and it'll retry");
				continue;
			}
		}
		return assignments;
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
	public double getTargetPartitionLoad() {
		return targetPartitionLoad;
	}

	/**
	 * @return the targetFlagPropagatorLoad
	 */
	public double getTargetFlagPropagatorLoad() {
		return targetFlagPropagatorLoad;
	}

	/**
	 * @param flagPropagatorTargetLoad the targetFlagPropagatorLoad to set
	 */
	public void setTargetFlagPropagatorLoad(double flagPropagatorTargetLoad) {
		this.targetFlagPropagatorLoad = flagPropagatorTargetLoad;
	}

	/**
	 * @return the assignedFlagPropagatorTopics
	 */
	public List<String> getAssignedFlagPropagatorTopics() {
		return assignedFlagPropagatorTopics;
	}

	/**
	 * @return the assignedPartitions
	 */
	public List<KaBoomPartition> getAssignedPartitions() {
		return assignedPartitions;
	}

}
