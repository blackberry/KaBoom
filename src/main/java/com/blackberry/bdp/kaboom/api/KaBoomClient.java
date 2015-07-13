/*
 * Copyright 2015 dariens.
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

import com.blackberry.bdp.common.versioned.Util;
import com.blackberry.bdp.kaboom.KaBoomNodeInfo;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.yaml.snakeyaml.Yaml;

public class KaBoomClient {

	private final int id;
	private String hostname;
	private int weight;
	private int load;
	private double targetLoad;
	
	//private final KaBoomNodeInfo nodeInfo;

	public KaBoomClient(int id, KaBoomNodeInfo nodeInfo) {		
		this(id,			 
			 nodeInfo.getHostname(),
			 nodeInfo.getWeight(),
			 nodeInfo.getLoad(),
			 nodeInfo.getTargetLoad());
	}

	public KaBoomClient(int id, 
		 String hostname,
		 int weight,
		 int load,
		 double targetLoad) {
		
		this.id = id;
		this.hostname = hostname;
		this.weight = weight;
		this.load = load;
		this.targetLoad = targetLoad;
	}

	public static List<KaBoomClient> getAll(CuratorFramework curator, String zkPath) throws Exception {
		List<KaBoomClient> clients = new ArrayList<>();
		List<String> clientNames = Util.childrenInZkPath(curator, zkPath);
		for (String clientName : clientNames) {
			byte[] bytes = curator.getData().forPath(zkPath + "/" + clientName);
			clients.add(new KaBoomClient(Integer.parseInt(clientName), getNodeInfoFromBytes(bytes)));
		}
		return clients;
	}
	
	public static KaBoomNodeInfo getNodeInfoFromBytes(byte[] bytes) {
		Yaml yaml = new Yaml();
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		KaBoomNodeInfo nodeInfo = new KaBoomNodeInfo();
		return yaml.loadAs(in, KaBoomNodeInfo.class);	
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
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
	 * @return the load
	 */
	public int getLoad() {
		return load;
	}

	/**
	 * @param load the load to set
	 */
	public void setLoad(int load) {
		this.load = load;
	}

	/**
	 * @return the targetLoad
	 */
	public double getTargetLoad() {
		return targetLoad;
	}

	/**
	 * @param targetLoad the targetLoad to set
	 */
	public void setTargetLoad(double targetLoad) {
		this.targetLoad = targetLoad;
	}
}
