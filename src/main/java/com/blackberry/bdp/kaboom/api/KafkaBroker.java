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
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBroker {

	private static Charset UTF8 = Charset.forName("UTF-8");	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaBroker.class);
	
	private int id;
	private int jmx_port;
	private long timestamp;
	private String host;
	private int version;
	private int port;
	
	public KafkaBroker() { }
	
	public KafkaBroker(int jmxPort, 
		 long timestamp,
		 String host,
		 int version,
		 int port) {
		
		this.jmx_port= jmxPort;
		this.timestamp = timestamp;
		this.host = host;
		this.version = version;
		this.port = port;
	}

	public static List<KafkaBroker> getAll(CuratorFramework curator, 
		 String zkPath) throws Exception {
		
		List<KafkaBroker> brokers = new ArrayList<>();
		List<String> brokerIds = Util.childrenInZkPath(curator, zkPath);
		for (String brokerId : brokerIds) {
			byte[] bytes = curator.getData().forPath(zkPath + "/" + brokerId);
			String brokerJson = new String(bytes, UTF8);			
			KafkaBroker broker;
			ObjectMapper mapper = new ObjectMapper();			
			try {
				broker = mapper.readValue(brokerJson, KafkaBroker.class);			
				broker.id = Integer.parseInt(brokerId);
				brokers.add(broker);				
			}
			catch (IOException | NumberFormatException e) {
				LOG.error("Failed to convert {}", brokerJson);
				throw e;
			}
		}
		return brokers;
	}
	
	public static HashMap<String, KafkaPartition> getUsefulMappings(
		 List<KafkaTopic> topics,
		 List<KafkaBroker>  brokers,
		 HashMap<Integer, KafkaBroker> brokerIdToBroker,
		 HashMap<String, KafkaPartition> leaderIdToKafkaPartition,
		 HashMap<String, KafkaPartition> partitionIdToKafkaPartition) {
		
		for (KafkaBroker broker : brokers) {
			brokerIdToBroker.put(broker.getId(), broker);
		}
		// Build the leader host to Kafka partition map
		HashMap<String, KafkaPartition> leaderToPart = new HashMap<>();
		for (KafkaTopic topic : topics) {
			for (KafkaPartition partition : topic.getPartitions()) {
				leaderToPart.put(brokerIdToBroker.get(partition.getLeader()).getHost(), partition);				
				String partitionId = String.format("%s-%d", topic, partition.getPartitionId());				
				partitionIdToKafkaPartition.put(partitionId, partition);
			}				
		}
		return leaderToPart;
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
	 * @return the jmx_port
	 */
	public int getJmx_port() {
		return jmx_port;
	}

	/**
	 * @param jmx_port the jmx_port to set
	 */
	public void setJmx_port(int jmx_port) {
		this.jmx_port = jmx_port;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * @return the version
	 */
	public int getVersion() {
		return version;
	}

	/**
	 * @param version the version to set
	 */
	public void setVersion(int version) {
		this.version = version;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}
	
}