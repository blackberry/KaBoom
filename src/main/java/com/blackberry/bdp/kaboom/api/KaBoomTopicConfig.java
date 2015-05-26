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

import com.blackberry.bdp.common.versioned.MissingConfigurationException;
import com.blackberry.bdp.common.versioned.VersionedAttribute;
import com.blackberry.bdp.common.versioned.ZkVersioned;
import java.util.ArrayList;
import lombok.Getter;
import lombok.Setter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaBoomTopicConfig extends ZkVersioned{
	private static final Logger LOG = LoggerFactory.getLogger(KaBoomTopicConfig.class);
	
	@Getter @Setter @VersionedAttribute public String id = "<NEW TOPIC NAME";
	@Getter @Setter @VersionedAttribute public String hdfsRootDir = "/path/to/some/dir";
	@Getter @Setter @VersionedAttribute public String proxyUser = "<SOMEUSER>";
	@Getter @Setter @VersionedAttribute public String defaultDirectory = "data";	
	@Getter @Setter @VersionedAttribute public ArrayList<TopicFilter> filterSet = new ArrayList<>();
	
	/**
	 * Instantiates a default KaBoomTopicConfig without any ZK interaction
	 */
	public KaBoomTopicConfig() { }
	
	/**
	 * Instantiates a ZkVersioned KaBoomTopicConfig with a specific ZK curator/path
	 * @param curator
	 * @param zkPath
	 * @throws Exception
	 */
	public KaBoomTopicConfig(CuratorFramework curator, String zkPath) throws Exception {
		super(curator, zkPath);		
	}	
	
	/**
	 * Static provider of a ZkVersioned KaBoomTopicConfig from a specific ZK curator/path
	 * 
	 * TODO: This is here and not within the interface because I can't find a way to have
	 * Jacskon create an instantiation of the implemented class within the abstract class.
	 * 
	 * @param curator
	 * @param zkPath
	 * @return
	 * @throws Exception
	 */	
	public static KaBoomTopicConfig get(CuratorFramework curator, String zkPath) throws Exception {
		Stat stat = curator.checkExists().forPath(zkPath);
		if (stat == null) {
			throw new MissingConfigurationException("Configuration doesn't exist in ZK at " + zkPath);
		}
		byte[] jsonBytes = curator.getData().forPath(zkPath);
		LOG.info("Attempt to retrieve {} with {}", KaBoomTopicConfig.class, new String(jsonBytes));
		KaBoomTopicConfig obj = mapper.readValue(jsonBytes, KaBoomTopicConfig.class);
		obj.setVersion(stat.getVersion());
		return obj;
	}
	
	public static ArrayList<KaBoomTopicConfig> getAll(CuratorFramework curator, String zkPathTopicRoot) throws Exception {
		Stat stat = curator.checkExists().forPath(zkPathTopicRoot);
		if (stat == null) {
			throw new MissingConfigurationException("Configuration doesn't exist in ZK at " + zkPathTopicRoot);
		}
		ArrayList<KaBoomTopicConfig> topicConfigs = new ArrayList<>();
		
		for (String topicName : Util.childrenInZkPath(curator, zkPathTopicRoot)) {
			String configPath = String.format("%s/%s", zkPathTopicRoot, topicName);
			Stat configStat = curator.checkExists().forPath(configPath);
			byte[] jsonBytes = curator.getData().forPath(configPath);				
			if (jsonBytes.length != 0) {
				LOG.info("Attempt to retrieve {} with {}", KaBoomTopicConfig.class, new String(jsonBytes));		
				KaBoomTopicConfig config = mapper.readValue(jsonBytes, KaBoomTopicConfig.class);
				config.setVersion(configStat.getVersion());
				config.setCurator(curator);
				config.setZkPath(configPath);
				config.id = topicName;
				topicConfigs.add(config);
			} else {
				// We don't have any bytes in the value of the znode, this is how versions
				// earlier than v0.8.0 stored partition data in children and there wasn't any 
				// actual value stored in the znode, so just return a default configuration
				KaBoomTopicConfig config = new KaBoomTopicConfig();
				config.id = topicName;
				topicConfigs.add(config);					
			}				
		}		
		return topicConfigs;
	}	
}
