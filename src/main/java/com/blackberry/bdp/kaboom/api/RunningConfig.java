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
import com.blackberry.bdp.kaboom.StartupConfig;
import lombok.Getter;
import lombok.Setter;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties({"zkPath", "curator"})
public class RunningConfig extends ZkVersioned{

	private static final Logger LOG = LoggerFactory.getLogger(RunningConfig.class);		
	
	@Getter @Setter @VersionedAttribute public Boolean allowOffsetOverrides = false;
	@Getter @Setter @VersionedAttribute public Boolean sinkToHighWatermark = false;
	@Getter @Setter @VersionedAttribute public Boolean useTempOpenFileDirectory = true;		
	@Getter @Setter @VersionedAttribute public Boolean useNativeCompression = false;
	@Getter @Setter @VersionedAttribute public Integer readyFlagPrevHoursCheck = 24;
	@Getter @Setter @VersionedAttribute public long leaderSleepDurationMs = 10 * 60 * 1000;
	@Getter @Setter @VersionedAttribute public short compressionLevel = 6;
	@Getter @Setter @VersionedAttribute public int boomFileBufferSize = 16 * 1024;
	@Getter @Setter @VersionedAttribute public short boomFileReplicas = 3;
	@Getter @Setter @VersionedAttribute public long boomFileBlocksize = 256 * 1024 * 1024;
	@Getter @Setter @VersionedAttribute public String boomFileTmpPrefix = "_tmp_";
	@Getter @Setter @VersionedAttribute public Long periodicHdfsFlushInterval = 30 * 1000l;
	@Getter @Setter @VersionedAttribute public Long periodicFileCloseInterval = 60 * 1000l;

	/**
	 * Instantiates a default RunningConfig without any ZK interaction
	 */
	public RunningConfig() { }
	
	/**
	 * Instantiates a ZkVersioned RunningConfig from a KaBoom StartupConfig
	 * @param startupConfig
	 * @throws Exception
	 */
	public RunningConfig(StartupConfig startupConfig) throws Exception {
		this(startupConfig.getCurator(), startupConfig.getRunningConfigZkPath());		
	}

	/**
	 * Instantiates a ZkVersioned RunningConfig with a specific ZK curator/path
	 * @param curator
	 * @param zkPath
	 * @throws Exception
	 */
	public RunningConfig(CuratorFramework curator, String zkPath) throws Exception {
		super(curator, zkPath);		
	}

	/**
	 * Static provider of a ZkVersioned RunningConfig from a specific ZK curator/path
	 * 
	 * TODO: This is here and not within the interface because I can't find a way to have
	 * Jacskon create an instantiation of the implemented class within the interface.
	 * 
	 * @param curator
	 * @param zkPath
	 * @return
	 * @throws Exception
	 */	
	public static RunningConfig get(CuratorFramework curator, String zkPath) throws Exception {
		Stat stat = curator.checkExists().forPath(zkPath);
		if (stat == null) {
			throw new MissingConfigurationException("Configuration doesn't exist in ZK at " + zkPath);
		}
		byte[] jsonBytes = curator.getData().forPath(zkPath);		
		LOG.info("Attempt to retrieve {} with {}", RunningConfig.class, new String(jsonBytes));		
		RunningConfig obj = mapper.readValue(jsonBytes, RunningConfig.class);
		obj.setVersion(stat.getVersion());
		return obj;
	}	
}
