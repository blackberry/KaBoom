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

import com.blackberry.bdp.common.versioned.VersionedAttribute;
import com.blackberry.bdp.common.versioned.ZkVersioned;
import com.blackberry.bdp.kaboom.StartupConfig;
import lombok.Getter;
import lombok.Setter;

import org.apache.curator.framework.CuratorFramework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunningConfig extends ZkVersioned{

	private static final Logger LOG = LoggerFactory.getLogger(RunningConfig.class);		
	
	@Getter @Setter @VersionedAttribute private Boolean allowOffsetOverrides = false;
	@Getter @Setter @VersionedAttribute private Boolean sinkToHighWatermark = false;
	@Getter @Setter @VersionedAttribute private Boolean useTempOpenFileDirectory = true;		
	@Getter @Setter @VersionedAttribute private Boolean useNativeCompression = false;
	@Getter @Setter @VersionedAttribute private Integer readyFlagPrevHoursCheck = 24;
	@Getter @Setter @VersionedAttribute private long leaderSleepDurationMs = 10 * 60 * 1000;
	@Getter @Setter @VersionedAttribute private short compressionLevel = 6;
	@Getter @Setter @VersionedAttribute private int boomFileBufferSize = 16 * 1024;
	@Getter @Setter @VersionedAttribute private short boomFileReplicas = 3;
	@Getter @Setter @VersionedAttribute private long boomFileBlocksize = 256 * 1024 * 1024;
	@Getter @Setter @VersionedAttribute private String boomFileTmpPrefix = "_tmp_";
	@Getter @Setter @VersionedAttribute private Long periodicHdfsFlushInterval = 30 * 1000l;
	@Getter @Setter @VersionedAttribute private Long periodicFileCloseInterval = 60 * 1000l;

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
	 * Instantiates a ZkVersioned RunningConfig from a specific ZK curator/path
	 * @param curator
	 * @param zkPath
	 * @throws Exception
	 */
	public RunningConfig(CuratorFramework curator, String zkPath) throws Exception {
		super(curator, zkPath);
		reload();
	}

	/**
	 * Static provider of a ZkVersioned RunningConfig from a specific ZK curator/path
	 * @param curator
	 * @param zkPath
	 * @return
	 * @throws Exception
	 */
	public static RunningConfig get(CuratorFramework curator, String zkPath) 
		 throws Exception {
		return (RunningConfig) ZkVersioned.get(curator, zkPath);
	}	
	
}
