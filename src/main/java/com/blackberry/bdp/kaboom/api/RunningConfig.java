/*
 * Copyright 2014 BlackBerry, Inc.
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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties({"version", "curator", "zkPath"})
public class RunningConfig extends ZkVersioned {
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
	@Getter @Setter @VersionedAttribute public long periodicHdfsFlushInterval = 30 * 1000l;	
	@Getter @Setter @VersionedAttribute public long kaboomServerSleepDurationMs = 10 * 1000;
	@Getter @Setter @VersionedAttribute public long fileCloseGraceTimeAfterExpiredMs = 30 * 1000;
	
	
	
	@Getter @Setter @VersionedAttribute public long forcedZkOffsetTsUpdateMs = 10 * 60 * 1000;
	@Getter @Setter @VersionedAttribute public String kafkaReadyFlagFilename = "_READY";
	@Getter @Setter @VersionedAttribute public int maxOpenBoomFilesPerPartition = 5;	
	@Getter @Setter @VersionedAttribute public long workerSprintDurationSeconds = 60 * 60;
	@Getter @Setter @VersionedAttribute public boolean propagateReadyFlags = false;
	@Getter @Setter @VersionedAttribute public long propagateReadyFlagFrequency = 10 * 60 *  1000;
	@Getter @Setter @VersionedAttribute public long propateReadyFlagDelayBetweenPathsMs = 0;

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
		super(startupConfig.getCurator(), startupConfig.getRunningConfigZkPath());		
	}
}
