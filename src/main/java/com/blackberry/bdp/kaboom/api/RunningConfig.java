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

import com.blackberry.bdp.common.annotations.VersionedAttribute;
import com.blackberry.bdp.common.annotations.VersionedComparable;
import com.blackberry.bdp.common.annotations.MissingConfigurationException;
import com.blackberry.bdp.kaboom.StartupConfig;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunningConfig extends VersionedComparable{

	private static final Logger LOG = LoggerFactory.getLogger(RunningConfig.class);
	
	private final static ObjectMapper mapper = new ObjectMapper();
	
	@VersionedAttribute private Boolean allowOffsetOverrides = false;
	@VersionedAttribute private Boolean sinkToHighWatermark = false;
	@VersionedAttribute private Boolean useTempOpenFileDirectory = true;		
	@VersionedAttribute private Boolean useNativeCompression = false;
	@VersionedAttribute private Integer readyFlagPrevHoursCheck = 24;
	@VersionedAttribute private long leaderSleepDurationMs = 10 * 60 * 1000;
	@VersionedAttribute private short compressionLevel = 6;
	@VersionedAttribute private int boomFileBufferSize = 16 * 1024;
	@VersionedAttribute private short boomFileReplicas = 3;
	@VersionedAttribute private long boomFileBlocksize = 256 * 1024 * 1024;
	@VersionedAttribute private String boomFileTmpPrefix = "_tmp_";
	@VersionedAttribute private Long periodicHdfsFlushInterval = 30 * 1000l;
	@VersionedAttribute private Long periodicFileCloseInterval = 60 * 1000l;

	/**
	 * Returns a default RunningConfig without any ZK interaction
	 */
	public RunningConfig() {
		
	}
	
	/**
	 * Returns a VersionedComparable RunningConfig from a KaBoom StartupConfig
	 * @param startupConfig
	 * @throws Exception
	 */
	public RunningConfig(StartupConfig startupConfig) throws Exception {
		this(startupConfig.getCurator(), startupConfig.getRunningConfigZkPath());		
	}

	/**
	 * Returns a VersionedComparable RunningConfig from a specific ZK curator/path
	 * @param curator
	 * @param zkPath
	 * @throws Exception
	 */
	public RunningConfig(CuratorFramework curator, String zkPath) throws Exception {
		super(curator, zkPath);
		reload();
	}

	/**
	 * Static provider of a VersionedComparable RunningConfig from a specific ZK curator/path
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
		RunningConfig  newRunningConfig = mapper.readValue(curator.getData().forPath(zkPath), RunningConfig.class);
		newRunningConfig.version = stat.getVersion();
		return newRunningConfig;		
	}	
	
	@Override
	public int getVersion() {
		return this.version;
	}
	
	/**
	 * @return the allowOffsetOverrides
	 */
	public Boolean getAllowOffsetOverrides() {
		return allowOffsetOverrides;
	}

	/**
	 * @param allowOffsetOverrides the allowOffsetOverrides to set
	 */
	public void setAllowOffsetOverrides(Boolean allowOffsetOverrides) {
		this.allowOffsetOverrides = allowOffsetOverrides;
	}

	/**
	 * @return the sinkToHighWatermark
	 */
	public Boolean getSinkToHighWatermark() {
		return sinkToHighWatermark;
	}

	/**
	 * @param sinkToHighWatermark the sinkToHighWatermark to set
	 */
	public void setSinkToHighWatermark(Boolean sinkToHighWatermark) {
		this.sinkToHighWatermark = sinkToHighWatermark;
	}

	/**
	 * @return the useTempOpenFileDirectory
	 */
	public Boolean getUseTempOpenFileDirectory() {
		return useTempOpenFileDirectory;
	}

	/**
	 * @param useTempOpenFileDirectory the useTempOpenFileDirectory to set
	 */
	public void setUseTempOpenFileDirectory(Boolean useTempOpenFileDirectory) {
		this.useTempOpenFileDirectory = useTempOpenFileDirectory;
	}

	/**
	 * @return the useNativeCompression
	 */
	public Boolean getUseNativeCompression() {
		return useNativeCompression;
	}

	/**
	 * @param useNativeCompression the useNativeCompression to set
	 */
	public void setUseNativeCompression(Boolean useNativeCompression) {
		this.useNativeCompression = useNativeCompression;
	}

	/**
	 * @return the readyFlagPrevHoursCheck
	 */
	public Integer getReadyFlagPrevHoursCheck() {
		return readyFlagPrevHoursCheck;
	}

	/**
	 * @param readyFlagPrevHoursCheck the readyFlagPrevHoursCheck to set
	 */
	public void setReadyFlagPrevHoursCheck(Integer readyFlagPrevHoursCheck) {
		this.readyFlagPrevHoursCheck = readyFlagPrevHoursCheck;
	}

	/**
	 * @return the leaderSleepDurationMs
	 */
	public long getLeaderSleepDurationMs() {
		return leaderSleepDurationMs;
	}

	/**
	 * @param leaderSleepDurationMs the leaderSleepDurationMs to set
	 */
	public void setLeaderSleepDurationMs(long leaderSleepDurationMs) {
		this.leaderSleepDurationMs = leaderSleepDurationMs;
	}

	/**
	 * @return the compressionLevel
	 */
	public short getCompressionLevel() {
		return compressionLevel;
	}

	/**
	 * @param defaultCompressionLevel the compressionLevel to set
	 */
	public void setCompressionLevel(short defaultCompressionLevel) {
		this.compressionLevel = defaultCompressionLevel;
	}

	/**
	 * @return the boomFileBufferSize
	 */
	public int getBoomFileBufferSize() {
		return boomFileBufferSize;
	}

	/**
	 * @param boomFileBufferSize the boomFileBufferSize to set
	 */
	public void setBoomFileBufferSize(int boomFileBufferSize) {
		this.boomFileBufferSize = boomFileBufferSize;
	}

	/**
	 * @return the boomFileReplicas
	 */
	public short getBoomFileReplicas() {
		return boomFileReplicas;
	}

	/**
	 * @param boomFileReplicas the boomFileReplicas to set
	 */
	public void setBoomFileReplicas(short boomFileReplicas) {
		this.boomFileReplicas = boomFileReplicas;
	}

	/**
	 * @return the boomFileBlocksize
	 */
	public long getBoomFileBlocksize() {
		return boomFileBlocksize;
	}

	/**
	 * @param boomFileBlocksize the boomFileBlocksize to set
	 */
	public void setBoomFileBlocksize(long boomFileBlocksize) {
		this.boomFileBlocksize = boomFileBlocksize;
	}

	/**
	 * @return the boomFileTmpPrefix
	 */
	public String getBoomFileTmpPrefix() {
		return boomFileTmpPrefix;
	}

	/**
	 * @param boomFileTmpPrefix the boomFileTmpPrefix to set
	 */
	public void setBoomFileTmpPrefix(String boomFileTmpPrefix) {
		this.boomFileTmpPrefix = boomFileTmpPrefix;
	}

	/**
	 * @return the periodicHdfsFlushInterval
	 */
	public Long getPeriodicHdfsFlushInterval() {
		return periodicHdfsFlushInterval;
	}

	/**
	 * @param periodicHdfsFlushInterval the periodicHdfsFlushInterval to set
	 */
	public void setPeriodicHdfsFlushInterval(Long periodicHdfsFlushInterval) {
		this.periodicHdfsFlushInterval = periodicHdfsFlushInterval;
	}

	/**
	 * @return the periodicFileCloseInterval
	 */
	public Long getPeriodicFileCloseInterval() {
		return periodicFileCloseInterval;
	}

	/**
	 * @param periodicFileCloseInterval the periodicFileCloseInterval to set
	 */
	public void setPeriodicFileCloseInterval(Long periodicFileCloseInterval) {
		this.periodicFileCloseInterval = periodicFileCloseInterval;
	}

}
