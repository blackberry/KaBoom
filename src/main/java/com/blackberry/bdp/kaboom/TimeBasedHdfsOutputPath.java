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
package com.blackberry.bdp.kaboom;

import com.blackberry.bdp.common.conversion.Converter;
import com.blackberry.bdp.kaboom.api.KaBoomTopicConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dariens
 */
public class TimeBasedHdfsOutputPath {

	private static final Logger LOG = LoggerFactory.getLogger(TimeBasedHdfsOutputPath.class);

	private final StartupConfig config;
	private final KaBoomTopicConfig topicConfig;
	private final String topic;
	private final int partition;
	private final FileSystem fileSystem;
	private final String partitionId;

	private final Map<Long, OutputFile> outputFileMap = new HashMap<>();

	public TimeBasedHdfsOutputPath(StartupConfig kaboomConfig, KaBoomTopicConfig topicConfig, int partition) {
		this.config = kaboomConfig;
		this.topicConfig = topicConfig;
		this.partition = partition;
		this.topic = topicConfig.getId();
		this.fileSystem = config.authenticatedFsForProxyUser(topicConfig.getProxyUser());
		this.partitionId = String.format("%s-%d", topic, partition);
	}

	public FastBoomWriter getBoomWriter(long shiftNumber, long ts, String filename) throws IOException, Exception {
		long requestedStartTime = ts - ts % (this.config.getRunningConfig().getWorkerShiftDurationSeconds() * 1000);
		OutputFile requestedOutputFile = outputFileMap.get(requestedStartTime);
		if (requestedOutputFile == null) {
			requestedOutputFile = new OutputFile(shiftNumber, filename, requestedStartTime);
			outputFileMap.put(requestedStartTime, requestedOutputFile);
			if (outputFileMap.size() > config.getRunningConfig().getMaxOpenBoomFilesPerPartition()) {
				long oldestTs = getOldestLastUsedTimestamp();
				try {
					OutputFile oldestOutputFile = outputFileMap.get(oldestTs);
					if (oldestOutputFile == null) {
						throw new Exception("Attempt at finding LRU output file returned null");
					}
					oldestOutputFile.close();
					LOG.info("[{}] Over max open boom file limit ({}/{}) closing LRU boom file: {}",
						 partitionId,
						 outputFileMap.size(),
						 config.getRunningConfig().getMaxOpenBoomFilesPerPartition(),
						 oldestOutputFile.openFilePath);
					outputFileMap.remove(oldestTs);
				} catch (Exception e) {
					LOG.error("[{}] Failed to close off oldest boom writer: ", partitionId, e);
				}
			}
		}
		requestedOutputFile.lastUsedTimestmap = System.currentTimeMillis();
		return requestedOutputFile.getBoomWriter();
	}

	private long getOldestLastUsedTimestamp() {
		long oldestTs = outputFileMap.entrySet().iterator().next().getValue().lastUsedTimestmap;
		long outputFileStartTime = outputFileMap.entrySet().iterator().next().getKey();
		for (Entry<Long, OutputFile> entry : outputFileMap.entrySet()) {
			if (oldestTs < entry.getValue().lastUsedTimestmap) {
				oldestTs = entry.getValue().lastUsedTimestmap;
				// We actually need the entry's key, which represents the outputfile's start time
				outputFileStartTime = entry.getKey();
			}
		}
		return outputFileStartTime;
	}

	public void abortAll() {
		for (Map.Entry<Long, OutputFile> entry : outputFileMap.entrySet()) {
			entry.getValue().abort();
		}
	}

	public void closeAll() {
		for (Map.Entry<Long, OutputFile> entry : outputFileMap.entrySet()) {
			entry.getValue().close();
		}
	}

	public void closeOffShift(long shiftNumber) throws Exception {
		Iterator<Map.Entry<Long, OutputFile>> iter = outputFileMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<Long, OutputFile> entry = iter.next();
			if (entry.getValue().shiftNumber == shiftNumber) {
				try {
					entry.getValue().close();
					LOG.info("[{}] Shift #{} file closed: {}  ({} files still open",
						 partitionId,
						 shiftNumber,
						 entry.getValue().openFilePath,
						 outputFileMap.size());
					iter.remove();
				} catch (Exception e) {
					LOG.error("Error closing output path {}", this, e);
					throw e;
				}
			}
		}
	}

	/**
	 * @return the partition
	 */
	public int getPartition() {
		return partition;
	}

	private class OutputFile {

		private String dir;
		private String openFileDirectory;
		private String filename;
		private Path finalPath;
		private Path openFilePath;
		private FastBoomWriter boomWriter;
		private HdfsDataOutputStream hdfsDataOut;
		private long startTime;
		private Boolean useTempOpenFileDir;
		private long lastUsedTimestmap;
		private final long shiftNumber;

		public OutputFile(long shiftNumber, String filename, Long startTime) {
			this.shiftNumber = shiftNumber;
			this.filename = filename;
			this.startTime = startTime;
			this.useTempOpenFileDir = config.getRunningConfig().getUseTempOpenFileDirectory();

			dir = Converter.timestampTemplateBuilder(startTime,
				 String.format("%s/%s", topicConfig.getHdfsRootDir(), topicConfig.getDefaultDirectory()));
			finalPath = new Path(dir + "/" + filename);
			openFilePath = finalPath;

			if (useTempOpenFileDir) {
				openFileDirectory = dir;
				openFileDirectory = String.format("%s/%s%s", dir, config.getRunningConfig().getBoomFileTmpPrefix(), this.filename);
				openFilePath = new Path(openFileDirectory + "/" + filename);
			}

			try {
				if (fileSystem.exists(openFilePath)) {
					fileSystem.delete(openFilePath, false);
					LOG.info("Removing file from HDFS because it already exists: {}", openFilePath);
				}

				hdfsDataOut = (HdfsDataOutputStream) fileSystem.create(
					 openFilePath,
					 config.getBoomFilePerms(),
					 false,
					 config.getRunningConfig().getBoomFileBufferSize(),
					 config.getRunningConfig().getBoomFileReplicas(),
					 config.getRunningConfig().getBoomFileBlocksize(),
					 null);

				boomWriter = new FastBoomWriter(
					 hdfsDataOut,
					 topic,
					 partition,
					 config);

				if (config.getRunningConfig().getUseNativeCompression()) {
					boomWriter.loadNativeDeflateLib();
				}

				LOG.info("[{}] FastBoomWriter created {}", partitionId, openFilePath);

			} catch (Exception e) {
				LOG.error("[{}] Error creating file {}: ", partitionId, openFilePath, e);
			}
		}

		public void abort() {
			LOG.info("Aborting output file: {}", openFilePath);

			try {
				boomWriter.close();
			} catch (IOException e) {
				LOG.error("[{}] Error closing boom writer: {}", partitionId, openFilePath, e);
			}

			try {
				hdfsDataOut.close();
			} catch (IOException e) {
				LOG.error("[{}] Error closing boom writer output file: {}", partitionId, openFilePath, e);
			}

			try {
				if (useTempOpenFileDir) {
					fileSystem.delete(new Path(openFileDirectory), true);
					LOG.info("[{}] Deleted temp open file directory: {}", partitionId, openFileDirectory);
				} else {
					fileSystem.delete(openFilePath, true);
					LOG.info("[{}] Deleted open file: {}", partitionId, openFilePath);
				}
			} catch (IOException e) {
				LOG.error("[{}] Error deleting open file: {}", partitionId, openFilePath, e);
			}
		}

		public void close() {
			LOG.info("[{}] Closing {}", partitionId, openFilePath);

			try {
				boomWriter.close();
				LOG.info("[{}] Boom writer closed for {}", partitionId, openFilePath);
				hdfsDataOut.close();
				LOG.info("[{}] Output stream closed for {}", partitionId, openFilePath);
			} catch (IOException ioe) {
				LOG.error("[{}] Error closing up boomWriter {}:", partitionId, openFilePath, ioe);
			}

			if (useTempOpenFileDir) {
				try {
					LOG.info("[{}] Moving {} to {}", partitionId, openFilePath, finalPath);
					fileSystem.rename(openFilePath, finalPath);
				} catch (Exception e) {
					LOG.error("[{}] Error moving {} to {}", partitionId, openFilePath, finalPath, e);
					abort();
				}

				try {
					fileSystem.delete(new Path(openFileDirectory), true);
					LOG.info("[{}] Deleted temp open file directory: {}", partitionId, openFileDirectory);
				} catch (IllegalArgumentException | IOException e) {
					LOG.error("[{}] Error deleting temp open file direcrory {}", partitionId, openFileDirectory, e);
				}
			}
		}

		public Long getStartTime() {
			return startTime;
		}

		public FastBoomWriter getBoomWriter() {
			return boomWriter;
		}

	}

}
