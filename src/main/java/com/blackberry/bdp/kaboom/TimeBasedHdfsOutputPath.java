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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
	private Worker kaboomWorker;
	private final String topic;
	private int partition;
	private final FileSystem fileSystem;
	private String partitionId = "unknown-partitionId";

	private final Map<Long, OutputFile> outputFileMap = new HashMap<>();

	/**
	 * Reusable objects only exist as class attributes because they are needed 
	 * very frequently Instead of continually re-instantiating transient objects 
	 * in the critical message path they are created once and long lived
	 */
	private long reusableRequestedStartTime;
	private OutputFile reusableRequestedOutputFile;

	public TimeBasedHdfsOutputPath(FileSystem fileSystem,StartupConfig kaboomConfig, KaBoomTopicConfig topicConfig) {
		this.fileSystem = fileSystem;
		this.config = kaboomConfig;
		this.topicConfig = topicConfig;
		this.topic = topicConfig.getId();
	}

	private static String dateString(Long ts) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date now = new Date();
		String strDate = sdf.format(ts);
		return strDate;
	}

	public FastBoomWriter getBoomWriter(long sprintNumber, long ts, String filename, ArrayList<Path> paths) throws IOException, Exception {
		reusableRequestedStartTime = ts - ts % (this.config.getRunningConfig().getWorkerSprintDurationSeconds() * 1000);
		reusableRequestedOutputFile = outputFileMap.get(reusableRequestedStartTime);
		if (reusableRequestedOutputFile == null) {
			reusableRequestedOutputFile = new OutputFile(sprintNumber, filename, reusableRequestedStartTime, paths);
			outputFileMap.put(reusableRequestedStartTime, reusableRequestedOutputFile);			
			if (outputFileMap.size() > config.getRunningConfig().getMaxOpenBoomFilesPerPartition()) {
				long oldestTs = getOldestLastUsedTimestamp();
				try {
					OutputFile oldestOutputFile = outputFileMap.get(oldestTs);
					oldestOutputFile.close();
					LOG.info("[{}]  over max open boom file limit ({}/{}) closing oldest last used boom file: {}",
						 partitionId,
						 outputFileMap.size(),
						 config.getRunningConfig().getMaxOpenBoomFilesPerPartition(),
						 oldestOutputFile.openFilePath);
					outputFileMap.remove(oldestTs);
					LOG.info("[{}] oldest last used boom file with starting hour {} removed from mapping", partitionId, dateString(oldestTs));
				} catch (Exception e) {
					LOG.error("[{}] Failed to close off oldest boom writer: ", partitionId, e);
				}
			}
		}
		reusableRequestedOutputFile.lastUsedTimestmap = System.currentTimeMillis();
		return reusableRequestedOutputFile.getBoomWriter();
	}

	private long getOldestLastUsedTimestamp() {
		long oldestTs = outputFileMap.entrySet().iterator().next().getValue().lastUsedTimestmap;
		for (Entry<Long, OutputFile> entry : outputFileMap.entrySet()) {
			if (oldestTs < entry.getValue().lastUsedTimestmap) {
				oldestTs = entry.getValue().lastUsedTimestmap;
			}
		}
		return oldestTs;
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
	
	public void closeOffSprint(long sprintNumber) throws Exception
	{
		Iterator<Map. Entry<Long,OutputFile>> iter = outputFileMap.entrySet().iterator();
		int closedFiles = 0;		
		while (iter.hasNext()) {
			Map.Entry<Long, OutputFile> entry = iter.next();
			if (entry.getValue().sprintNumber == sprintNumber) {
				try {					
					entry.getValue().close();
					closedFiles++;
					LOG.info("[{}] Sprint {} file closed : {}  ({} files still open): {}", 
						 partitionId, 
						 sprintNumber, 
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
	 * @param partitionId the partitionId to set
	 */
	public void setPartitionId(String partitionId) {
		this.partitionId = partitionId;
	}

	/**
	 * @param kaboomWorker the kaboomWorker to set
	 */
	public void setKaboomWorker(Worker kaboomWorker) {
		this.kaboomWorker = kaboomWorker;
	}

	/**
	 * @return the kaboomWorker
	 */
	public Worker getKaboomWorker() {
		return kaboomWorker;
	}

	/**
	 * @return the partition
	 */
	public int getPartition() {
		return partition;
	}

	/**
	 * @param partition the partition to set
	 */
	public void setPartition(int partition) {
		this.partition = partition;
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
		private ArrayList<Path> paths;
		private final long sprintNumber;

		public OutputFile(long sprintNumber, String filename, Long startTime, ArrayList<Path> paths) {			
			this.sprintNumber = sprintNumber;
			this.filename = filename;
			this.startTime = startTime;
			this.useTempOpenFileDir = config.getRunningConfig().getUseTempOpenFileDirectory();
			this.lastUsedTimestmap = System.currentTimeMillis();
			this.paths = paths;
			
			dir = Converter.timestampTemplateBuilder(startTime, 
				 String.format("%s/%s", topicConfig.getHdfsRootDir(), topicConfig.getDefaultDirectory()));			
			finalPath = new Path(dir + "/" + filename);
			openFilePath = finalPath;

			if (useTempOpenFileDir) {
				openFileDirectory = dir;
				openFileDirectory = String.format("%s/%s%s", dir, config.getRunningConfig().getBoomFileTmpPrefix(), this.filename);
				openFilePath = new Path(openFileDirectory + "/" + filename);				
				paths.add(new Path(openFileDirectory));
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
				
				paths.add(openFilePath);

				boomWriter = new FastBoomWriter(
					 hdfsDataOut,
					 topic,
					 partition,
					 config);

				boomWriter.setPeriodicHdfsFlushInterval(config.getRunningConfig().getPeriodicHdfsFlushInterval());
				boomWriter.setUseNativeCompression(config.getRunningConfig().getUseNativeCompression());

				if (config.getRunningConfig().getUseNativeCompression()) {
					boomWriter.loadNativeDeflateLib();
				}

			} catch (Exception e) {
				LOG.error("[{}] Error creating file {}", partitionId, openFilePath, e);
			}
		}
		
		/*
		@Override
		public String toString() {
			return String.format("%s:%n"
				 + "\ttmpPath: %s%n"
				 + "\tfinalPath: %s%n"
				 + "\tstarts: %s (%s)%n"
				 + "\tcloses: %s (%s)%n",
				 getClass().getName(),
				 this.openFilePath,
				 this.finalPath,
				 this.startTime, dateString(this.startTime));
		}
		*/

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
					paths.remove(new Path(openFileDirectory));
					LOG.info("[{}] Deleted temp open file directory: {}", partitionId, openFileDirectory);
				} else {
					fileSystem.delete(openFilePath, true);
					paths.remove(openFilePath);
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
					paths.remove(openFilePath);
					paths.add(finalPath);
				} catch (Exception e) {
					LOG.error("[{}] Error moving {} to {}", partitionId, openFilePath, finalPath, e);
					abort();
				}

				try {
					fileSystem.delete(new Path(openFileDirectory), true);
					paths.remove(new Path(openFileDirectory));
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
	
	/*
	@Override
	public String toString() {
		return String.format("%s:%n"
			 + "\tseconds: %s%n"
			 + "\tpathTemplate: %s%n",
			 getClass().getName(),
			 this.topicConfig.getDefaultDirectory());
	}
	*/
}
