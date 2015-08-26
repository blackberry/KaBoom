/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.blackberry.bdp.kaboom;

import java.io.IOException;
import java.util.Calendar;

import java.util.List;
import java.util.TimeZone;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.common.threads.NotifyingThread;
import com.blackberry.bdp.common.conversion.Converter;
import com.blackberry.bdp.kaboom.api.KaBoomTopic;
import java.util.concurrent.TimeUnit;

public class ReadyFlagWriter extends NotifyingThread {

	private static final Object fsLock = new Object();
	private static final Logger LOG = LoggerFactory.getLogger(ReadyFlagWriter.class);

	private FileSystem fs;
	private final CuratorFramework curator;
	private final StartupConfig config;
	private final List<KaBoomTopic> kaboomTopics;

	private final String KAFKA_READY_FLAG;
	
	public final String WORKING_DIR = "working";
	private static final String LOG_TAG = "[ready flag writer] ";

	public ReadyFlagWriter(StartupConfig config, 
		 List<KaBoomTopic> kaboomTopics) throws Exception {
		this.config = config;
		this.kaboomTopics = kaboomTopics;
		this.curator = config.getKaBoomCurator();
		this.KAFKA_READY_FLAG = config.getRunningConfig().getKafkaReadyFlagFilename();
	}

	/*
	 * Takes an HDFS path (or template) 
	 * Returns the last occurrence of a sub directory
	 */
	public static String subDirectoryFromPath(String hdfsPath, String dir) {
		String result = new String();
		int lastCharPos = hdfsPath.lastIndexOf(dir);
		// Check to see if last occurrence of FLAG_DIR is the end of the string
		if (hdfsPath.length() == lastCharPos + dir.length()) {
			result = hdfsPath;
		} else {
			result = hdfsPath.substring(0, lastCharPos + dir.length());
		}
		return result;
	}

	/*
	 * Takes an HDFS path (or template) and a sub directory
	 * Returns the parent directory for the last occurrence of sub directory
	 */
	public static String parentFromPath(String hdfsPath, String dir) {
		int lastCharPos = hdfsPath.lastIndexOf("/" + dir);
		return hdfsPath.substring(0, lastCharPos);
	}

	/**
	 * Do we need to write the _KAFKA_READY flag for the last hour?
	 *
	 * Look at all the partitions of a topic If all the topic's partitions's offsets 
	 * have passed a previous hour then write the _KAFKA_READY flag in the 
	 * previous hour's data directory
	 *
	 * Note: This is intended to be invoked infrequently (no more than once 
	 * every 10 minutes or so... If that frequency increases then optimizations 
	 * should be explored.
	 *
	 */
	@Override
	public void doRun() throws Exception {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		Calendar previousHourCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

		long currentTimestamp = cal.getTimeInMillis();
		long startOfHourTimestamp = currentTimestamp - currentTimestamp % (60 * 60 * 1000);

		for (KaBoomTopic topic : kaboomTopics) {			
			
			String hdfsTemplate = topic.getConfig().getHdfsRootDir();
			String topicName = topic.getKafkaTopic().getName();

			if (hdfsTemplate == null) {
				LOG.error(LOG_TAG + "HDFS path property for topic={} is not defined in configuraiton, skipping topic", topicName);
				continue;
			}

			fs = config.authenticatedFsForProxyUser(topic.getConfig().proxyUser);

			Long oldestTimestamp = topic.oldestPartitionOffset();			
			if (oldestTimestamp == null) {
				LOG.info(LOG_TAG + "failed to get the oldest partition timestamp for {}", topic);
				continue;
			}
				 
			long oldestTimestampMillisAgo = System.currentTimeMillis() - oldestTimestamp;

			LOG.info(LOG_TAG + "oldest partition for topic {} is {}", topicName,
				 String.format("%d minutes and %d seconds ago",
					  TimeUnit.MILLISECONDS.toMinutes(oldestTimestampMillisAgo),
					  TimeUnit.MILLISECONDS.toSeconds(oldestTimestampMillisAgo) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(oldestTimestampMillisAgo))));

			for (Integer hourNum = 1; hourNum <= config.getRunningConfig().getReadyFlagPrevHoursCheck(); hourNum++) {
				/**
				 * We know what the current timestamp was when we started, so start subtracting hourNum * 60 * 60 * 1000 from it so we're checking previous hours...
				 *
				 * NOTE: Must start looking in the previous hour, as there's no certainty for the current hour
				 */

				long prevHourStartTimestmap = startOfHourTimestamp - (60 * 60 * 1000) * hourNum;
				previousHourCal.setTimeInMillis(prevHourStartTimestmap);

				if (oldestTimestamp < prevHourStartTimestmap) {					
					// Skip as the oldest partiton is still before the top of this hour
					continue;
				}

				try {
					final Path topicRoot = new Path(Converter.timestampTemplateBuilder(prevHourStartTimestmap, hdfsTemplate));

					final Path dataDirectory = new Path(topicRoot + "/" + topic.getConfig().getDefaultDirectory());
					final Path workingDirectory = new Path(topicRoot + "/" + WORKING_DIR);

					final Path kafkaReadyFlag1 = new Path(dataDirectory.toString() + "/" + KAFKA_READY_FLAG);
					final Path kafkaReadyFlag2 = new Path(topicRoot.toString() + "/" + KAFKA_READY_FLAG);

					Path[] kafkaReadyFlags = {kafkaReadyFlag1, kafkaReadyFlag2};

					LOG.trace(LOG_TAG + "HDFS path for topic root is: {}", topicRoot.toString());
					LOG.trace(LOG_TAG + "HDFS path for data directory is: {}", dataDirectory.toString());

					for (int i = 0; i < kafkaReadyFlags.length; i++) {
						LOG.trace(LOG_TAG + "HDFS path for kafka ready flag #{} is: {}", i + 1, kafkaReadyFlags[i].toString());
					}

					if (fs.exists(workingDirectory)) {
						LOG.trace(LOG_TAG + "skipping {} since working directory {} exists", topicName, workingDirectory.toString());
						continue;
					}

					/**
					 * We now have multiple flag path support, iterate over them and check if they exist
					 */
					for (Path flagPath : kafkaReadyFlags) {
						if (fs.exists(flagPath)) {
							LOG.trace(LOG_TAG + "skipping {} since kafka's ready flag {} already exists", topicName, flagPath.toString());
							continue;
						}

						synchronized (fsLock) {
							try {
								fs.create(flagPath).close();
								LOG.info(LOG_TAG + "[{}] wrote {} as {}", topicName, flagPath.toString(), topic.getConfig().getProxyUser());
							} catch (IOException e) {
								LOG.error("Error getting File System: {}", e.toString());
							}
						}
					}
				} catch (Exception e) {
					LOG.error(LOG_TAG + "topic {} error occured processing a partition: ", topicName, e);
				}
			}
		}
	}

}
