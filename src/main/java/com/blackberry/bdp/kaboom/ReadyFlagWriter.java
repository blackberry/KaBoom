/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blackberry.bdp.kaboom;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import kafka.javaapi.PartitionMetadata;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.common.utils.threads.NotifyingThread;
import com.blackberry.bdp.common.utils.conversion.Converter;
import java.util.ArrayList;
import org.apache.commons.lang.StringUtils;

public class ReadyFlagWriter extends NotifyingThread
{
	private static final Object fsLock = new Object();
	private static final Logger LOG = LoggerFactory.getLogger(ReadyFlagWriter.class);

	private FileSystem fs;
	private CuratorFramework curator;
	private KaboomConfiguration config;

	private static final String ZK_ROOT = "/kaboom";
	public static final String MERGE_READY_FLAG = "_READY";
	public static final String KAFKA_READY_FLAG = "_KAFKA_READY";
	public static final String INCOMING_DIR = "incoming";
	public static final String WORKING_DIR = "working";
	public static final String LOG_TAG = "[ready flag writer] ";

	public ReadyFlagWriter(KaboomConfiguration config, CuratorFramework curator) throws Exception
	{
		this.config = config;
		this.curator = curator;
	}

	/*
	 * Takes an HDFS path (or template) 
	 * Returns the last occurrence of a sub directory
	 */
	public static String subDirectoryFromPath(String hdfsPath, String dir)
	{
		String result = new String();
		int lastCharPos = hdfsPath.lastIndexOf(dir);
		// Check to see if last occurrence of FLAG_DIR is the end of the string
		if (hdfsPath.length() == lastCharPos + dir.length())
		{
			result = hdfsPath;
		} else
		{
			result = hdfsPath.substring(0, lastCharPos + dir.length());
		}
		return result;
	}

	/*
	 * Takes an HDFS path (or template) and a sub directory
	 * Returns the parent directory for the last occurrence of sub directory
	 */
	public static String parentFromPath(String hdfsPath, String dir)
	{
		int lastCharPos = hdfsPath.lastIndexOf("/" + dir);
		return hdfsPath.substring(0, lastCharPos);		
	}

	/*
	 * For a given topic and list of partitions
	 * Returns the oldest timestamp stored in ZK for partition offset 
	 */
	public long oldestPartitionOffsetForTopic(String topicName, List<PartitionMetadata> partitions) throws Exception
	{
		long oldestTimestamp = -1;

		for (PartitionMetadata partition : partitions)
		{
			String zk_offset_path = ZK_ROOT + "/topics/" + topicName + "/" + partition.partitionId() + "/offset_timestamp";
			Stat stat = this.curator.checkExists().forPath(zk_offset_path);

			if (stat != null)
			{
				Long thisTimestamp = Converter.longFromBytes(curator.getData().forPath(zk_offset_path), 0);
				LOG.info(LOG_TAG + "found topic={} partition={} offset timestamp={}", topicName, partition.partitionId(), thisTimestamp);
				if (thisTimestamp < oldestTimestamp || oldestTimestamp == -1)
				{
					oldestTimestamp = thisTimestamp;
				}
				stat = null;
			} 
			else
			{
				LOG.error(LOG_TAG + "cannot get stat for path {}", zk_offset_path);
			}
		}

		return oldestTimestamp;
	}

	/*
	 * Do we need to write the _KAFKA_READY flag for the last hour?
	 * 
	 * Look at all the partitions of a topic If all the topic's partitions's
	 * offsets have passed the previous hour Then write the _KAFKA_READY flag in
	 * the previous hour's incoming directory
	 * 
	 * Note: This is intended to be invoked infrequently (no more than once every
	 * 10 minutes or so... If that frequency increases then optimizations should
	 * be explored.
	 * 
	 * Note: This isn't intelligent enough to look earlier than the previous hour.
	 * If this method isn't called at least once per hour then there will be flags
	 * that will never become set.
	 */
	@Override
	public void doRun() throws Exception
	{
		Map<String, List<PartitionMetadata>> topicsWithPartitions = new HashMap<>();
		StateUtils.getTopicParitionMetaData(config.getKafkaZkConnectionString(), config.getKafkaSeedBrokers(), topicsWithPartitions);

		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		Calendar previousHourCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

		long currentTimestamp = cal.getTimeInMillis();
		long startOfHourTimestamp = currentTimestamp - currentTimestamp % (60 * 60 * 1000);
		
		ArrayList<String> skippedTopicNames = new ArrayList<>();

		for (Map.Entry<String, List<PartitionMetadata>> entry : topicsWithPartitions.entrySet())
		{
			String topicName = entry.getKey();
				
			if (!config.getTopicToSupportedStatus().containsKey(topicName)
				 || config.getTopicToSupportedStatus().get(topicName) == false)
			{				
				skippedTopicNames.add(topicName);
				continue;
			}

			for (Integer hourNum = 0; hourNum <= config.getReadyFlagPrevHoursCheck(); hourNum++)
			{
				/**
				 * We know what the current timestamp was when we started, so start subtracting 
				 * hourNum * 60 * 60 * 100 from it so we're checking previous hours... Note hourNum 
				 * starts at 0 so we're not skipping the immediate previous hour
				 */

				long prevHourStartTimestmap = startOfHourTimestamp - (60 * 60 * 1000);
				previousHourCal.setTimeInMillis(prevHourStartTimestmap);

				try
				{
					LOG.info(LOG_TAG + "Checking {} partition(s) in topic={} for offset timestamps...", entry.getValue().size(), topicName);

					String hdfsTemplate = config.getTopicToKafkaReadyFlagPath().get(topicName);

					if (hdfsTemplate == null)
					{
						LOG.error(LOG_TAG + "HDFS path property for topic={} is not defined in configuraiton, skipping topic", topicName);
						continue;
					}

					String hdfsPath = Converter.timestampTemplateBuilder(prevHourStartTimestmap, hdfsTemplate);

					final Path topicRoot = new Path(parentFromPath(hdfsPath, INCOMING_DIR));
					final Path mergeReadyFlag = new Path(topicRoot + "/" + MERGE_READY_FLAG);
					final Path incomingDirectory = new Path(topicRoot + "/" + INCOMING_DIR);
					final Path workingDirectory = new Path(topicRoot + "/" + WORKING_DIR);
					final Path kafkaReadyFlag = new Path(incomingDirectory.toString() + "/" + KAFKA_READY_FLAG);

					LOG.info(LOG_TAG + "HDFS path for topic root is: {}", topicRoot.toString());
					LOG.info(LOG_TAG + "HDFS path for merge ready flag is: {}", mergeReadyFlag.toString());
					LOG.info(LOG_TAG + "HDFS path for kafka ready flag is: {}", kafkaReadyFlag.toString());
					LOG.info(LOG_TAG + "HDFS path for incoming directory is: {}", incomingDirectory.toString());
					LOG.info(LOG_TAG + "opening {}", topicRoot.toString());

					Authenticator.getInstance().runPrivileged(config.getTopicToProxyUser().get(topicName),
						 new PrivilegedExceptionAction<Void>()
						 {
							 @Override
							 public Void run() throws Exception
							 {
								 synchronized (fsLock)
								 {
									 try
									 {
										 fs = topicRoot.getFileSystem(config.getHadoopConfiguration());
									 } 
									 catch (IOException e)
									 {
										 LOG.error(LOG_TAG + "Error getting file system for path {}, error: {}.", topicRoot.toString(), e);
									 }
								 }
								 return null;
							 }
						 });

					if (!fs.exists(incomingDirectory))
					{
						LOG.info(LOG_TAG + "skipping {} since incoming directory {} doesn't exist (no data)", topicName, incomingDirectory.toString());
						continue;
					}

					if (fs.exists(mergeReadyFlag))
					{
						LOG.info(LOG_TAG + "skipping {} since merge's ready flag {} already exists", topicName, mergeReadyFlag.toString());
						continue;
					}

					if (fs.exists(kafkaReadyFlag))
					{
						LOG.info(LOG_TAG + "skipping {} since kafka's ready flag {} already exists", topicName, kafkaReadyFlag.toString());
						continue;
					}

					if (fs.exists(workingDirectory))
					{
						LOG.info(LOG_TAG + "skipping {} since working directory {} already exists", topicName, workingDirectory.toString());
						continue;
					}

					LOG.info(LOG_TAG + "topic {} might be candidate for kafka ready flag (incoming data exists, no other flags exist)", topicName);
					long oldestTimestamp = oldestPartitionOffsetForTopic(topicName, entry.getValue());
					LOG.info(LOG_TAG + "oldest timestamp for topic={} is {}", topicName, oldestTimestamp);

					if (oldestTimestamp > startOfHourTimestamp)
					{
						LOG.info(LOG_TAG + "topic {} oldest timestamp is within the current hour, flag write required", topicName);
						LOG.info(LOG_TAG + "topic {} need to write {} as proxy user {}", topicName, kafkaReadyFlag.toString(), config.getTopicToProxyUser().get(topicName));
						synchronized (fsLock)
						{
							try
							{
								fs.create(kafkaReadyFlag).close();
							} 
							catch (IOException e)
							{
								LOG.error("Error getting File System: {}", e.toString());
							}
						}
						LOG.info(LOG_TAG + "topic {} flag {} written as {}", topicName, kafkaReadyFlag.toString(), config.getTopicToProxyUser().get(topicName));
					}

				}
				catch (Exception e)
				{
					LOG.error(LOG_TAG + "topic {} error occured processing a partition: {}", topicName, e.toString());
				}

				LOG.info(LOG_TAG + "finished {} topic(s) after {} seconds", topicsWithPartitions.size(), (cal.getTimeInMillis() - currentTimestamp) / 1000);
			}
			
			startOfHourTimestamp -= 60 * 60 * 1000;

		}
		
		LOG.info("The ReadFlagWriter thead skipped the following unsupported topics:  {}", StringUtils.join(skippedTopicNames, String.format("%n\t")));
	}
}
