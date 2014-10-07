package com.blackberry.kaboom;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import kafka.javaapi.PartitionMetadata;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.common.threads.NotifyingThread;
import com.blackberry.common.conversion.Converter;

public class ReadyFlagWriter extends NotifyingThread 
{
	private static final Logger LOG = LoggerFactory.getLogger(ReadyFlagWriter.class);
	private CuratorFramework curator;
	private Map<String, String> topicFileLocation;
	private Map<String, String> topicToProxyUser;
	private String kafkaZkConnectionString;
	private String kafkaSeedBrokers;
	private static final Object fsLock = new Object();
	private FileSystem fs;
	private Configuration hConf;
	private static final String ZK_ROOT = "/kaboom"; // TODO: This is currently a duplicate hard coded variable... Also exists in Worker.java
	public static final String READY_FLAG = "_KAFKA_READY";
	public static final String FLAG_DIR = "/incoming";
	public static final String LOG_TAG = "[ready flag writer]";

	public ReadyFlagWriter(String kafkaZkConnectionString,
			String kafkaSeedBrokers, CuratorFramework curator,
			Map<String, String> topicFileLocation, Configuration hConf,
			Map<String, String> topicToProxyUser) throws Exception 
	{
		this.kafkaZkConnectionString = kafkaZkConnectionString;
		this.kafkaSeedBrokers = kafkaSeedBrokers;
		this.curator = curator;
		this.topicFileLocation = topicFileLocation;
		this.hConf = hConf;
		this.topicToProxyUser = topicToProxyUser;
	}

	/*
	 * Takes an HDFS (or template) Looks for the last occurrence of a directory
	 * Truncates after the last occurrence of the directory
	 */

	public static String flagRootFromHdfsPath(String hdfsPath) 
	{
		String flagRoot = new String();
		int lastCharPos = hdfsPath.lastIndexOf(FLAG_DIR);	
		// Check to see if last occurrence of FLAG_DIR is the end of the string
		if (hdfsPath.length() == lastCharPos + FLAG_DIR.length()) 
		{
			flagRoot = hdfsPath;
		} else 
		{
			flagRoot = hdfsPath.substring(0, lastCharPos + FLAG_DIR.length());
		}
		return flagRoot;
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
	 * that will never bet set.
	 */
	
	@Override
	public void doRun() throws Exception 
	{
		Map<String, List<PartitionMetadata>> topicsWithPartitions = new HashMap<String, List<PartitionMetadata>>();
		StateUtils.getTopicParitionMetaData(kafkaZkConnectionString, kafkaSeedBrokers, topicsWithPartitions);
		
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		Calendar previousHourCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		
		long currentTimestamp = cal.getTimeInMillis();
		long startOfHourTimestamp = currentTimestamp - currentTimestamp % (60 * 60 * 1000);
		long prevHourStartTimestmap = startOfHourTimestamp - (60 * 60 * 1000);		
		
		previousHourCal.setTimeInMillis(prevHourStartTimestmap);

		for (Map.Entry<String, List<PartitionMetadata>> entry : topicsWithPartitions.entrySet()) {
			
			String topicName = entry.getKey();
			
			try 
			{
				LOG.debug(LOG_TAG + " Checking {} partition(s) in topic={} for offset timestamps...", entry.getValue().size(), topicName);

				String hdfsTemplate = topicFileLocation.get(topicName);

				if (hdfsTemplate == null) 
				{
					LOG.error(LOG_TAG + " HDFS path property for topic={} is not defined in configuraiton, skipping topic", topicName);
					continue;
				}

				String hdfsFlagRoot;
				
				hdfsFlagRoot = flagRootFromHdfsPath(hdfsTemplate);				
				hdfsFlagRoot = Converter.timestampTemplateBuilder(prevHourStartTimestmap, hdfsFlagRoot);
				
				final Path hdfsFlagRootPath = new Path(hdfsFlagRoot);
				final Path hdfsFlagFilePath = new Path(hdfsFlagRoot + "/" + READY_FLAG);
				
				LOG.debug(LOG_TAG + " HDFS root for the ready flag is: {}", hdfsFlagRootPath.toString());
				LOG.debug(LOG_TAG + " HDFS path for the ready flag is: {}", hdfsFlagFilePath.toString());				

				Authenticator.getInstance().runPrivileged(topicToProxyUser.get(topicName),
						new PrivilegedExceptionAction<Void>() 
						{
							@Override
							public Void run() throws Exception {
								synchronized (fsLock) {
									try {
										fs = hdfsFlagRootPath.getFileSystem(hConf);
									} catch (IOException e) {
										LOG.error(LOG_TAG + " Error getting File System for path {}, error: {}.", hdfsFlagRootPath.toString(), e);
									}
								}
								return null;
							}
						});

				LOG.debug(LOG_TAG + " opening {}", hdfsFlagFilePath.toString());

				if (fs.exists(hdfsFlagFilePath)) 
				{
					LOG.debug(LOG_TAG + " flag {} already exists at {}", READY_FLAG, hdfsFlagFilePath.toString());
					continue;
				}
				else
				{
					LOG.debug(LOG_TAG + " flag {} doesn't exist yet at {}", READY_FLAG, hdfsFlagFilePath.toString());
				}
				
				if (!fs.exists(hdfsFlagRootPath))
				{
					LOG.debug(LOG_TAG + " flag_root {} doesn't exist (no data)", hdfsFlagRootPath.toString());
					continue;
				}
				else
				{
					LOG.debug(LOG_TAG + " flag_root {} does in fact exist (there is data)", hdfsFlagRootPath.toString());
				}
				
				long oldestTimestamp = -1;

				for (PartitionMetadata partition : entry.getValue()) 
				{
					String zk_offset_path = ZK_ROOT + "/topics/" + topicName + "/" + partition.partitionId() + "/offset_timestamp";
					Stat stat = curator.checkExists().forPath(zk_offset_path);
					
					if (stat != null) {
						Long thisTimestamp = Converter.longFromBytes(curator.getData().forPath(zk_offset_path), 0);
						LOG.debug(LOG_TAG + " found topic={} partition={} offset timestamp={}",topicName, partition.partitionId(), thisTimestamp);
						if (thisTimestamp < oldestTimestamp || oldestTimestamp == -1) {
							oldestTimestamp = thisTimestamp;
						}
						stat = null;
					} else {
						LOG.error(LOG_TAG + " cannot get stat for path {}", zk_offset_path);
					}					
				}

				LOG.debug(LOG_TAG + " oldest timestamp for topic={} is {}", topicName, oldestTimestamp);

				if (oldestTimestamp > startOfHourTimestamp) 
				{
					LOG.debug(LOG_TAG + " oldest timestamp is within the current hour, flag write required");
					LOG.debug(LOG_TAG + " need to write {} as proxy user {}", hdfsFlagFilePath.toString(), topicToProxyUser.get(topicName));

					synchronized (fsLock) {
						try {
							fs.create(hdfsFlagFilePath).close();
						} catch (IOException e) {
							LOG.error("Error getting File System: {}", e.toString());
						}
					}

					LOG.debug(LOG_TAG + " flag {} written as {}", hdfsFlagFilePath.toString(), topicToProxyUser.get(topicName));
				}
			} 
			catch (Exception e) 
			{
				LOG.error(LOG_TAG + " error occured processing a partition: {}", e.toString());
			}

			LOG.debug(LOG_TAG + " finished {} topic(s) after {} seconds", topicsWithPartitions.size(), (cal.getTimeInMillis() - currentTimestamp) / 1000);
		}
	}
}
