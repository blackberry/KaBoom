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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.krackle.MetricRegistrySingleton;
import com.blackberry.bdp.krackle.consumer.Consumer;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import com.blackberry.bdp.common.utils.conversion.Converter;
import java.util.ArrayList;

public class Worker implements Runnable
{
	private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

	/**
	 * @return the ZK_ROOT
	 */
	public static String getZK_ROOT()
	{
		return ZK_ROOT;
	}

	private String partitionId;

	private Consumer consumer;
	private long offset;
	private long lowerOffsetsReceived = 0;
	private long timestamp;
	private boolean stopping = false;

	private String hostname;
	private KaboomConfiguration config;

	private CuratorFramework curator;
	private static final String ZK_ROOT = "/kaboom";
	private String zkPath;
	private String zkPath_offSetTimestamp;
	private String zkPath_offSetOverride;

	private String topic;
	private int partition;

	private long startTime;

	private long lag = 0;
	private int lag_sec = 0;
	private long linesread = 0;

	private String lagGaugeName;
	private String lagSecGaugeName;
	private String msgWrittenGaugeName;
	private String lowerOffsetsGaugeName;
	
	private ArrayList<TimeBasedHdfsOutputPath> hdfsOutputPaths;

	private static Set<Worker> workers = new HashSet<>();
	private static final Object workersLock = new Object();

	static 
	{
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			.register("kaboom:total:max message lag sec", new Gauge<Integer>()
				  {
					  @Override
					  public Integer getValue()
					  {
						  int maxLagSec = 0;
						  synchronized (workersLock)
						  {
							  for (Worker w : workers)
							  {
								  maxLagSec = Math.max(maxLagSec, w.getLagSec());
							  }
						  }
						  return maxLagSec;
					  }
			 });
	}

	static
	{
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			.register("kaboom:total:sum message lag sec", new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  long sumLag = 0;
						  synchronized (workersLock)
						  {
							  for (Worker w : workers)
							  {
								  sumLag += w.getLagSec();
							  }
						  }
						  return sumLag;
					  }
			 });
	}

	static
	{
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:avg message lag sec", new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  long sumLag = 0;
						  int count = 0;
						  synchronized (workersLock)
						  {
							  for (Worker w : workers)
							  {
								  count++;
								  sumLag += w.getLagSec();
							  }
						  }
						  long avgLag = sumLag / count;
						  return avgLag;
					  }
			 });
	}

	static
	{
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:max message lag", new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  long maxLag = 0;
						  synchronized (workersLock)
						  {
							  for (Worker w : workers)
							  {
								  maxLag = Math.max(maxLag, w.getLag());
							  }
						  }
						  return maxLag;
					  }
			 });
	}

	static
	{
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:sum message lag", new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  long sumLag = 0;
						  synchronized (workersLock)
						  {
							  for (Worker w : workers)
							  {
								  sumLag += w.getLag();
							  }
						  }
						  return sumLag;
					  }
			 });
	}

	static
	{
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:avg message lag", new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  long sumLag = 0;
						  int count = 0;
						  synchronized (workersLock)
						  {
							  for (Worker w : workers)
							  {
								  count++;
								  sumLag += w.getLag();
							  }
						  }
						  long avgLag = sumLag / count;
						  return avgLag;
					  }
			 });
	}

	static
	{
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:avg messages written per sec", new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  long sumMsgWritten = 0;
						  int count = 0;
						  synchronized (workersLock)
						  {
							  for (Worker w : workers)
							  {
								  count++;
								  sumMsgWritten += w.getMsgWrittenPerSec();
							  }
						  }
						  return sumMsgWritten / count;
					  }
			 });
	}

	static
	{
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:total messages written per sec", new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  long sumMsgWritten = 0;
						  synchronized (workersLock)
						  {
							  for (Worker w : workers)
							  {
								  sumMsgWritten += w.getMsgWrittenPerSec();
							  }
						  }
						  return sumMsgWritten;
					  }
			 });
	}

	//Worker worker = new Worker(config, 	curator, topic, partition);
	
	public Worker(KaboomConfiguration config, CuratorFramework curator, String topic, int partition) throws Exception
	{
		this.config = config;
		this.curator = curator;
		this.topic = topic;
		this.partition = partition;
		this.startTime = System.currentTimeMillis();
		this.linesread = 0;
		this.hdfsOutputPaths = config.getTopicToHdfsPaths().get(topic);
		
		partitionId = topic + "-" + partition;
		
		LOG.info("Worker instantiated for {} and configured for {} output paths", partitionId, hdfsOutputPaths.size());
		
		for (TimeBasedHdfsOutputPath outputPath : hdfsOutputPaths)
		{
			LOG.info("\t {} {} => {}", config.getKaboomId(), partitionId, outputPath);
		}

		lagGaugeName = "kaboom:partitions:" + partitionId + ":message lag";
		lagSecGaugeName = "kaboom:partitions:" + partitionId + ":message lag sec";
		msgWrittenGaugeName = "kaboom:partitions:" + partitionId + ":messages written per second";
		lowerOffsetsGaugeName = "kaboom:partitions:" + partitionId + ":early offsets received";

		String[] metrics_to_remove = {lagGaugeName, lagSecGaugeName, msgWrittenGaugeName, lowerOffsetsGaugeName};

		for (final String metric_name : metrics_to_remove)
		{
			if (MetricRegistrySingleton.getInstance().getMetricsRegistry()
				 .getGauges(new MetricFilter()
					  {
						  @Override
						  public boolean matches(String s, Metric m)
						  {
							  return s.equals(metric_name);
						  }
				 }).size() > 0)
			{
				LOG.debug("Removing existing metric: '{}'", metric_name);
				MetricRegistrySingleton.getInstance().getMetricsRegistry()
					 .remove(metric_name);
			}
		}

		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register(lagGaugeName, new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  return lag;
					  }
			 });

		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register(lagSecGaugeName, new Gauge<Integer>()
				  {
					  @Override
					  public Integer getValue()
					  {
						  return lag_sec;
					  }
			 });

		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register(msgWrittenGaugeName, new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  return linesread / ((System.currentTimeMillis() - startTime) / 1000);
					  }
			 });

		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register(lowerOffsetsGaugeName, new Gauge<Long>()
				  {
					  @Override
					  public Long getValue()
					  {
						  return lowerOffsetsReceived;
					  }
			 });
		
		synchronized (workersLock)
		{
			// NetBeans considers this unsafe: See https://www.google.ca/#q=leaking+this+in+constructor
			workers.add(this);
		}
		
		LOG.info("[{}] Created worker.", partitionId);
	}

	@Override
	public void run()
	{
		try
		{
			zkPath = getZK_ROOT() + "/topics/" + getTopic() + "/" + getPartition();
			zkPath_offSetTimestamp = zkPath + "/offset_timestamp";
			zkPath_offSetOverride = zkPath + "/offset_override";						

			try
			{
				offset = getOffset();
			} 
			catch (Exception e)
			{
				LOG.error("[{}] Error getting offset.", getPartitionId(), e);
				return;
			}

			try
			{
				hostname = InetAddress.getLocalHost().getCanonicalHostName();
			} 
			catch (UnknownHostException e)
			{
				LOG.error("[{}] Can't determine local hostname", getPartitionId());
				hostname = "unknown.host";
			}

			String clientId = "kaboom-" + hostname;

			consumer = new Consumer(config.getConsumerConfiguration(), clientId, getTopic(), getPartition(),offset, MetricRegistrySingleton.getInstance().getMetricsRegistry());

			LOG.info("[{}] Created worker.  Starting at offset {}.", getPartitionId(), offset);

			byte[] bytes = new byte[1024 * 1024];
			int length;
			byte version;
			int pos;			
			PriParser pri = new PriParser();
			VersionParser ver = new VersionParser();
			TimestampParser tsp = new TimestampParser();
			long maxTimestamp = -1;
			
			while (stopping == false)
			{
				try
				{
					length = consumer.getMessage(bytes, 0, bytes.length);

					if (length == -1)
					{
						continue;
					}

					// offset always refers to the next offset we expect to 
					// since we just called consumer.getMessage() let's see
					// if the offset of the last message is what we expected
					// and handle the fun edge cases when it's not
					
					if (offset != consumer.getLastOffset())
					{
						long highWatermark = consumer.getHighWaterMark();
						
						if (offset > consumer.getLastOffset())
						{
							if (offset < highWatermark)
							{
								/* 
								 * When using SNAPPY compression in Krackle's consumer there will be messages received
								 * that are in the snappy block that are from earlier than our requested offset.  When this
								 * happens the consumer will continue to send us messages from within that block so we
								 * should just be patient until the offsets are from where we want.  
								*/

								LOG.debug("[{}] skipping last offset {} since it's lower than high watermark {}", getPartitionId(), consumer.getLastOffset(), highWatermark);
								
								lowerOffsetsReceived++;
								continue;
							} 
							else if (offset > highWatermark)
							{
								/*	
								 *	If the expected offset is greater than than actual offset and also higher than the high watermark 
								 *	then perhaps the broker we're receiving messages from has changed and the new broker has a 
								 *	lower offset because it was behind when it took over... Maybe?  
								 */

								if (config.getSinkToHighWatermark())
								{
									LOG.warn("[{}] offset {} is greater than high watermark {} and sinkToHighWatermark is {}, sinking to high watermark.", getPartitionId(), offset, highWatermark, config.getSinkToHighWatermark());

									consumer.setNextOffset(highWatermark);
									offset = highWatermark;

									LOG.info("[{}] Successfully set offset to the high watermark of {}", getPartitionId(), highWatermark);

									continue;
								} 
								else
								{
									LOG.error("[{}] offset {} is greater than high watermark {} and sinkToHighWatermark is {}, ignoring offset and skipping message.", getPartitionId(), offset, highWatermark, config.getSinkToHighWatermark());

									continue;
								}		
							} 
							else
							{
								// TODO: Should this continue or not?

								LOG.error("[{}] Unhandled edge case when expected offset is greater than last offset "
									 + "and expected offset is also the high watermark");
							}
						} 
						else
						{
							LOG.error("[{}] Offset anomaly! Expected:{}, Got {}, Consumer high watermark {}, latest {}, earliest {}", getPartitionId(), 
								 offset, 
								 consumer.getLastOffset(), 
								 consumer.getHighWaterMark(), 
								 consumer.getLatestOffset(),
								 consumer.getEarliestOffset());
						}
					}

					linesread++;
					offset = consumer.getNextOffset();
					lag = consumer.getHighWaterMark() - offset;

					// Check for version
					if (bytes[0] == (byte) 0xFE)
					{
						version = bytes[1];
						
						if (version == (byte) 0x00)
						{
							// Version 0 has a timestamp in the front, so we can skip that for now. Come back if we need it.
							pos = 10;
						}
						else
						{
							LOG.warn("[{}] Unrecognized encoding version: {}", getPartitionId(), version);
							pos = 0;
						}
					} 
					else
					{
						// version -1 is a raw log
						version = (byte) 0xFF;
						pos = 0;
					}

					// Optional PRI at the start of the line.
					if (pri.parsePri(bytes, pos, length))
					{
						pos += pri.getPriLength();
					}

					// On the off chance that someone is following RFC5424 and has
					// inserted a version in the log line.
					
					if (ver.parseVersion(bytes, pos, length - pos))
					{
						// Skip the length of the version and the following space.
						pos += ver.getVersionLength() + 1;
					}

					tsp.parse(bytes, pos, length - pos);
					
					if (tsp.getError() == TimestampParser.NO_ERROR)
					{
						timestamp = tsp.getTimestamp();
						// Move position to the end of the timestamp						
						pos += tsp.getLength();
						
						// mbruce: occasionally we get a line that is truncated partway through the timestamp,
						// however we still have the rest of the last message in the byte buffer and parsing the 
						// timestamp will push us past then end of the line
						
						if (pos > length)
						{
							LOG.error("Error: parsing timestamp has went beyond length of the message");
							continue;
						}
						// If the next char is a space, skip that too.
						if (pos < length && bytes[pos] == ' ')
						{
							pos++;
						}
					} 
					else
					{
						if (version == (byte) 0x00)
						{
							LOG.debug("[{}] Failed to parse timestamp.  Using stored timestamp", getPartitionId());
							timestamp = Converter.longFromBytes(bytes, 2);
						} 
						else
						{
							LOG.error("[{}] Error parsing timestamp.", getPartitionId());
							timestamp = System.currentTimeMillis();
						}
					}

					lag_sec = (int) (System.currentTimeMillis() - timestamp) / 1000;
					
					if (lag_sec < 0) 
					{
						lag_sec = 0;
					}

					if ((length - pos) < 0)
					{
						LOG.info("[{}] Skipping offset as length - Offset is < 0: timestamp: {}, pos: {}, length: {}", getPartitionId(), timestamp, pos, length);
						continue;
					}					
					
					for (TimeBasedHdfsOutputPath path : getHdfsOutputPaths())
					{
						if (!path.isConfigured())
						{
							String filename = String.format("%s-%08d.bm", getPartitionId(), offset);
							path.configure(filename);
						}
						
						path.getBoomWriter(timestamp).writeLine(timestamp, bytes, pos, length - pos);
					}

					/*
					 * Let's track the max timestamp and write that to ZK for the 
					 * partitions oldest offset timestamp.  Previously we used the last
					 * message's timestamp however that could lead to problems if there
					 * was ever corruption of the timestamp or if there's significant
					 * skew in the log's time.
					 */
					
					if (timestamp > maxTimestamp || maxTimestamp == -1)
					{
						maxTimestamp = timestamp;
					}

				} 
				catch (Throwable t)
				{
					LOG.error("[{}] Error processing message.", getPartitionId(), t);
					LOG.info("[{}] Deleting all tmp files", getPartitionId());
					
					for (TimeBasedHdfsOutputPath path : getHdfsOutputPaths())
					{
						path.abortAll();
					}								
					
					return;
				}
			}
			
			LOG.info("[{}] KaBoom client shutting down and closing all output files.", getPartitionId());
			
			for (TimeBasedHdfsOutputPath path : getHdfsOutputPaths())
			{
				path.closeAll();
			}			
			
			try
			{
				storeOffset();
				storeOffsetTimestamp(getPartitionId(), maxTimestamp);
			
				LOG.info("[{}] storing offset {} and max timestamp {} into ZooKeeper.", getPartitionId(), offset, maxTimestamp);
			} 
			catch (Exception e)
			{
				LOG.error("[{}] Error storing offset {} and timestamp {} in ZooKeeper", getPartitionId(), e, offset, maxTimestamp);
			}

			MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(lagGaugeName);
			MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(lagSecGaugeName);
			MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(msgWrittenGaugeName);
			
			LOG.info("[{}] Worker stopped. (Read {} lines.  Next offset is {})", getPartitionId(), linesread, offset);
		}
		catch (Exception e) 
		{
			LOG.error("[{}] An exception occured while setting up this worker thread giving up and returing => error : {} ", getPartitionId(), e);
		}
		finally
		{
			synchronized (workersLock)
			{
				workers.remove(this);
			}
		}
	}

	private void storeOffset() throws Exception
	{
		if (curator.checkExists().forPath(zkPath) == null)
		{
			curator.create().creatingParentsIfNeeded()
				 .withMode(CreateMode.PERSISTENT).forPath(zkPath, Converter.getBytes(offset));
		} else
		{
			curator.setData().forPath(zkPath, Converter.getBytes(offset));
		}

	}

	/*
	 * Get the offset in ZK, but if an override exists, perfer it instead.
	 */
	
	private long getOffset() throws Exception
	{
		if (curator.checkExists().forPath(zkPath) == null)
		{
			return 0L;
		} 
		else
		{
			long zkOffset = Converter.longFromBytes(curator.getData().forPath(zkPath), 0);

			if (curator.checkExists().forPath(zkPath_offSetOverride) != null)
			{
				long zkOffsetOverride = Converter.longFromBytes(curator.getData().forPath(zkPath_offSetOverride), 0);

				if (config.getAllowOffsetOverrides())
				{
					LOG.warn("{} : offset in ZK is {} but an override of {} exists and allowOffsetOverride={}", this.getPartitionId(), zkOffset, zkOffsetOverride, config.getAllowOffsetOverrides());
					
					curator.delete().forPath(zkPath_offSetOverride);
					
					LOG.info("{} successfully deleted offset override ZK path: {}", this.getPartitionId(), zkPath_offSetOverride);
					
					return zkOffsetOverride;
				} 
				else
				{
					LOG.warn("{} : offset in ZK is {} and an override of {} exists however allowOffsetOverride={}", this.getPartitionId(), zkOffset, zkOffsetOverride, config.getAllowOffsetOverrides());
				}
			}

			return zkOffset;
		}
	}
	
	/**
	 * Stores the partitions offset timestamp in ZK
	 *
	 * @throws Exception
	 */
	
	private void storeOffsetTimestamp(String partitionId, long offsetTimestamp) throws Exception
	{
		if (offsetTimestamp == -1)
		{
			LOG.info("Partition {} has a -1 offsetTime and will not be written to ZK", partitionId);
			return;
		}

		if (curator.checkExists().forPath(zkPath_offSetTimestamp) == null)
		{
			curator.create().creatingParentsIfNeeded()
				 .withMode(CreateMode.PERSISTENT)
				 .forPath(zkPath_offSetTimestamp, Converter.getBytes(offsetTimestamp));
		} 
		else
		{
			curator.setData().forPath(zkPath_offSetTimestamp,
				 Converter.getBytes(offsetTimestamp));
		}
	}

	/**
	 * Returns the partitions offset timestamp from ZK
	 *
	 * @return the offset timestamp
	 * @throws Exception
	 */
	
	private long getOffsetTimestamp() throws Exception
	{
		if (curator.checkExists().forPath(zkPath_offSetTimestamp) == null)
		{
			return 0L;
		} 
		else
		{
			return Converter.longFromBytes(curator.getData().forPath(zkPath_offSetTimestamp), 0);
		}
	}

	public void stop()
	{
		LOG.info("Stop request received.");
		stopping = true;
	}

	public long getLag()
	{
		return lag;
	}

	public int getLagSec()
	{
		return lag_sec;
	}

	public long getMsgWrittenPerSec()
	{
		return linesread / ((System.currentTimeMillis() - startTime) / 1000);
	}

	/**
	 * @return the hdfsOutputPaths
	 */
	public ArrayList<TimeBasedHdfsOutputPath> getHdfsOutputPaths()
	{
		return hdfsOutputPaths;
	}

	/**
	 * @return the partitionId
	 */
	public String getPartitionId()
	{
		return partitionId;
	}

	/**
	 * @return the topic
	 */
	public String getTopic()
	{
		return topic;
	}

	/**
	 * @return the partition
	 */
	public int getPartition()
	{
		return partition;
	}

	/**
	 * @param partition the partition to set
	 */
	public void setPartition(int partition)
	{
		this.partition = partition;
	}
}
