package com.blackberry.kaboom;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.krackle.MetricRegistrySingleton;
import com.blackberry.krackle.consumer.Consumer;
import com.blackberry.krackle.consumer.ConsumerConfiguration;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import com.blackberry.common.conversion.Converter;

public class Worker implements Runnable
{
	private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
	private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

	private String partitionId;

	private Consumer consumer;
	private long offset;
	private Boolean allowOffsetOverride = false;
	private Boolean sinkToHighWatermark = false;
	private long lowerOffsetsReceived = 0;
	private long timestamp;
	private boolean stopping = false;

	private static final Object fsLock = new Object();
	private FileSystem fs;

	private String hostname;
	private String template;

	private long hour;
	private OutputFile outputFile;
	private Map<Long, OutputFile> outputFileMap = new HashMap<Long, OutputFile>();

	private Configuration hConf;
	private String proxyUserName;
	private FsPermission permissions = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);
	private int bufferSize = 16 * 1024;
	private short replicas = 3;
	private long blocksize = 256 * 1024 * 1024;

	private CuratorFramework curator;
	private static final String ZK_ROOT = "/kaboom";
	private String zkPath;
	private String zkPath_offSetTimestamp;
	private String zkPath_offSetOverride;

	private String topic;
	private int partition;
	private ConsumerConfiguration consumerConfig;

	private long startTime;
	private long endTime;

	private long lag = 0;
	private int lag_sec = 0;
	private long linesread = 0;

	private String lagGaugeName;
	private String lagSecGaugeName;
	private String msgWrittenGaugeName;
	private String lowerOffsetsGaugeName;

	private static Set<Worker> workers = new HashSet<Worker>();
	private static Object workersLock = new Object();

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

	public Worker(ConsumerConfiguration consumerConfig, Configuration hConf,
		 CuratorFramework curator, String topic, int partition, long runDuration,
		 String template) throws Exception
	{
		this(consumerConfig, hConf, curator, topic, partition, runDuration,
			 template, "", false, false);
	}

	public Worker(ConsumerConfiguration consumerConfig, Configuration hConf,
		 CuratorFramework curator, String topic, int partition, long runDuration,
		 String template, String proxyUser, Boolean offsetOverride, Boolean sinkToHighWatermark) throws Exception
	{
		this.endTime = System.currentTimeMillis() + runDuration;
		this.hConf = hConf;
		this.template = template;
		this.curator = curator;
		this.proxyUserName = proxyUser;
		this.topic = topic;
		this.partition = partition;
		this.consumerConfig = consumerConfig;
		this.startTime = System.currentTimeMillis();
		this.linesread = 0;
		this.allowOffsetOverride = offsetOverride;
		this.sinkToHighWatermark = sinkToHighWatermark;

		partitionId = topic + "-" + partition;

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
			workers.add(this);
		}
		
		LOG.info("[{}] Created worker.", partitionId);
	}

	@Override
	public void run()
	{
		try
		{
			zkPath = ZK_ROOT + "/topics/" + topic + "/" + partition;
			zkPath_offSetTimestamp = zkPath + "/offset_timestamp";
			zkPath_offSetOverride = zkPath + "/offset_override";
						

			try
			{
				offset = getOffset();
			} 
			catch (Exception e)
			{
				LOG.error("[{}] Error getting offset.", partitionId, e);
				return;
			}

			try
			{
				hostname = InetAddress.getLocalHost().getCanonicalHostName();
			} 
			catch (UnknownHostException e)
			{
				LOG.error("[{}] Can't determine local hostname", partitionId);
				hostname = "unknown.host";
			}

			String clientId = "kaboom-" + hostname;

			consumer = new Consumer(consumerConfig, clientId, topic, partition,offset, MetricRegistrySingleton.getInstance().getMetricsRegistry());

			LOG.info("[{}] Created worker.  Starting at offset {}.", partitionId, offset);

			byte[] bytes = new byte[1024 * 1024];
			int length = -1;
			byte version = -1;
			int pos = 0;			
			PriParser pri = new PriParser();
			VersionParser ver = new VersionParser();
			TimestampParser tsp = new TimestampParser();
			long maxTimestamp = -1;
			
			while (System.currentTimeMillis() < endTime)
			{
				try
				{
					if (stopping)
					{
						LOG.info("[{}] Stopping Worker.", partitionId);
						break;
					}

					length = consumer.getMessage(bytes, 0, bytes.length);

					if (length == -1)
					{
						continue;
					}

					// offset always refers to the next offset we expect to 
					// since we just called consumer.getMessage() let's see
					// if the offset of the last message is what we expected
					// and handle the fun edge cases when it's not
					
					if (offset == consumer.getLastOffset())
					{
						offset = consumer.getNextOffset();
					} 
					else
					{
						if (offset > consumer.getLastOffset())
						{
							if (offset < consumer.getHighWaterMark())
							{
								/* 
								 * When using SNAPPY compression in Krackle's consumer there will be messages received
								 * that are in the snappy block that are from earlier than our requested offset.  When this
								 * happens the consumer will continue to send us messages from within that block so we
								 * should just be patient until the offsets are from where we want.  
								*/

								LOG.debug("[{}] skipping last offset {} since it's lower than high watermark {}",
									 partitionId, consumer.getLastOffset(), consumer.getHighWaterMark());
								
								lowerOffsetsReceived++;
								continue;
							} 
							else
							{
								if (offset > consumer.getHighWaterMark())
								{
									/*	
									 *	If the expected offset is greater than than actual offset and also higher than the high watermark 
									 *	then perhaps the broker we're receiving messages from has changed and the new broker has a 
									 *	lower offset because it was behind when it took over... Maybe?  
									 */

									LOG.warn("[{}] last offset {} (expected offset {} and higher than high watermark ({})",
										 partitionId, consumer.getLastOffset(), offset, consumer.getHighWaterMark());

									if (sinkToHighWatermark)
									{
										LOG.warn("[{}] Resetting offset to high watermark {} since sinkToHighWatermark is {}",
											 partitionId, consumer.getLastOffset(), offset, sinkToHighWatermark);

										consumer.setNextOffset((consumer.getHighWaterMark()));
										offset = consumer.getHighWaterMark();

										LOG.info("[{}] Successfully set offset to the high watermark of {}",
											 partitionId, consumer.getHighWaterMark());

										continue;
									} 
									else
									{
										LOG.error("[{}] sinkToHighWatermark is {}, ignoring offset and skipping message.",
											 partitionId, consumer.getLastOffset(), offset, sinkToHighWatermark);

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
						} 
						else
						{
							LOG.error("[{}] Offset anomaly! Expected:{}, Got {}, Consumer high watermark {}, latest {}, earliest {}", 
								 partitionId, 
								 offset, 
								 consumer.getLastOffset(), 
								 consumer.getHighWaterMark(), 
								 consumer.getLatestOffset(),
								 consumer.getEarliestOffset());
						}
					}

					linesread++;
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
							LOG.warn("[{}] Unrecognized encoding version: {}", partitionId, version);
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
							LOG.debug("[{}] Failed to parse timestamp.  Using stored timestamp", partitionId);
							timestamp = Converter.longFromBytes(bytes, 2);
						} 
						else
						{
							LOG.error("[{}] Error parsing timestamp.", partitionId);
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
						LOG.info("[{}] Length - Offset is < 0: timestamp: {}, pos: {}, length: {}", partitionId, timestamp, pos, length);
					}

					getBoomWriter(timestamp).writeLine(timestamp, bytes, pos, length - pos);

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
					LOG.error("[{}] Error processing message.", partitionId, t);
					LOG.info("[{}] Deleting all tmp files", partitionId);
					
					for (Entry<Long, OutputFile> entry : outputFileMap.entrySet())
					{
						entry.getValue().abort();
					}
					return;
				}
			}

			LOG.info("[{}] Closing all output files.", partitionId);
			
			for (Entry<Long, OutputFile> entry : outputFileMap.entrySet())
			{
				try
				{
					entry.getValue().getBoomWriter().close();
					entry.getValue().close();
				} 
				catch (IOException e)
				{
					LOG.error("[{}] Error closing output file", partitionId, e);
				}
			}

			try
			{
				storeOffset();
				storeOffsetTimestamp(partitionId, maxTimestamp);
			
				LOG.info("[{}] storing offset {} and max timestamp {} into ZooKeeper.", partitionId, offset, maxTimestamp);
			} 
			catch (Exception e)
			{
				LOG.error("[{}] Error storing offset/timestamp in ZooKeeper", partitionId, e);
			}

			MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(lagGaugeName);
			MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(lagSecGaugeName);
			MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(msgWrittenGaugeName);
			
			LOG.info("[{}] Worker stopped. (Read {} lines.  Next offset is {})", partitionId, linesread, offset);
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

				if (allowOffsetOverride)
				{
					LOG.warn("{} : offset in ZK is {} but an override of {} exists and allowOffsetOverride={}", 
						 this.partitionId, zkOffset, zkOffsetOverride, allowOffsetOverride);
					
					curator.delete().forPath(zkPath_offSetOverride);
					
					LOG.info("{} successfully deleted offset override ZK path: {}", this.partitionId, zkPath_offSetOverride);
					
					return zkOffsetOverride;
				} 
				else
				{
					LOG.warn("{} : offset in ZK is {} and an override of {} exists however allowOffsetOverride={}", 
						 this.partitionId, zkOffset, zkOffsetOverride, allowOffsetOverride);
					return zkOffset;
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

	private FastBoomWriter getBoomWriter(long timestamp) throws IOException
	{
		hour = timestamp - timestamp % (60 * 60 * 1000);
		outputFile = outputFileMap.get(hour);
		if (outputFile == null)
		{
			outputFile = new OutputFile(hour);
			outputFileMap.put(hour, outputFile);
		}

		return outputFile.getBoomWriter();
	}

	private class OutputFile
	{
		private String dir;
		private String tmpdir;
		private String filename;

		private Path finalPath;
		private Path tmpPath;

		private FastBoomWriter boomWriter;
		private OutputStream out;

		public OutputFile(long hour)
		{
			dir = Converter.timestampTemplateBuilder(hour, template);

			/*
			 * Related to IPGBD-1028 Output topic-partitionId-offset-incrementval.bm
			 */
			
			filename = String.format("%s-%08d.bm", partitionId, offset);
			tmpdir = String.format("%s/_tmp_%s-%08d.bm", dir, partitionId, offset);

			finalPath = new Path(dir + "/" + filename);
			tmpPath = new Path(tmpdir + "/" + filename);

			try
			{
				Authenticator.getInstance().runPrivileged(proxyUserName, new PrivilegedExceptionAction<Void>()
				 {
					 @Override
					 public Void run() throws Exception
					 {
						 synchronized (fsLock)
						 {
							 try
							 {
								 fs = tmpPath.getFileSystem(hConf);
							 } 
							 catch (IOException e)
							 {
								 LOG.error("Error getting File System.", e);
							 }
						 }

						 LOG.info("[{}] Opening {}.", partitionId, tmpPath);

						 if (fs.exists(tmpPath))
						 {
							 fs.delete(tmpPath, false);
							 LOG.info("[{}] Removing file from HDFS because it already exists: {}", partitionId, tmpPath.toString());
						 }

						 out = fs.create(tmpPath, permissions, false, bufferSize, replicas, blocksize, null);
						 boomWriter = new FastBoomWriter(out);
						 return null;
					 }
				 });
			} 
			catch (Exception e)
			{
				LOG.error("Error creating file.", e);
			}
		}

		public void abort()
		{
			LOG.info("[{}] Aborting output file.", partitionId);
			try
			{
				boomWriter.close();
			} 
			catch (IOException e)
			{
				LOG.error("[{}] Error closing boom writer.", partitionId, e);
			}
			
			try
			{
				out.close();
			} 
			catch (IOException e)
			{
				LOG.error("[{}] Error closing boom writer output file.", partitionId, e);
			}
			
			synchronized (fsLock)
			{
				try
				{
					fs = tmpPath.getFileSystem(hConf);
					fs.delete(new Path(tmpdir), true);
				} 
				catch (IOException e)
				{
					LOG.error("[{}] Error deleting temp files.", partitionId, e);
				}
			}
		}

		public FastBoomWriter getBoomWriter()
		{
			return boomWriter;
		}

		public void close() throws IOException
		{
			LOG.info("[{}] Closing {}.", partitionId, tmpPath);
			boomWriter.close();
			out.close();

			try
			{
				Authenticator.getInstance().runPrivileged(proxyUserName, new PrivilegedExceptionAction<Void>()
				 {
					 @Override
					 public Void run() throws Exception
					 {
						 synchronized (fsLock)
						 {
							 try
							 {
								 fs = tmpPath.getFileSystem(hConf);
							 } 
							 catch (IOException e)
							 {
								 LOG.error("[{}] Error getting File System.", partitionId, e);
							 }
						 }
						 try
						 {
							 LOG.info("[{}] Moving {} to {}.", partitionId, tmpPath,
								  finalPath);
							 fs.rename(tmpPath, finalPath);
						 } 
						 catch (Exception e)
						 {
							 LOG.error("[{}] Error renaming file.", partitionId, e);
							 abort();
						 }
						 
						 fs.delete(new Path(tmpdir), true);
						 return null;
					 }
				 });
			} 
			catch (Exception e)
			{
				LOG.error("[{}] Error creating file.", partitionId, e);
			}

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
}
