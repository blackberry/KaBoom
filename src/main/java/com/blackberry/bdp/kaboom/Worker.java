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

import java.text.SimpleDateFormat;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.kaboom.api.KaBoomTopicConfig;
import com.blackberry.bdp.common.conversion.Converter;
import com.blackberry.bdp.common.jmx.MetricRegistrySingleton;
import com.blackberry.bdp.krackle.consumer.Consumer;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Meter;
import org.apache.hadoop.fs.Path;

public class Worker implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

	/**
	 * @return the ZK_ROOT
	 */
	public static String getZK_ROOT() {
		return ZK_ROOT;
	}

	private String partitionId;
	private Consumer consumer;
	private long lowerOffsetsReceived = 0;
	private long timestamp;
	private long lastMessageReceivedTimestamp = -1;
	private long lastForcedZkOffsetTimestampStore = -1;
	private boolean stopping = false;
	private String hostname;
	private StartupConfig config;
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
	private long messagesWritten = 0;
	private String lagGaugeName;
	private String lagSecGaugeName;
	private String msgWrittenGaugeName;
	private String lowerOffsetsGaugeName;
	private Meter boomWritesMeter;
	private Meter boomWritesMeterTopic;
	private Meter boomWritesMeterTotal;
	private TimeBasedHdfsOutputPath hdfsOutputPath;
	private static Set<Worker> workers = new HashSet<>();
	private static final Object workersLock = new Object();
	private Boolean pinged = false;
	private Boolean pong = false;
	private Boolean killed = false;
	private KaBoomTopicConfig topicConfig;	
	private long sprintCounter;
	private WorkSprint previousSprint;
	private WorkSprint currentSprint;

	static {
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:max message lag sec", new Gauge<Integer>() {
				 @Override
				 public Integer getValue() {
					 int maxLagSec = 0;
					 synchronized (workersLock) {
						 for (Worker w : workers) {
							 maxLagSec = Math.max(maxLagSec, w.getLagSec());
						 }
					 }
					 return maxLagSec;
				 }

			 });
	}

	static {
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:sum message lag sec", new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 long sumLag = 0;
					 synchronized (workersLock) {
						 for (Worker w : workers) {
							 sumLag += w.getLagSec();
						 }
					 }
					 return sumLag;
				 }

			 });
	}

	static {
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:avg message lag sec", new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 long sumLag = 0;
					 int count = 0;
					 synchronized (workersLock) {
						 for (Worker w : workers) {
							 count++;
							 sumLag += w.getLagSec();
						 }
					 }
					 long avgLag = sumLag / count;
					 return avgLag;
				 }

			 });
	}

	static {
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:max message lag", new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 long maxLag = 0;
					 synchronized (workersLock) {
						 for (Worker w : workers) {
							 maxLag = Math.max(maxLag, w.getLag());
						 }
					 }
					 return maxLag;
				 }

			 });
	}

	static {
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:sum message lag", new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 long sumLag = 0;
					 synchronized (workersLock) {
						 for (Worker w : workers) {
							 sumLag += w.getLag();
						 }
					 }
					 return sumLag;
				 }

			 });
	}

	static {
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:avg message lag", new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 long sumLag = 0;
					 int count = 0;
					 synchronized (workersLock) {
						 for (Worker w : workers) {
							 count++;
							 sumLag += w.getLag();
						 }
					 }
					 long avgLag = sumLag / count;
					 return avgLag;
				 }

			 });
	}

	static {
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:avg messages written per sec", new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 long sumMsgWritten = 0;
					 int count = 0;
					 synchronized (workersLock) {
						 for (Worker w : workers) {
							 count++;
							 sumMsgWritten += w.getMsgWrittenPerSec();
						 }
					 }
					 return sumMsgWritten / count;
				 }

			 });
	}

	static {
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register("kaboom:total:total messages written per sec", new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 long sumMsgWritten = 0;
					 synchronized (workersLock) {
						 for (Worker w : workers) {
							 sumMsgWritten += w.getMsgWrittenPerSec();
						 }
					 }
					 return sumMsgWritten;
				 }
			 });
	}

	public Worker(StartupConfig config, CuratorFramework curator, String topic, int partition) throws Exception {
		this.sprintCounter = 0;
		this.config = config;
		this.curator = curator;
		this.topic = topic;
		this.partition = partition;
		this.startTime = System.currentTimeMillis();
		this.messagesWritten = 0;
		this.topicConfig = KaBoomTopicConfig.get(config.getCurator(), ZK_ROOT + "/topics/" + topic);		
		this.boomWritesMeterTopic = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:topic:" + topic + ":boom writes");
		this.boomWritesMeterTotal = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:total:boom writes");
		
		this.hdfsOutputPath = new TimeBasedHdfsOutputPath(
			 config.authenticatedFsForProxyUser(topicConfig.getProxyUser()),
			 config,
			 topicConfig);

		partitionId = topic + "-" + partition;
		hdfsOutputPath.setPartition(partition);
		hdfsOutputPath.setPartitionId(partitionId);		
		
		LOG.info("\t {} {} => {}", config.getKaboomId(), partitionId, hdfsOutputPath);
		LOG.info("Worker instantiated for {} and configured", partitionId);

		this.boomWritesMeter = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:partitions:" + partitionId + ":boom writes");

		lagGaugeName = "kaboom:partitions:" + partitionId + ":message lag";
		lagSecGaugeName = "kaboom:partitions:" + partitionId + ":message lag sec";
		msgWrittenGaugeName = "kaboom:partitions:" + partitionId + ":messages written per second";
		lowerOffsetsGaugeName = "kaboom:partitions:" + partitionId + ":early offsets received";

		String[] metrics_to_remove = {lagGaugeName, lagSecGaugeName, msgWrittenGaugeName, lowerOffsetsGaugeName};

		for (final String metric_name : metrics_to_remove) {
			if (MetricRegistrySingleton.getInstance().getMetricsRegistry()
				 .getGauges(new MetricFilter() {
					 @Override
					 public boolean matches(String s, Metric m) {
						 return s.equals(metric_name);
					 }

				 }).size() > 0) {
				LOG.debug("Removing existing metric: '{}'", metric_name);
				MetricRegistrySingleton.getInstance().getMetricsRegistry()
					 .remove(metric_name);
			}
		}

		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register(lagGaugeName, new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 return lag;
				 }

			 });

		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register(lagSecGaugeName, new Gauge<Integer>() {
				 @Override
				 public Integer getValue() {
					 return lag_sec;
				 }

			 });

		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register(msgWrittenGaugeName, new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 return messagesWritten / ((System.currentTimeMillis() - startTime) / 1000);
				 }

			 });

		MetricRegistrySingleton.getInstance().getMetricsRegistry()
			 .register(lowerOffsetsGaugeName, new Gauge<Long>() {
				 @Override
				 public Long getValue() {
					 return lowerOffsetsReceived;
				 }

			 });

		synchronized (workersLock) {			
			workers.add(this);
		}

		LOG.info("[{}] Created worker.", partitionId);
	}

	private static String dateString(Long ts) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date now = new Date();
		String strDate = sdf.format(ts);
		return strDate;
	}

	@Override
	public void run() {
		try {
			hdfsOutputPath.setKaboomWorker(this);
			zkPath = getZK_ROOT() + "/topics/" + getTopic() + "/" + getPartition();
			zkPath_offSetTimestamp = zkPath + "/offset_timestamp";
			zkPath_offSetOverride = zkPath + "/offset_override";		
			
			try {
				previousSprint = null;
				currentSprint = new WorkSprint(getStoredOffsetFromZk());
				hostname = InetAddress.getLocalHost().getCanonicalHostName();
			} catch (UnknownHostException uhe) {
				LOG.error("[{}] Can't determine local hostname", getPartitionId());
				hostname = "unknown.host";				
			} catch (Exception e) {
				LOG.error("[{}] Error getting offset.", getPartitionId(), e);
				return;
			}
			
			String clientId = "kaboom-" + hostname;

			consumer = new Consumer(config.getConsumerConfiguration(), 
				 clientId, getTopic(), 
				 getPartition(), 
				 currentSprint.offset, 
				 MetricRegistrySingleton.getInstance().getMetricsRegistry());

			LOG.info("[{}] Created worker.  Starting at offset {}.", getPartitionId(), currentSprint.offset);

			byte[] bytes = new byte[1024 * 1024];
			int length;
			byte version;
			int pos;
			PriParser pri = new PriParser();
			VersionParser ver = new VersionParser();
			TimestampParser tsp = new TimestampParser();
			int topicConfigVersion = topicConfig.getVersion();
			
			while (stopping == false) {
				try {					
					if (pinged) {
						pong = true;
					}
					if (killed || Thread.interrupted()) {
						throw new Exception("A kill request/interrupt has been received");
					}
					if (currentSprint.sprintEnd <= System.currentTimeMillis()) {
						previousSprint = currentSprint;
						currentSprint = new WorkSprint(previousSprint.offset);
					} else if (previousSprint != null  && System.currentTimeMillis() - previousSprint.sprintEnd 
						 >=  config.getRunningConfig().getFileCloseGraceTimeAfterExpiredMs()) {						
						previousSprint.finish();
						previousSprint = null;						
					}
					
					length = consumer.getMessage(bytes, 0, bytes.length);
					if (length == -1) {
						continue;
					}
					
					/**
					 * offset always refers to the next offset we expect and
					 * since we just called consumer.getMessage() let's see
					 * if the offset of the last message is what we expected
					 * and handle the fun edge cases when it's not
					 */					
					if (currentSprint.offset != consumer.getLastOffset()) {						
						long highWatermark = consumer.getHighWaterMark();						
						if (currentSprint.offset > consumer.getLastOffset()) {	
							if (currentSprint.offset < highWatermark) {
								/* 
								 * When using SNAPPY compression in Krackle's consumer there will be messages received
								 * that are in the snappy block that are from earlier than our requested offset.  When this
								 * happens the consumer will continue to send us messages from within that block so we
								 * should just be patient until the offsets are from where we want.  
								 */
								LOG.debug("[{}] skipping last offset {} since earlier than our requested offset 's and lower than high watermark {}", 
									 getPartitionId(), 
									 consumer.getLastOffset(), 
									 highWatermark);
								lowerOffsetsReceived++;
								continue;
							} else if (currentSprint.offset > highWatermark) {
								/*	
								 *	If the expected offset is greater than than actual offset and also higher than the high watermark 
								 *	then perhaps the broker we're receiving messages from has changed and the new broker has a 
								 *	lower offset because it was behind when it took over... Maybe?  
								 */
								if (config.getRunningConfig().getSinkToHighWatermark()) {
									LOG.warn("[{}] offset {} is greater than high watermark {} and sinkToHighWatermark is {}, sinking to high watermark.",
										 getPartitionId(), currentSprint.offset, highWatermark, config.getRunningConfig().getSinkToHighWatermark());
									consumer.setNextOffset(highWatermark);
									currentSprint.offset = highWatermark;

									LOG.info("[{}] Successfully set offset to the high watermark of {}", getPartitionId(), highWatermark);

									continue;
								} else {
									LOG.error("[{}] offset {} is greater than high watermark {} and sinkToHighWatermark is {}, ignoring offset and skipping message.",
										 getPartitionId(), currentSprint.offset, highWatermark, config.getRunningConfig().getSinkToHighWatermark());
									continue;
								}
							} else {									
								LOG.error("[{}] Unhandled edge case: offset > last offset && offset == high watermark");
								continue;
							}
						} else {
							LOG.error("[{}] Offset anomaly! Expected:{}, Got {}, Consumer high watermark {}, latest {}, earliest {}", 
								 partitionId,
								 currentSprint.offset,
								 consumer.getLastOffset(),
								 consumer.getHighWaterMark(),
								 consumer.getLatestOffset(),
								 consumer.getEarliestOffset());
						}
					}

					messagesWritten++;
					currentSprint.offset = consumer.getNextOffset();
					lag = consumer.getHighWaterMark() - currentSprint.offset;

					// (byte) 0xFE: -2
					// (byte) 0x00: 0
					// (byte) 0xFF: -1
					// Check for version
					if (bytes[0] == (byte) 0xFE) {
						version = bytes[1];
						if (version == (byte) 0x00) {
							// Version 0 has a timestamp in the front, so we can skip that for now. Come back if we need it.
							pos = 10;
						} else {
							LOG.warn("[{}] Unrecognized encoding version: {}", getPartitionId(), version);
							pos = 0;
						}
					} else {
						// version -1 is a raw log
						version = (byte) 0xFF;
						pos = 0;
					}

					// Optional PRI at the start of the line.
					if (pri.parsePri(bytes, pos, length)) {
						pos += pri.getPriLength();
					}

					// On the off chance that someone is following RFC5424 and has
					// inserted a version in the log line.
					if (ver.parseVersion(bytes, pos, length - pos)) {
						// Skip the length of the version and the following space.
						pos += ver.getVersionLength() + 1;
					}

					tsp.parse(bytes, pos, length - pos);

					if (tsp.getError() == TimestampParser.NO_ERROR) {
						timestamp = tsp.getTimestamp();
						// Move position to the end of the timestamp						
						pos += tsp.getLength();
						/**
						 * mbruce: occasionally we get a line that is truncated partway through the timestamp,
						 * however we still have the rest of the last message in the byte buffer and parsing the 
						 * timestamp will push us past then end of the line
						 */
						if (pos > length) {
							LOG.error("Error: parsing timestamp has went beyond length of the message");
							continue;
						}
						// If the next char is a space, skip that too.
						if (pos < length && bytes[pos] == ' ') {
							pos++;
						}
					} else {
						if (version == (byte) 0x00) {
							LOG.debug("[{}] Failed to parse timestamp.  Using stored timestamp", getPartitionId());
							timestamp = Converter.longFromBytes(bytes, 2);
						} else {
							LOG.error("[{}] Error parsing timestamp.", getPartitionId());
							timestamp = System.currentTimeMillis();
						}
					}

					lag_sec = (int) (System.currentTimeMillis() - timestamp) / 1000;

					if (lag_sec < 0) {
						lag_sec = 0;
					}

					if ((length - pos) < 0) {
						LOG.info("[{}] Skipping offset as length - Offset is < 0: timestamp: {}, pos: {}, length: {}", getPartitionId(), timestamp, pos, length);
						continue;
					}

					hdfsOutputPath.getBoomWriter(
						 currentSprint.sprintNumber,
						 timestamp, 
						 partitionId + "-" + currentSprint.offset + ".bm",
						 currentSprint.paths).writeLine(timestamp, bytes, pos, length - pos);
					
					boomWritesMeter.mark();
					boomWritesMeterTopic.mark();
					boomWritesMeterTotal.mark();
					lastMessageReceivedTimestamp = System.currentTimeMillis();

					currentSprint.checkTimestamp(timestamp);
					
				} catch (Exception e) {
					LOG.error("[{}] Error processing message: ", partitionId, e);
					LOG.info("[{}] Calling abort on {}", partitionId, hdfsOutputPath);
					hdfsOutputPath.abortAll();					
					return;
				}
			}

			LOG.info("[{}] KaBoom client shutting down and closing all output files.", getPartitionId());

			MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(lagGaugeName);
			MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(lagSecGaugeName);
			MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(msgWrittenGaugeName);

			try {
				hdfsOutputPath.closeAll();
				LOG.info("[{}] storing offset {} and max timestamp {} into ZooKeeper.",
					 partitionId, 
					 currentSprint.offset, 
					 currentSprint.maxMessageTimestamp);
				currentSprint.storeOffset();
				currentSprint.storeOffsetTimestamp();
			} catch (Exception e) {
				LOG.error("[{}] Error storing offset {} and timestamp {} in ZooKeeper", 
					 partitionId, 
					 currentSprint.offset, 
					 currentSprint.maxMessageTimestamp, 
					 e);
			}
			LOG.info("[{}] Worker stopped. (Read {} lines.  Next offset is {})", partitionId, messagesWritten, currentSprint.offset);
		} catch (Exception e) {
			LOG.error("[{}] An exception occured while setting up this worker thread", getPartitionId(), e);
		} finally {
			synchronized (workersLock) {
				workers.remove(this);
			}
		}
	}

	private long getStoredOffsetFromZk() throws Exception {
		if (curator.checkExists().forPath(zkPath) == null) {
			LOG.info("[{}] offset path {} does not exist, returning zero", partitionId, zkPath);
			return 0L;
		} else {
			long zkOffset = Converter.longFromBytes(curator.getData().forPath(zkPath), 0);

			LOG.info("[{}] offset {} found in path {}", partitionId, zkOffset, zkPath);

			if (curator.checkExists().forPath(zkPath_offSetOverride) != null) {
				long zkOffsetOverride = Converter.longFromBytes(curator.getData().forPath(zkPath_offSetOverride), 0);

				if (config.getRunningConfig().getAllowOffsetOverrides()) {
					LOG.warn("{} : offset in ZK is {} but an override of {} exists and allowOffsetOverride={}",
						 this.getPartitionId(), zkOffset, zkOffsetOverride, config.getRunningConfig().getAllowOffsetOverrides());
					curator.delete().forPath(zkPath_offSetOverride);
					LOG.info("{} successfully deleted offset override ZK path: {}", this.getPartitionId(), zkPath_offSetOverride);
					return zkOffsetOverride;
				} else {
					LOG.warn("{} : offset in ZK is {} and an override of {} exists however allowOffsetOverride={}",
						 this.getPartitionId(), zkOffset, zkOffsetOverride, config.getRunningConfig().getAllowOffsetOverrides());
				}
			}
			return zkOffset;
		}
	}
	
	private class WorkSprint{
		private long sprintStart;
		private long sprintEnd;
		private final ArrayList<Path> paths;
		private long offset;
		private long maxMessageTimestamp;
		private final long sprintNumber;
		
		private WorkSprint(long startingOffset) {			
			sprintCounter++;
			calculateSprintTimes();
			this.offset = startingOffset;
			this.paths = new ArrayList<>();			
			this.sprintNumber = sprintCounter;
			this.maxMessageTimestamp = -1;
			LOG.info("[{}] New sprint #{} for offest {} start time is {} and end time is {}", 
				 partitionId, 
				 sprintNumber, 
				 this.offset,
				 dateString(sprintStart), 
				 dateString(sprintEnd));
		}
		
		private void checkTimestamp(long timestamp) {
			if (timestamp > maxMessageTimestamp) {
				maxMessageTimestamp = timestamp;
			}
		}
		
		private void calculateSprintTimes() {
			sprintStart = System.currentTimeMillis() - System.currentTimeMillis() 
				 % (config.getRunningConfig().getWorkerSprintDurationSeconds() * 1000);
			sprintEnd = sprintStart + config.getRunningConfig().getWorkerSprintDurationSeconds() * 1000;			
		}
		
		private void finish() {
			try {
				LOG.info("[{}] Sprint ending at {} is finished", partitionId, dateString(sprintEnd));
				hdfsOutputPath.closeOffSprint(sprintNumber);
				storeOffset();
				storeOffsetTimestamp();
				
			} catch (Exception e) {
				LOG.error("[{}] There was an error closing off a sprint: ", partitionId, e);
			}
			
		}
		private void storeOffsetTimestamp() throws Exception {
			if (maxMessageTimestamp == -1) {
				LOG.info("[{}] -1 offsetTimestamp will not be written to ZK", partitionId);
				return;
			}

			if (curator.checkExists().forPath(zkPath_offSetTimestamp) == null) {
				curator.create().creatingParentsIfNeeded()
					 .withMode(CreateMode.PERSISTENT)
					 .forPath(zkPath_offSetTimestamp, Converter.getBytes(maxMessageTimestamp));
			} else {
				curator.setData().forPath(zkPath_offSetTimestamp,
					 Converter.getBytes(maxMessageTimestamp));
			}

			LOG.info("[{}] stored offset timestamp in ZK {} ({})", 
				 partitionId, 
				 maxMessageTimestamp, 
				 dateString(maxMessageTimestamp));
		}
		
		private void storeOffset() throws Exception {
			if (curator.checkExists().forPath(zkPath) == null) {
				curator.create().creatingParentsIfNeeded()
					 .withMode(CreateMode.PERSISTENT).forPath(zkPath, Converter.getBytes(offset));
				LOG.info("[{}] new ZK path created to write offset {} to {}", partitionId, offset, zkPath);
			} else {
				curator.setData().forPath(zkPath, Converter.getBytes(offset));
				LOG.info("[{}] wrote offset {} to existing path {}", partitionId, offset, zkPath);
			}
		}
	}
	
	public void stop() {
		LOG.info("[{}] Stop request received", partitionId);
		stopping = true;
	}

	public void kill() {
		LOG.info("[{}] Kill request received", partitionId);
		killed = true;
	}

	public void ping() {
		this.pinged = true;
		this.pong = false;
	}

	public long getLag() {
		return lag;
	}

	public int getLagSec() {
		return lag_sec;
	}

	public long getMsgWrittenPerSec() {
		return messagesWritten / ((System.currentTimeMillis() - startTime) / 1000);
	}

	public String getPartitionId() {
		return partitionId;
	}

	public String getTopic() {
		return topic;
	}

	public int getPartition() {
		return partition;
	}

	public Boolean pinged() {
		return pinged;
	}

	public Boolean getPong() {
		return pong;
	}
}
