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

import java.text.SimpleDateFormat;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

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
import java.nio.charset.Charset;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.data.Stat;

public final class Worker extends AsyncAssignee implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
	protected static final Charset UTF8 = Charset.forName("UTF-8");

	private String partitionId;
	private Consumer consumer;
	private long lowerOffsetsReceived = 0;
	private long timestamp;
	private String hostname;
	private final StartupConfig config;

	private final String zkRoot;
	private String zkPath;
	private String zkPath_offSetTimestamp;
	private String zkPath_offSetOverride;

	private boolean stopping = false;
	private boolean aborting = false;
	private Boolean pinged = false;
	private Boolean pong = false;

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
	private Meter tsParseErrorsMeterTopic;
	private Meter priParseErrorsMeterTopic;
	private TimeBasedHdfsOutputPath hdfsOutputPath;
	private static Set<Worker> workers = new HashSet<>();
	private static final Object workersLock = new Object();
	private KaBoomTopicConfig topicConfig;
	private WorkerShift previousShift = null;
	private WorkerShift currentShift;
	private InterProcessMutex lock;

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

	public Worker(StartupConfig config, String topicName, int partition) throws Exception {
		// Set up our Synchronous Worker and obtain our assignment lock
		super(config.getKaBoomCurator(),
			 String.format("KaBoom Client ID=%d", config.getKaboomId()),
			 String.valueOf(config.getKaboomId()).getBytes(),
			 config.zkPathPartitionAssignment(topicName, partition),
			 config.getRunningConfig().getAssignmentLockTimeout());

		/**
		 * Since we're now holding a lock we need to ensure we're wrapping 
		 * everything in the constructor around a try block and releasing the 
		 * lock on any errors.  The run() method of the thread then releases 
		 * the lock in it's finally block, so there should be no way for a lock to
		 * not get released once the worker is finished.
		 */
		try {
			partitionId = String.format("%s-%d", topicName, partition);

			this.config = config;
			this.topic = topicName;
			this.partition = partition;
			this.startTime = System.currentTimeMillis();
			this.messagesWritten = 0;

			this.zkRoot = config.getZkRootPathKaBoom();
			this.topicConfig = KaBoomTopicConfig.get(KaBoomTopicConfig.class, config.getKaBoomCurator(), zkRoot + "/topics/" + topic);

			this.tsParseErrorsMeterTopic = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:topic:" + topic + ":timestamp parse errors");
			this.priParseErrorsMeterTopic = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:topic:" + topic + ":PRI parse errors");
			this.boomWritesMeterTopic = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:topic:" + topic + ":boom writes");
			this.boomWritesMeterTotal = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:total:boom writes");
			this.boomWritesMeter = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:partitions:" + partitionId + ":boom writes");
			this.hdfsOutputPath = new TimeBasedHdfsOutputPath(config, topicConfig, partition);

			zkPath = String.format("%s/%s/%d", config.getZkRootPathTopicConfigs(), topic, partition);
			zkPath_offSetTimestamp = zkPath + "/offset_timestamp";
			zkPath_offSetOverride = zkPath + "/offset_override";

			LOG.info("[{}] worker instantiated with topic configuration version {}", partitionId, topicConfig.getVersion());

			NodeCache nodeCache = new NodeCache(curator, topicConfig.getZkPath());
			nodeCache.getListenable().addListener(new NodeCacheListener() {
				@Override
				public void nodeChanged() throws Exception {
					Stat newZkStat = curator.checkExists().forPath(topicConfig.getZkPath());
					if (newZkStat == null) {
						LOG.info("[{}] topic configuration for {} has been deleted",
							 partitionId,
							 topicConfig.getId());
						abort();
					} else {
						KaBoomTopicConfig newTopicConfig = KaBoomTopicConfig.get(
							 KaBoomTopicConfig.class, curator, zkRoot + "/topics/" + topic);
						if (!newTopicConfig.getVersion().equals(topicConfig.getVersion())) {
							LOG.info("[{}] topic {} configuration (version {}) was updated to version {}, shutting down worker...",
								 partitionId,
								 topicConfig.getId(),
								 topicConfig.getVersion(),
								 newTopicConfig.getVersion());
							stop();
						}
					}
				}

			});
			nodeCache.start();

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
		} catch (Exception e) {
			//releaseAssignment();
			throw e;
		}
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
			aquireAssignment();
			try {
				currentShift = new WorkerShift();
				hostname = InetAddress.getLocalHost().getCanonicalHostName();
			} catch (UnknownHostException uhe) {
				LOG.error("[{}] Can't determine local hostname", getPartitionId());
				hostname = "unknown.host";
			} catch (Exception e) {
				LOG.error("[{}] Error: ",partitionId, e);
				return;
			}

			consumer = new Consumer(config.getConsumerConfiguration(),
				 "kaboom-" + hostname, 
				 getTopic(),
				 getPartition(),
				 currentShift.offset,
				 MetricRegistrySingleton.getInstance().getMetricsRegistry());

			LOG.info("[{}] Created worker with topic config version {} starting at offset {}.",
				 getPartitionId(),
				 topicConfig.getVersion(),
				 currentShift.offset);

			byte[] bytes = new byte[1024 * 1024];
			int length;
			byte version;
			int pos;
			PriParser pri = new PriParser();
			VersionParser ver = new VersionParser();
			TimestampParser tsp = new TimestampParser();

			while (stopping == false && aborting == false) {
				try {

					if (pinged) {
						pong = true;
					}

					if (paused) {
						Thread.sleep(100);
						continue;
					}

					if (currentShift.isOver()) {
						previousShift = currentShift;
						currentShift = new WorkerShift(previousShift);
					} else {
						if (previousShift != null && previousShift.isTimeToFinish()) {
							previousShift.finish(true);
							previousShift = null;
						}
					}

					length = consumer.getMessage(bytes, 0, bytes.length);
					if (length == -1) {
						continue;
					}

					/**
					 * offset always refers to the next offset we expect and since we just 
					 * called consumer.getMessage() let's see if the offset of the last message 
					 * is what we expected and handle the fun edge cases when it's not
					 */
					if (currentShift.offset != consumer.getLastOffset()) {
						long highWatermark = consumer.getHighWaterMark();
						if (currentShift.offset > consumer.getLastOffset()) {
							if (currentShift.offset < highWatermark) {
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
							} else {
								if (currentShift.offset > highWatermark) {
									/*	
									 *	If the expected offset is greater than than actual offset and also higher than the high watermark 
									 *	then perhaps the broker we're receiving messages from has changed and the new broker has a 
									 *	lower offset because it was behind when it took over... Maybe?  
									 */
									if (config.getRunningConfig().getSinkToHighWatermark()) {
										LOG.warn("[{}] offset {} is greater than high watermark {} and sinkToHighWatermark is {}, sinking to high watermark.",
											 getPartitionId(), currentShift.offset, highWatermark, config.getRunningConfig().getSinkToHighWatermark());
										consumer.setNextOffset(highWatermark);
										currentShift.offset = highWatermark;

										LOG.info("[{}] Successfully set offset to the high watermark of {}", getPartitionId(), highWatermark);

										continue;
									} else {
										LOG.error("[{}] offset {} is greater than high watermark {} and sinkToHighWatermark is {}, ignoring offset and skipping message.",
											 getPartitionId(), currentShift.offset, highWatermark, config.getRunningConfig().getSinkToHighWatermark());
										continue;
									}
								} else {
									LOG.error("[{}] Unhandled edge case: offset > last offset && offset == high watermark");
									continue;
								}
							}
						} else {
							LOG.error("[{}] Offset anomaly! Expected:{}, Got {}, Consumer high watermark {}, latest {}, earliest {}",
								 partitionId,
								 currentShift.offset,
								 consumer.getLastOffset(),
								 consumer.getHighWaterMark(),
								 consumer.getLatestOffset(),
								 consumer.getEarliestOffset());
						}
					}

					messagesWritten++;
					currentShift.offset = consumer.getNextOffset();
					lag = consumer.getHighWaterMark() - currentShift.offset;

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
					try {
						if (pri.parsePri(bytes, pos, length)) {
							pos += pri.getPriLength();
						}						
					} catch (Exception e) {
						priParseErrorsMeterTopic.mark();
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
						 * mbruce: occasionally we get a line that is truncated partway through the timestamp, however we still have the rest of the last message in the byte buffer and parsing the timestamp will push us past then end of the line
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
							LOG.debug("[{}] Error parsing timestamp.", getPartitionId());
							tsParseErrorsMeterTopic.mark();
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
						 currentShift.shiftNumber,
						 timestamp,
						 config.getKaboomId() + "-" + partitionId + "-" + currentShift.offset + ".bm").writeLine(timestamp, bytes, pos, length - pos);

					boomWritesMeter.mark();
					boomWritesMeterTopic.mark();
					boomWritesMeterTotal.mark();

					currentShift.checkTimestamp(timestamp);

				} catch (Exception e) {
					LOG.error("[{}] Error processing message: ", partitionId, e);
					LOG.info("[{}] Calling abort on {}", partitionId, hdfsOutputPath);
					abort();
				}
			}
			shutdown();
		} catch (Exception e) {
			LOG.error("[{}] An exception occured while setting up this worker thread", getPartitionId(), e);
		} finally {
			LOG.info("[{}] Worker finished after having processed {} events", partitionId, messagesWritten);
			try {
				releaseAssignment();
			} catch (Exception ex) {
				LOG.error("Failed to release assignment after worker thread finished: ", ex);
			}
			synchronized (workersLock) {
				workers.remove(this);
			}
		}
	}

	private void shutdown() {
		MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(lagGaugeName);
		MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(lagSecGaugeName);
		MetricRegistrySingleton.getInstance().getMetricsRegistry().remove(msgWrittenGaugeName);

		LOG.info("[{}] Shutting down (abortting: {}) on shift number {} with offset={} and timestamp={} ({})",
			 partitionId, isAborting(),
			 currentShift.shiftNumber,
			 currentShift.offset,
			 currentShift.maxMessageTimestamp,
			 dateString(currentShift.maxMessageTimestamp));

		try {
			if (isAborting()) {
				hdfsOutputPath.abortAll();
				LOG.info("[{}] all HDFS output paths have been aborted", partitionId);
			} else {
				if (previousShift != null && !previousShift.isFinished()) {
					previousShift.finish();
				}
				currentShift.finish(true);
			}
		} catch (Exception e) {
			LOG.error("[{}] Exception raised during shutdown: ", partitionId, e);
		}
	}

	private class WorkerShift {

		private long shiftStart;
		private long shiftEnd;
		private long offset;
		private long maxMessageTimestamp;
		private final long shiftNumber;
		private boolean finished;

		public WorkerShift() throws Exception {
			this(null);
		}

		public WorkerShift(WorkerShift previousShift) throws Exception {
			calculateShiftTimes();
			this.maxMessageTimestamp = -1;
			this.finished = false;

			if (previousShift != null) {
				this.offset = previousShift.offset;
				this.shiftNumber = previousShift.shiftNumber + 1;
			} else {
				this.offset = getStoredOffsetFromZk();				
				this.shiftNumber = 1;
			}

			LOG.info("[{}] Starting shift #{} for offest {} start time is {} and end time is {}",
				 partitionId,
				 shiftNumber,
				 this.offset,
				 dateString(shiftStart),
				 dateString(shiftEnd));
		}

		private void checkTimestamp(long timestamp) {
			if (timestamp > maxMessageTimestamp) {
				maxMessageTimestamp = timestamp;
			}
		}

		private void calculateShiftTimes() {
			shiftStart = System.currentTimeMillis() - System.currentTimeMillis()
				 % (config.getRunningConfig().getWorkerShiftDurationSeconds() * 1000);
			shiftEnd = shiftStart + config.getRunningConfig().getWorkerShiftDurationSeconds() * 1000;
		}

		private void finish() throws Exception {
			finish(false);
		}

		/** 				 
		@param persistMetadata whether to persist the partition metadata to ZK
		*/
		private void finish(boolean persistMetadata) throws Exception {
			LOG.info("[{}] Shift ending at {} is finished", partitionId, dateString(shiftEnd));
			hdfsOutputPath.closeOffShift(shiftNumber);
			if (persistMetadata) {
				storeOffset();
				storeOffsetTimestamp();
			}
			finished = true;			
		}

		private boolean isOver() {
			return System.currentTimeMillis() >= shiftEnd;
		}

		private boolean isFinished() {
			return finished;
		}

		private boolean isTimeToFinish() {
			return !isFinished() && System.currentTimeMillis() - shiftEnd
				 >= config.getRunningConfig().getFileCloseGraceTimeAfterExpiredMs();
		}
		
		private void storeOffsetTimestamp() throws Exception {
			long zkTimestamp = maxMessageTimestamp;
			if (zkTimestamp == -1) {
				LOG.info("[{}] Shift #{} never recieved a message, using shift end time {} ({}) for offset timestamp",
					 partitionId,
					 shiftNumber,
					 shiftEnd,
					 dateString(shiftEnd));
				zkTimestamp = shiftEnd;
			}

			if (curator.checkExists().forPath(zkPath_offSetTimestamp) == null) {
				curator.create().creatingParentsIfNeeded()
					 .withMode(CreateMode.PERSISTENT)
					 .forPath(zkPath_offSetTimestamp, Converter.getBytes(zkTimestamp));
			} else {
				curator.setData().forPath(zkPath_offSetTimestamp,
					 Converter.getBytes(zkTimestamp));
			}

			LOG.info("[{}] Shift #{} stored offset timestamp {} to ZK {} ({})",
				 partitionId,
				 shiftNumber,
				 zkTimestamp,
				 zkPath_offSetTimestamp,
				 dateString(zkTimestamp));
		}

		private void storeOffset() throws Exception {
			if (curator.checkExists().forPath(zkPath) == null) {
				curator.create().creatingParentsIfNeeded()
					 .withMode(CreateMode.PERSISTENT).forPath(zkPath, Converter.getBytes(offset));
			} else {
				curator.setData().forPath(zkPath, Converter.getBytes(offset));
			}
			LOG.info("[{}] Shift #{} wrote offset {} to existing path {}",
				 partitionId, shiftNumber, offset, zkPath);
		}

		private Long getStoredOffsetFromZk() throws Exception {
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
							 partitionId, zkOffset, zkOffsetOverride, config.getRunningConfig().getAllowOffsetOverrides());
						curator.delete().forPath(zkPath_offSetOverride);
						LOG.info("{} successfully deleted offset override ZK path: {}", partitionId, zkPath_offSetOverride);
						return zkOffsetOverride;
					} else {
						LOG.warn("{} : offset in ZK is {} and an override of {} exists however allowOffsetOverride={}",
							 partitionId, zkOffset, zkOffsetOverride, config.getRunningConfig().getAllowOffsetOverrides());
					}
				}
				return zkOffset;
			}
		}

	}

	/**
	 * Stops the worker gracefully
	 */
	@Override
	public void stop() {
		LOG.info("[{}] Graceful shutdown request received", partitionId);
		stopping = true;
	}

	/**
	 * Stops working and aborts all progress
	 */
	@Override
	protected void abort() {
		LOG.info("[{}] Abort request received", partitionId);
		aborting = true;
	}

	/**
	 * @return the aborting
	 */
	public boolean isAborting() {
		return aborting;
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
