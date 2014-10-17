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

public class Worker implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
	private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

	private String partitionId;

	private Consumer consumer;
	private long offset;
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
	private FsPermission permissions = new FsPermission(FsAction.READ_WRITE,
			FsAction.READ, FsAction.NONE);
	private int bufferSize = 16 * 1024;
	private short replicas = 3;
	private long blocksize = 256 * 1024 * 1024;

	private CuratorFramework curator;
	private static final String ZK_ROOT = "/kaboom";
	private String zkPath;
	private String zkPath_offSetTimestamp;

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

	private static Set<Worker> workers = new HashSet<Worker>();
	private static Object workersLock = new Object();

	
	
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
						long avgLag = sumLag/count;
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
						long avgLag = sumLag/count;
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
						return sumMsgWritten/count;
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

	public Worker(ConsumerConfiguration consumerConfig, Configuration hConf,
			CuratorFramework curator, String topic, int partition, long runDuration,
			String template) throws Exception {
		this(consumerConfig, hConf, curator, topic, partition, runDuration,
				template, "");
	}

	public Worker(ConsumerConfiguration consumerConfig, Configuration hConf,
			CuratorFramework curator, String topic, int partition, long runDuration,
			String template, String proxyUser) throws Exception {
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

		partitionId = topic + "-" + partition;

		lagGaugeName = "kaboom:partitions:" + partitionId + ":message lag";
		lagSecGaugeName = "kaboom:partitions:" + partitionId + ":message lag sec";
		msgWrittenGaugeName = "kaboom:partitions:" + partitionId + ":messages written per second";

		/*
		 * dariens: iterate through an array of metrics that need to be removed when new 
		 * workers are instantiated 
		 */
				
		String[] metrics_to_remove = {lagGaugeName, lagSecGaugeName, msgWrittenGaugeName};
		
		for (final String metric_name: metrics_to_remove)
		{
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
		
		// Add: long lagGaugeName
		
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
				.register(lagGaugeName, new Gauge<Long>() {
					@Override
					public Long getValue() {
						return lag;
					}
				});
		
		// Add: int lagSecGaugeName
		
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
				.register(lagSecGaugeName, new Gauge<Integer>() {
					@Override
					public Integer getValue() {
						return lag_sec;
					}
				});
		
	// Add: long msgWrittenGaugeName
		
		MetricRegistrySingleton.getInstance().getMetricsRegistry()
				.register(msgWrittenGaugeName, new Gauge<Long>() {
					@Override
					public Long getValue() {
						return linesread / ((System.currentTimeMillis() - startTime) / 1000);
					}
				});
		
		synchronized (workersLock) {
			workers.add(this);
		}
		LOG.info("[{}] Created worker.", partitionId);
	}

	@Override
	public void run() {
		try {
			zkPath = ZK_ROOT + "/topics/" + topic + "/" + partition;
			zkPath_offSetTimestamp = zkPath + "/offset_timestamp";

			try {
				offset = getOffset();
			} catch (Exception e) {
				LOG.error("[{}] Error getting offset.", partitionId, e);
				return;
			}

			try {
				hostname = InetAddress.getLocalHost().getCanonicalHostName();
			} catch (UnknownHostException e) {
				LOG.error("[{}] Can't determine local hostname", partitionId);
				hostname = "unknown.host";
			}

			String clientId = "kaboom-" + hostname;

			consumer = new Consumer(consumerConfig, clientId, topic, partition,
					offset, MetricRegistrySingleton.getInstance().getMetricsRegistry());

			LOG.info("[{}] Created worker.  Starting at offset {}.", partitionId,
					topic, partition, offset);
			
			//mbruce: Adding check to ensure that the latest offset in Kafka is not less than where we are.
			long highwatermark = consumer.getHighWaterMark(); 
			if(highwatermark < offset) {
				LOG.warn("[{}] has a lower High Water Mark {} than we're trying to consume from {}.  Likely this is caused by a non-ISR taking leadership.  Resetting offset to High Water Mark", partitionId, highwatermark, offset);
				offset = highwatermark;
			}

			byte[] bytes = new byte[1024 * 1024];
			int length = -1;

			byte version = -1;

			int pos = 0;
			PriParser pri = new PriParser();
			VersionParser ver = new VersionParser();
			TimestampParser tsp = new TimestampParser();
			
			long maxTimestamp = -1;

			while (System.currentTimeMillis() < endTime) {
				try {
					if (stopping) {
						LOG.info("[{}] Stopping Worker.", partitionId);
						break;
					}

					length = consumer.getMessage(bytes, 0, bytes.length);

					if (length == -1) {
						continue;
					}

					if (consumer.getLastOffset() < offset) {
						// Sometimes the consumer may report data we've already seen.
						LOG.debug(
								"[{}] Received offset {} which is before requested offset of {}.  Skipping message.",
								partitionId, consumer.getLastOffset(), offset);
						continue;
					}

					// LOG.info("Read message: {}", new String(bytes, 0, length, "UTF8"));
					linesread++;

					if (offset != consumer.getLastOffset()) {
						LOG.info("[{}] Offset anomaly! Expected:{}, Got:{}", partitionId,
								offset, consumer.getLastOffset());
					}

					lag = consumer.getHighWaterMark() - offset;
					offset = consumer.getNextOffset();

					// Check for version
					if (bytes[0] == (byte) 0xFE) {
						version = bytes[1];
						if (version == (byte) 0x00) {
							// Version 0 has a timestamp in the front, so we can skip that for
							// now. Come back if we need it.
							pos = 10;
						} else {
							LOG.warn("[{}] Unrecognized encoding version: {}", partitionId,
									version);
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
						// Move position to the end of the timestamp.
						pos += tsp.getLength();
						// mbruce: occasionally we get a line that is truncated partway
						// through the timestamp,
						// however we still have the rest of the last message in the byte
						// buffer and
						// parsing the timestamp will push us past then end of the line
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
							LOG.debug(
									"[{}] Failed to parse timestamp.  Using stored timestamp",
									partitionId);
							timestamp = Converter.longFromBytes(bytes, 2);
						} else {
							LOG.error("[{}] Error parsing timestamp.", partitionId);
							timestamp = System.currentTimeMillis();
						}
					}
					
					// dariens: calculate lag seconds
					
					lag_sec = (int) (System.currentTimeMillis() - timestamp) / 1000;
					if (lag_sec < 0) lag_sec = 0;

					/*
					 * aryder: Adding if statement to catch if length - pos < 0, and log
					 * on the information passed to getBoomWriter.
					 */
					if ((length - pos) < 0)
						LOG.info(
								"[{}] Length - Offset is < 0: timestamp: {}, pos: {}, length: {}",
								partitionId, timestamp, pos, length);

					getBoomWriter(timestamp).writeLine(timestamp, bytes, pos,
							length - pos);
					
					/*
					 * Let's track the max timestamp and write that to ZK for the 
					 * partitions oldest offset timestamp.  Previously we used the last
					 * message's timestamp however that could lead to problems if there
					 * was ever corruption of the timestamp or if there's significant
					 * skew in the log's time.
					 */
					
					if (timestamp > maxTimestamp || maxTimestamp == -1) {
						maxTimestamp = timestamp;
					}
					

				} catch (Throwable t) {
					LOG.error("[{}] Error processing message.", partitionId, t);
					LOG.info("[{}] Deleting all tmp files", partitionId);
					for (Entry<Long, OutputFile> entry : outputFileMap.entrySet()) {
						entry.getValue().abort();
					}
					return;
				}
			}

			// Close all writers, and store offset
			LOG.info("[{}] Closing all output files.", partitionId);
			for (Entry<Long, OutputFile> entry : outputFileMap.entrySet()) {
				try {
					entry.getValue().getBoomWriter().close();
					entry.getValue().close();
				} catch (IOException e) {
					LOG.error("[{}] Error closing output file", partitionId, e);
				}
			}
			LOG.info("[{}] Storing processed offsets into ZooKeeper.", partitionId);
			try {
				storeOffset();
				storeOffsetTimestamp(maxTimestamp);
			} catch (Exception e) {
				LOG.error("[{}] Error storing offset/timestamp in ZooKeeper", partitionId, e);
			}

			MetricRegistrySingleton.getInstance().getMetricsRegistry()
					.remove(lagGaugeName);
			MetricRegistrySingleton.getInstance().getMetricsRegistry()
			.remove(lagSecGaugeName);
			MetricRegistrySingleton.getInstance().getMetricsRegistry()
			.remove(msgWrittenGaugeName);
			LOG.info("[{}] Worker stopped. (Read {} lines.  Next offset is {})",
					partitionId, linesread, offset);
		} finally {
			synchronized (workersLock) {
				workers.remove(this);
			}
		}
	}

	private void storeOffset() throws Exception {
		if (curator.checkExists().forPath(zkPath) == null) {
			curator.create().creatingParentsIfNeeded()
					.withMode(CreateMode.PERSISTENT).forPath(zkPath, Converter.getBytes(offset));
		} else {
			curator.setData().forPath(zkPath, Converter.getBytes(offset));
		}

	}

	private long getOffset() throws Exception {
		if (curator.checkExists().forPath(zkPath) == null) {
			return 0L;
		} else {
			return Converter.longFromBytes(curator.getData().forPath(zkPath), 0);
		}
	}
	
  /** 
   * Stores the partitions offset timestamp in ZK
   * @throws Exception
   */
  private void storeOffsetTimestamp(long offsetTimestamp) throws Exception {
      if (curator.checkExists().forPath(zkPath_offSetTimestamp) == null) {
          curator.create().creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT).forPath(zkPath_offSetTimestamp, Converter.getBytes(offsetTimestamp));
      } else {
          curator.setData().forPath(zkPath_offSetTimestamp, Converter.getBytes(offsetTimestamp));
      }   

  }   

  /** 
   * Returns the partitions offset timestamp from ZK
   * @return the offset timestamp  
   * @throws Exception
   */
  private long getOffsetTimestamp() throws Exception {
      if (curator.checkExists().forPath(zkPath_offSetTimestamp) == null) {
          return 0L; 
      } else {
          return Converter.longFromBytes(curator.getData().forPath(zkPath_offSetTimestamp), 0); 
      }   
  }  
  
	@Deprecated
  private long longFromytes(byte[] data, int offset) {
		return ((long) data[offset] & 0xFFL) << 56 //
				| ((long) data[offset + 1] & 0xFFL) << 48 //
				| ((long) data[offset + 2] & 0xFFL) << 40 //
				| ((long) data[offset + 3] & 0xFFL) << 32 //
				| ((long) data[offset + 4] & 0xFFL) << 24 //
				| ((long) data[offset + 5] & 0xFFL) << 16 //
				| ((long) data[offset + 6] & 0xFFL) << 8 //
				| ((long) data[offset + 7] & 0xFFL);

	}

	@Deprecated
	private byte[] getBytes(long l) {
		return new byte[] //
		{ (byte) (l >> 56), //
				(byte) (l >> 48), //
				(byte) (l >> 40),//
				(byte) (l >> 32), //
				(byte) (l >> 24),//
				(byte) (l >> 16),//
				(byte) (l >> 8),//
				(byte) (l) //
		};
	}

	private FastBoomWriter getBoomWriter(long timestamp) throws IOException {
		hour = timestamp - timestamp % (60 * 60 * 1000);
		outputFile = outputFileMap.get(hour);
		if (outputFile == null) {
			outputFile = new OutputFile(hour);
			outputFileMap.put(hour, outputFile);
		}

		return outputFile.getBoomWriter();
	}

	private class OutputFile {
		private String dir;
		private String tmpdir;
		private String filename;

		private Path finalPath;
		private Path tmpPath;

		private FastBoomWriter boomWriter;
		private OutputStream out;

		public OutputFile(long hour) {
			dir = Converter.timestampTemplateBuilder(hour, template);

			/*
			 * Related to IPGBD-1028 Output topic-partitionId-offset-incrementval.bm
			 */
			filename = String.format("%s-%08d.bm", partitionId, offset);
			tmpdir = String.format("%s/_tmp_%s-%08d.bm", dir, partitionId,	offset);

			finalPath = new Path(dir + "/" + filename);
			tmpPath = new Path(tmpdir + "/" + filename);

			try {
				Authenticator.getInstance().runPrivileged(proxyUserName,
						new PrivilegedExceptionAction<Void>() {
							@Override
							public Void run() throws Exception {
								// Apparently getting a FileSystem from a path
								// is not thread safe
								synchronized (fsLock) {
									try {
										fs = tmpPath.getFileSystem(hConf);
									} catch (IOException e) {
										LOG.error("Error getting File System.", e);
									}
								}
								LOG.info("[{}] Opening {}.", partitionId, tmpPath);
								if(fs.exists(tmpPath)) {
									fs.delete(tmpPath, false);
									LOG.info("[{}] Removing file from HDFS because it already exists: {}", partitionId, tmpPath.toString());
								}
								out = fs.create(tmpPath, permissions, false, bufferSize,
										replicas, blocksize, null);

								boomWriter = new FastBoomWriter(out);
								return null;
							}
						});
			} catch (Exception e) {
				LOG.error("Error creating file.", e);
			}
		}

		public void abort() {
			LOG.info("[{}] Aborting output file.", partitionId);
			try {
				boomWriter.close();
			} catch (IOException e) {
				LOG.error("[{}] Error closing boom writer.", partitionId, e);
			}
			try {
				out.close();
			} catch (IOException e) {
				LOG.error("[{}] Error closing boom writer output file.", partitionId, e);
			}
			synchronized (fsLock) {
				try {
					fs = tmpPath.getFileSystem(hConf);
					fs.delete(new Path(tmpdir), true);
				} catch (IOException e) {
					LOG.error("[{}] Error deleting temp files.", partitionId, e);
				}
			}
		}

		public FastBoomWriter getBoomWriter() {
			return boomWriter;
		}

		public void close() throws IOException {
			LOG.info("[{}] Closing {}.", partitionId, tmpPath);
			boomWriter.close();
			out.close();

			try {
				Authenticator.getInstance().runPrivileged(proxyUserName,
						new PrivilegedExceptionAction<Void>() {
							@Override
							public Void run() throws Exception {
								// Apparently getting a FileSystem from a path
								// is not thread
								// safe
								synchronized (fsLock) {
									try {
										fs = tmpPath.getFileSystem(hConf);
									} catch (IOException e) {
										LOG.error("[{}] Error getting File System.", partitionId, e);
									}
								}
								try {
									LOG.info("[{}] Moving {} to {}.", partitionId, tmpPath,
											finalPath);
									fs.rename(tmpPath, finalPath);
								} catch (Exception e) {
									LOG.error("[{}] Error renaming file.", partitionId, e);
									abort();
								}
								fs.delete(new Path(tmpdir), true);
								return null;
							}
						});
			} catch (Exception e) {
				LOG.error("[{}] Error creating file.", partitionId, e);
			}

		}
	}

	@Deprecated
	private String fillInTemplate(long timestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(UTC);
		cal.setTimeInMillis(timestamp);

		long templateLength = template.length();

		StringBuilder sb = new StringBuilder();
		int i = 0;
		int p = 0;
		char c;
		while (true) {
			p = template.indexOf('%', i);
			if (p == -1) {
				sb.append(template.substring(i));
				break;
			}
			sb.append(template.substring(i, p));

			if (p + 1 < templateLength) {
				c = template.charAt(p + 1);
				switch (c) {
				case 'y':
					sb.append(String.format("%04d", cal.get(Calendar.YEAR)));
					break;
				case 'M':
					sb.append(String.format("%02d", cal.get(Calendar.MONTH) + 1));
					break;
				case 'd':
					sb.append(String.format("%02d", cal.get(Calendar.DAY_OF_MONTH)));
					break;
				case 'H':
					sb.append(String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)));
					break;
				case 'm':
					sb.append(String.format("%02d", cal.get(Calendar.MINUTE)));
					break;
				case 's':
					sb.append(String.format("%02d", cal.get(Calendar.SECOND)));
					break;
				case 'l':
					sb.append(hostname);
					break;
				default:
					sb.append('%').append(c);
				}
			} else {
				sb.append('%');
				break;
			}

			i = p + 2;

			if (i >= templateLength) {
				break;
			}
		}

		return sb.toString();
	}

	public void stop() {
		LOG.info("Stop request received.");
		stopping = true;
	}

	/*
	 * aryder: changed int to long
	 */
	public long getLag() {
		return lag;
	}
	public int getLagSec() {
		return lag_sec;
	}
	public long getMsgWrittenPerSec() {
		return linesread / ((System.currentTimeMillis() - startTime) / 1000);
	}
}
