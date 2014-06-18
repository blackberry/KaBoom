package com.blackberry.logdriver.kaboom;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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

public class Worker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private Consumer consumer;
  private long offset;
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

  private String topic;
  private int partition;
  private ConsumerConfiguration consumerConfig;

  private long endTime;

  private static final String id = UUID.randomUUID().toString();

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

    LOG.info("Created worker for topic '{}', partition {}.", topic, partition);
  }

  @Override
  public void run() {
    zkPath = ZK_ROOT + "/topics/" + topic + "/" + partition;

    try {
      offset = getOffset();
    } catch (Exception e) {
      LOG.error("Error getting offset.", e);
      return;
    }

    try {
      hostname = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      LOG.error("Can't determine local hostname");
      hostname = "unknown.host";
    }

    String clientId = "kaboom-" + hostname;

    consumer = new Consumer(consumerConfig, clientId, topic, partition, offset,
        MetricRegistrySingleton.getInstance().getMetricsRegistry());

    LOG.info(
        "Created worker for topic '{}', partition {}.  Starting at offset {}.",
        topic, partition, offset);

    byte[] bytes = new byte[1024 * 1024];
    int length = -1;
    long timestamp;

    byte version = -1;

    int pos = 0;
    PriParser pri = new PriParser();
    VersionParser ver = new VersionParser();
    TimestampParser tsp = new TimestampParser();

    long linesread = 0;
    while (System.currentTimeMillis() < endTime) {
      try {
        if (stopping) {
          LOG.info("Stopping Worker.");
          break;
        }

        length = consumer.getMessage(bytes, 0, bytes.length);

        if (length == -1) {
          continue;
        }

        if (consumer.getLastOffset() < offset) {
          // Sometimes the consumer may report data we've already seen.
          LOG.debug(
              "Received offset {} which is before requested offset of {}.  Skipping message.",
              consumer.getLastOffset(), offset);
          continue;
        }

        // LOG.info("Read message: {}", new String(bytes, 0, length, "UTF8"));
        linesread++;

        if (offset != consumer.getLastOffset()) {
          LOG.info("Offset anomaly! Expected:{}, Got:{}", offset,
              consumer.getLastOffset());
        }
        offset = consumer.getNextOffset();

        // Check for version
        if (bytes[0] == (byte) 0xFE) {
          version = bytes[1];
          if (version == (byte) 0x00) {
            // Version 0 has a timestamp in the front, so we can skip that for
            // now. Come back if we need it.
            pos = 10;
          } else {
            LOG.warn("Unrecognized encoding version: {}", version);
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
          // If the next char is a space, skip that too.
          if (pos < length && bytes[pos] == ' ') {
            pos++;
          }
        } else {
          if (version == (byte) 0x00) {
            LOG.debug("Failed to parse timestamp.  Using stored timestamp");
            timestamp = longFromBytes(bytes, 2);
          } else {
            LOG.error("Error parsing timestamp.");
            timestamp = System.currentTimeMillis();
          }
        }
        getBoomWriter(timestamp).writeLine(timestamp, bytes, pos, length - pos);

      } catch (Throwable t) {
        LOG.error("Error processing message.", t);
        LOG.info("Deleting all tmp files");
        for (Entry<Long, OutputFile> entry : outputFileMap.entrySet()) {
          entry.getValue().abort();
        }
        return;
      }
    }

    // Close all writers, and store offset
    LOG.info("Closing all output files.");
    for (Entry<Long, OutputFile> entry : outputFileMap.entrySet()) {
      try {
        entry.getValue().getBoomWriter().close();
        entry.getValue().close();
      } catch (IOException e) {
        LOG.error("Error closing output file", e);
      }
    }
    LOG.info("Storing processed offsets into ZooKeeper.");
    try {
      storeOffset();
    } catch (Exception e) {
      LOG.error("Error storing offset in ZooKeeper", e);
    }

    LOG.info("Worker stopped. (Read {} lines.  Next offset is {})", linesread,
        offset);
  }

  private void storeOffset() throws Exception {
    if (curator.checkExists().forPath(zkPath) == null) {
      curator.create().creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT).forPath(zkPath, getBytes(offset));
    } else {
      curator.setData().forPath(zkPath, getBytes(offset));
    }

  }

  private long getOffset() throws Exception {
    if (curator.checkExists().forPath(zkPath) == null) {
      return 0L;
    } else {
      return longFromBytes(curator.getData().forPath(zkPath), 0);
    }
  }

  private long longFromBytes(byte[] data, int offset) {
    return ((long) data[offset] & 0xFFL) << 56 //
        | ((long) data[offset + 1] & 0xFFL) << 48 //
        | ((long) data[offset + 2] & 0xFFL) << 40 //
        | ((long) data[offset + 3] & 0xFFL) << 32 //
        | ((long) data[offset + 4] & 0xFFL) << 24 //
        | ((long) data[offset + 5] & 0xFFL) << 16 //
        | ((long) data[offset + 6] & 0xFFL) << 8 //
        | ((long) data[offset + 7] & 0xFFL);

  }

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

  private static AtomicLong counter = new AtomicLong(0L);

  private class OutputFile {
    private String dir;
    private String tmpdir;
    private String filename;

    private Path finalPath;
    private Path tmpPath;

    private FastBoomWriter boomWriter;
    private OutputStream out;
    private long i;

    public OutputFile(long hour) {
      dir = fillInTemplate(hour);
      i = counter.getAndIncrement();

      filename = String.format("kaboom-%s-%08d.bm", id, i);
      tmpdir = String.format("%s/_tmp_kaboom_%s_%08d", dir, id, i);

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
      LOG.info("Aborting output file.");
      try {
        boomWriter.close();
      } catch (IOException e) {
        LOG.error("Error closing boom writer.", e);
      }
      try {
        out.close();
      } catch (IOException e) {
        LOG.error("Error closing boom writer output file.", e);
      }
      synchronized (fsLock) {
        try {
          fs = tmpPath.getFileSystem(hConf);
          fs.delete(new Path(tmpdir), true);
        } catch (IOException e) {
          LOG.error("Error deleting temp files.", e);
        }
      }
    }

    public FastBoomWriter getBoomWriter() {
      return boomWriter;
    }

    public void close() throws IOException {
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
                    LOG.error("Error getting File System.", e);
                  }
                }
                try {
                  fs.rename(tmpPath, finalPath);
                } catch (Exception e) {
                  LOG.error("Error renaming file.", e);
                  abort();
                }
                fs.delete(new Path(tmpdir), true);
                return null;
              }
            });
      } catch (Exception e) {
        LOG.error("Error creating file.", e);
      }

    }
  }

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

}
