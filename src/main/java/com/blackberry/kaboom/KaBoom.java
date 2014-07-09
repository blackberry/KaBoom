package com.blackberry.kaboom;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.blackberry.krackle.MetricRegistrySingleton;
import com.blackberry.krackle.consumer.ConsumerConfiguration;

public class KaBoom {
  private static final Logger LOG = LoggerFactory.getLogger(KaBoom.class);
  private static final Charset UTF8 = Charset.forName("UTF-8");

  boolean shutdown = false;

  public static void main(String[] args) throws Exception {
    new KaBoom(args).run();
  }

  public KaBoom(String[] args) throws Exception {
  }

  private void run() throws Exception {
    // Read in properties file and configure
    Properties props = new Properties();
    try {
      InputStream propsIn = null;
      if (System.getProperty("kaboom.configuration") != null) {
        propsIn = new FileInputStream(
            System.getProperty("kaboom.configuration"));
        props.load(propsIn);
      } else {
        LOG.info("Loading configs from {}", KaBoom.class.getClassLoader()
            .getResource("kaboom.properties"));
        propsIn = KaBoom.class.getClassLoader().getResourceAsStream(
            "kaboom.properties");
        props.load(propsIn);
      }
    } catch (Throwable t) {
      System.err.println("Error getting config file.");
      t.printStackTrace();
      System.exit(1);
    }

    // Check to see if we need to enable console reporting
    if (Boolean.parseBoolean(System.getProperty("metrics.to.console", "false"))) {
      MetricRegistrySingleton.getInstance().enableConsole();
    }

    // props.list(System.out);

    // My ID
    if (props.get("kaboom.id") == null) {
      LOG.error("Missing required property: kaboom.id");
      return;
    }
    final int kaboomId = Integer.parseInt(props.getProperty("kaboom.id"));
    String kaboomIdString = Integer.toString(kaboomId);

    // Various configs
    long fileRotateInterval = Long.parseLong(props.getProperty(
        "file.rotate.interval", "" + (3 * 60 * 1000)));
    int weight = Integer.parseInt(props.getProperty("kaboom.weighting", ""
        + Runtime.getRuntime().availableProcessors()));

    // Mapping to topics to output locations
    Pattern topicPathPattern = Pattern.compile("^topic\\.([^\\.]+)\\.path$");
    Map<String, String> topicFileLocation = new HashMap<String, String>();
    for (Entry<Object, Object> e : props.entrySet()) {
      Matcher m = topicPathPattern.matcher(e.getKey().toString());
      if (m.matches()) {
        topicFileLocation.put(m.group(1), e.getValue().toString());
      }
    }

    // Also proxy users
    Pattern topicProxyUserPattern = Pattern
        .compile("^topic\\.([^\\.]+)\\.proxy.user$");
    Map<String, String> topicProxyUserLocation = new HashMap<String, String>();
    for (Entry<Object, Object> e : props.entrySet()) {
      Matcher m = topicProxyUserPattern.matcher(e.getKey().toString());
      if (m.matches()) {
        topicProxyUserLocation.put(m.group(1), e.getValue().toString());
      }
    }

    // Consumer config
    ConsumerConfiguration consumerConfig = new ConsumerConfiguration(props);

    // Kerberos config
    if (props.containsKey("kerberos.principal")
        && props.containsKey("kerberos.keytab")) {
      LOG.info("Using Kerberos authentication.");
      LOG.info("  kerberos.principal = {}",
          props.getProperty("kerberos.principal"));
      LOG.info("  kerberos.keytab = {}", props.getProperty("kerberos.keytab"));
      Authenticator.getInstance().setKerbConfPrincipal(
          props.getProperty("kerberos.principal"));
      Authenticator.getInstance().setKerbKeytab(
          props.getProperty("kerberos.keytab"));
    }

    // Hadoop config
    final Configuration hConf = new Configuration();
    // Add standard configs
    if (new File("/etc/hadoop/conf/core-site.xml").exists())
      hConf.addResource(new FileInputStream("/etc/hadoop/conf/core-site.xml"));
    if (new File("/etc/hadoop/conf/hdfs-site.xml").exists())
      hConf.addResource(new FileInputStream("/etc/hadoop/conf/hdfs-site.xml"));
    // Add any more standard configs we find in the classpath
    for (String file : new String[] { "core-site.xml", "hdfs-site.xml" }) {
      InputStream in = this.getClass().getClassLoader()
          .getResourceAsStream(file);
      if (in != null) {
        hConf.addResource(in);
      }
    }
    hConf.setBoolean("fs.automatic.close", false);

    // Check for local hostname
    String hostname = props.getProperty("kaboom.hostname", InetAddress
        .getLocalHost().getHostName());

    // Register existence with ZooKeeper
    String zookeeperConnectionString = null;
    if (props.containsKey("zookeeper.connection.string")) {
      zookeeperConnectionString = props
          .getProperty("zookeeper.connection.string");
    } else {
      LOG.error("Missing required property: zookeeper.connection.string");
      return;
    }
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    String[] connStringAndPrefix = zookeeperConnectionString.split("/", 2);
    final CuratorFramework curator;
    if (connStringAndPrefix.length == 1) {
      curator = CuratorFrameworkFactory.newClient(zookeeperConnectionString,
          retryPolicy);
    } else {
      curator = CuratorFrameworkFactory.builder()
          .namespace(connStringAndPrefix[1])
          .connectString(connStringAndPrefix[0]).retryPolicy(retryPolicy)
          .build();
    }

    curator.start();

    // Ensure the existence of certain nodes
    if (curator.checkExists().forPath("/kaboom/leader") == null) {
      try {
        curator.create().creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT).forPath("/kaboom/leader");
      } catch (Exception e) {
        LOG.error("Error creating ZooKeeper node /kaboom/leader", e);
      }
    }

    if (curator.checkExists().forPath("/kaboom/clients") == null) {
      try {
        curator.create().creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT).forPath("/kaboom/clients");
      } catch (Exception e) {
        LOG.error("Error creating ZooKeeper node /kaboom/clients", e);
      }
    }

    if (curator.checkExists().forPath("/kaboom/assignments") == null) {
      try {
        curator.create().creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT).forPath("/kaboom/assignments");
      } catch (Exception e) {
        LOG.error("Error creating ZooKeeper node /kaboom/assignments", e);
      }
    }

    // Register my existence
    {
      Yaml yaml = new Yaml();
      ByteArrayOutputStream nodeOutputStream = new ByteArrayOutputStream();
      OutputStreamWriter writer = new OutputStreamWriter(nodeOutputStream);
      KaBoomNodeInfo data = new KaBoomNodeInfo();
      data.setHostname(hostname);
      data.setWeight(weight);
      yaml.dump(data, writer);
      writer.close();
      byte[] nodeContents = nodeOutputStream.toByteArray();
      long backoff = 1000;
      long retries = 6;
      for (int i = 0; i < retries; i++) {
        try {
          curator.create().withMode(CreateMode.EPHEMERAL)
              .forPath("/kaboom/clients/" + kaboomId, nodeContents);
          break;
        } catch (Exception e) {
          if (i < retries - 1) {
            LOG.warn("Failed to register with ZooKeeper.  Retrying.", e);
            Thread.sleep(backoff);
            backoff *= 2;
            continue;
          } else {
            throw new Exception("Failed to register with ZooKeeper.", e);
          }
        }
      }
    }

    // Start leader election thread.
    // The leader assigns work to each instance
    LoadBalancer loadBalancer = new LoadBalancer(props);
    final LeaderSelector leaderSelector = new LeaderSelector(curator,
        "/kaboom/leader", loadBalancer);
    leaderSelector.autoRequeue();
    leaderSelector.start();

    final List<Worker> workers = new ArrayList<Worker>();
    final List<Thread> threads = new ArrayList<Thread>();

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        shutdown();

        for (Worker w : workers) {
          w.stop();
        }
        for (Thread t : threads) {
          try {
            t.join();
          } catch (InterruptedException e) {
            LOG.error("Interrupted joining thread.", e);
          }
        }

        try {
          FileSystem.get(hConf).close();
        } catch (IOException e) {
          LOG.error("Error closing Hadoop filesystem");
        }

        try {
          curator.delete().forPath("/kaboom/clients/" + kaboomId);
        } catch (Exception e) {
          LOG.error("Error deleting /kaboom/clients/{}", kaboomId, e);
        }

        leaderSelector.close();
        curator.close();
      }
    }));

    // If we have assigned partitions, then grab a lock on the partition and
    // start working.
    // Run for a specified duration, then check again.
    Pattern topicPartitionPattern = Pattern.compile("^(.*)-(\\d+)$");
    while (shutdown == false) {
      workers.clear();
      threads.clear();

      for (String node : curator.getChildren().forPath("/kaboom/assignments")) {
        String assignee = new String(curator.getData().forPath(
            "/kaboom/assignments/" + node), UTF8);
        if (assignee.equals(kaboomIdString)) {
          // run it
          LOG.info("Running data pull for {}", node);
          Matcher m = topicPartitionPattern.matcher(node);
          if (m.matches()) {
            String topic = m.group(1);
            int partition = Integer.parseInt(m.group(2));

            String path = topicFileLocation.get(topic);
            if (path == null) {
              LOG.error("Topic has no configured output path: {}", topic);
              continue;
            }

            String proxyUser = topicProxyUserLocation.get(topic);
            if (proxyUser == null) {
              proxyUser = "";
            }

            Worker worker = new Worker(consumerConfig, hConf, curator, topic,
                partition, fileRotateInterval, path, proxyUser);
            workers.add(worker);
            Thread t = new Thread(worker);
            threads.add(t);
            t.start();

          } else {
            LOG.error("Could not get topic and partition from node name. ({})",
                node);
          }
        }
      }

      if (threads.size() > 0) {
        for (Thread t : threads) {
          t.join();
        }
      } else {
        Thread.sleep(10000);
      }
    }
  }

  public void shutdown() {
    shutdown = true;
  }
}
