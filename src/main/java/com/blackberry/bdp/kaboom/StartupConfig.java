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

import com.blackberry.bdp.common.jmx.MetricRegistrySingleton;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;

import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;

import org.apache.curator.retry.ExponentialBackoffRetry;
import com.blackberry.bdp.common.props.Parser;

import com.blackberry.bdp.kaboom.api.RunningConfig;
import com.blackberry.bdp.kaboom.api.KaBoomTopicConfig;
import com.blackberry.bdp.krackle.consumer.ConsumerConfiguration;
import com.codahale.metrics.Meter;
import java.util.Iterator;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.zookeeper.data.Stat;

/**
 *
 * @author dariens
 */
public class StartupConfig {

	private static final Logger LOG = LoggerFactory.getLogger(StartupConfig.class);

	private static final String defaultProperyFile = "kaboom.properties";
	private final Parser propsParser;
	private final Object fsLock = new Object();

	private final ConsumerConfiguration consumerConfiguration;
	private Configuration hadoopConfiguration;
	private String kafkaSeedBrokers;
	private final Path hadoopUrlPath;
	private int kaboomId;
	private int weight;
	private final CuratorFramework kaboomCurator;
	private final CuratorFramework kafkaCurator;
	private String kerberosPrincipal;
	private String kerberosKeytab;
	private String hostname;
	private String kaboomZkConnectionString;
	private String kafkaZkConnectionString;
	private final String loadBalancerType;
	private final RunningConfig runningConfig;
	
	private final Meter deadWorkerMeter;
	private final Meter gracefulWorkerShutdownMeter;
	
	private String zkRootPathKafka = "";
	private String zkRootPathKafkaBrokers = String.format("%s/%s", zkRootPathKafka, "brokers/ids");	
	private String zkRootPathKaBoom = "/kaboom";	
	private String zkRootPathClients = String.format("%s/%s", zkRootPathKaBoom, "clients");
	private String zkRootPathTopicConfigs = String.format("%s/%s", zkRootPathKaBoom, "topics");
	private String zkRootPathPartitionAssignments = String.format("%s/%s", zkRootPathKaBoom, "assignments");
	private String zkRootPathFlagAssignments = String.format("%s/%s", zkRootPathKaBoom, "flag-assignments");
	private String zkPathRunningConfig = String.format("%s/%s", zkRootPathKaBoom, "config");
	private String zkPathLeaderClientId = String.format("%s/%s", zkRootPathKaBoom, "leader");
	
	private final HashMap<String, FileSystem> proxyUserToFileSystem = new HashMap<>();

	/**
	 * POSIX style NONE("---"), EXECUTE("--x"), WRITE("-w-"), WRITE_EXECUTE("-wx"), READ("r--"), READ_EXECUTE("r-x"), READ_WRITE("rw-"), ALL("rwx");
	 */
	private final FsPermission boomFilePerms = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);

	public void logConfiguraton() {
		LOG.info(" *** start dumping configuration *** ");
		LOG.info("kaboomId: {}", getKaboomId());
		LOG.info("weight: {}", getWeight());
		LOG.info("kerberosPrincipal: {}", kerberosPrincipal);
		LOG.info("kerberosKeytab: {}", kerberosKeytab);
		LOG.info("hostname: {}", getHostname());
		LOG.info("kaboomZkConnectionString: {}", kaboomZkConnectionString);
		LOG.info("kafkaZkConnectionString: {}", kafkaZkConnectionString);
		LOG.info("kafkaSeedBrokers: {}", kafkaSeedBrokers);
		LOG.info("loadBalancerType: {}", loadBalancerType);		
		LOG.info("kerberosPrincipal: {}", kerberosPrincipal);
		LOG.info("kerberosKeytab: {}", kerberosKeytab);
		LOG.info("zkRootPathKaBoom: {}", zkRootPathKaBoom);		
		LOG.info("zkRootPathKafkaBrokers: {}", zkRootPathKafkaBrokers);
		LOG.info("zkRootPathKafka: {}", zkRootPathKafka);
		LOG.info("zkRootPathClients: {}", zkRootPathClients);
		LOG.info("zkRootPathTopicConfigs: {}", zkRootPathTopicConfigs);
		LOG.info("zkRootPathPartitionAssignments: {}", getZkRootPathPartitionAssignments());
		LOG.info("zkPathRunningConfig: {}", zkPathRunningConfig);
		LOG.info("zkPathLeaderClientId: {}", zkPathLeaderClientId);		
		LOG.info(" *** end dumping configuration *** ");
	}

	public StartupConfig() throws Exception {
		this(getProperties());
	}

	public StartupConfig(Properties props) throws Exception {
		propsParser = new Parser(props);

		hadoopConfiguration = buildHadoopConfiguration();
		consumerConfiguration = new ConsumerConfiguration(props);
		kaboomId = propsParser.parseInteger("kaboom.id");
		hadoopUrlPath = new Path(propsParser.parseString("hadooop.fs.uri"));
		weight = propsParser.parseInteger("kaboom.weighting", Runtime.getRuntime().availableProcessors());
		kerberosKeytab = propsParser.parseString("kerberos.keytab");
		kerberosPrincipal = propsParser.parseString("kerberos.principal");
		hostname = propsParser.parseString("kaboom.hostname", InetAddress.getLocalHost().getHostName());
		kaboomZkConnectionString = propsParser.parseString("zookeeper.connection.string");
		kafkaZkConnectionString = propsParser.parseString("kafka.zookeeper.connection.string");
		kafkaSeedBrokers = propsParser.parseString("metadata.broker.list");
		loadBalancerType = propsParser.parseString("kaboom.load.balancer.type", "even");
		
		zkRootPathKafka = propsParser.parseString("kafka.zk.root.path", zkRootPathKafka);
		zkRootPathKafkaBrokers = propsParser.parseString("kafka.zk.root.path.brokers", zkRootPathKafkaBrokers);
		zkRootPathKaBoom = propsParser.parseString("kaboom.zk.root.path", zkRootPathKaBoom);
		zkRootPathTopicConfigs = propsParser.parseString("kaboom.zk.root.path.topic.configs", zkRootPathTopicConfigs);
		zkRootPathClients = propsParser.parseString("kaboom.zk.root.path.clients", zkRootPathClients);
		zkRootPathPartitionAssignments = propsParser.parseString("kaboom.zk.root.path.partition.assignments", zkRootPathPartitionAssignments);
		zkRootPathFlagAssignments = propsParser.parseString("kaboom.zk.root.path.flag.assignments", zkRootPathFlagAssignments);
		zkPathRunningConfig = propsParser.parseString("kaboom.zk.path.runningConfig", zkPathRunningConfig);
		zkPathLeaderClientId = propsParser.parseString("kaboom.zk.path.leader.clientId", zkPathLeaderClientId);

		
		deadWorkerMeter = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:total:dead workers");
		gracefulWorkerShutdownMeter = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:total:gracefully shutdown workers");
		
		kaboomCurator = buildCuratorFramework(kaboomZkConnectionString);
		kafkaCurator = buildCuratorFramework(kafkaZkConnectionString);

		runningConfig = RunningConfig.get(RunningConfig.class, kaboomCurator, zkPathRunningConfig);

		final NodeCache nodeCache = new NodeCache(kaboomCurator, zkPathRunningConfig);
		nodeCache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				LOG.info("The running configuration has changed in ZooKeeper");
				runningConfig.reload();
			}

		});
		nodeCache.start();

		/**
		 * Get all the topic configurations and build the following maps while we're at it
		 *  - topicConfigs
		 *  - topicToProxyUser 
		 *  - topicToSupportedStatus
		 */
		Iterator<KaBoomTopicConfig> iter = KaBoomTopicConfig.getAll(KaBoomTopicConfig.class, kaboomCurator, "/kaboom/topics").iterator();

		while (iter.hasNext()) {
			final KaBoomTopicConfig topicConfig = iter.next();
			final NodeCache nodeCacheTopic = new NodeCache(kaboomCurator, "/kaboom/topics/" + topicConfig.getId());
			nodeCacheTopic.getListenable().addListener(new NodeCacheListener() {
				@Override
				public void nodeChanged() throws Exception {
					Stat newZkStat = kaboomCurator.checkExists().forPath(topicConfig.getZkPath());
					if (newZkStat == null) {
						LOG.info("The topic configuration for {} has been deleted", topicConfig.getId());
					} else {
						LOG.info("The topic configuration has changed in ZooKeeper");
						String oldTopicId = topicConfig.getId();
						topicConfig.reload();
						if (!topicConfig.getId().equals(oldTopicId)) {
							// Remove all references from old topic name
							// Add references to new topic name
							authenticatedFsForProxyUser(topicConfig.getProxyUser());
							LOG.info("the topic {} was renamed to {} deleted old topic references",
								 oldTopicId, topicConfig.getId());
						}
					}
				}
			});
			nodeCacheTopic.start();
		}
	}

	/**
	 * Builds the proxyUserToFileSystem mapping
	 *
	 * @param proxyUser
	 * @return
	 */
	public final FileSystem authenticatedFsForProxyUser(final String proxyUser) {
		Authenticator.getInstance().setKerbConfPrincipal(kerberosPrincipal);
		Authenticator.getInstance().setKerbKeytab(kerberosKeytab);
		if (proxyUserToFileSystem.containsKey(proxyUser)) {
			return proxyUserToFileSystem.get(proxyUser);
		} else {
			try {
				LOG.info("Attempting to create file system {} for {}", getHadoopUrlPath(), proxyUser);
				Authenticator.getInstance().runPrivileged(proxyUser, new PrivilegedExceptionAction<Void>() {
					@Override
					public Void run() throws Exception {
						synchronized (fsLock) {
							try {
								FileSystem fs = getHadoopUrlPath().getFileSystem(hadoopConfiguration);
								proxyUserToFileSystem.put(proxyUser, fs);
								LOG.debug("Opening {} for proxy user {}", getHadoopUrlPath(), proxyUser);
							} catch (IOException ioe) {
								LOG.error("Error getting file system {} for proxy user {}", getHadoopUrlPath(), proxyUser, ioe);
								throw ioe;
							}
						}
						return null;
					}

				});
			} catch (IOException | InterruptedException e) {
				LOG.error("Error creating file.", e);
			}
			return proxyUserToFileSystem.get(proxyUser);
		}
	}

	/**
	 * Instantiates properties from either the specified configuration file or the default for the class
	 *
	 * @return Properties
	 */
	public static Properties getProperties() {
		Properties props = new Properties();
		try {
			InputStream propsIn;

			if (System.getProperty("kaboom.configuration") != null) {
				propsIn = new FileInputStream(System.getProperty("kaboom.configuration"));
				props.load(propsIn);
			} else {
				LOG.info("Loading configs from default properties file {}", KaBoom.class.getClassLoader().getResource(defaultProperyFile));
				propsIn = KaBoom.class.getClassLoader().getResourceAsStream(defaultProperyFile);
				props.load(propsIn);
			}
		} catch (Throwable t) {
			System.err.println("Error getting config file:");
			System.err.printf("stacktrace: %s", t.getStackTrace().toString());
			System.exit(1);
		}

		return props;
	}

	/**
	 * Instantiates properties from either the specified configuration file or the default for class
	 *
	 * @return Configuration
	 */
	private Configuration buildHadoopConfiguration() throws FileNotFoundException {
		Configuration newHadoopConfiguration = new Configuration();

		if (new File("/etc/hadoop/conf/core-site.xml").exists()) {
			newHadoopConfiguration.addResource(new FileInputStream("/etc/hadoop/conf/core-site.xml"));
		}

		if (new File("/etc/hadoop/conf/hdfs-site.xml").exists()) {
			newHadoopConfiguration.addResource(new FileInputStream("/etc/hadoop/conf/hdfs-site.xml"));
		}

		// Adds any more standard configs we find in the classpath
		for (String file : new String[]{"core-site.xml", "hdfs-site.xml"}) {
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);

			if (in != null) {
				newHadoopConfiguration.addResource(in);
			}
		}

		newHadoopConfiguration.setBoolean("fs.automatic.close", false);

		return newHadoopConfiguration;
	}

	/**
	 * Returns an instantiated kaboomCurator framework object
	 *
	 * @return CuratorFramework
	 */
	private CuratorFramework buildCuratorFramework(String connectionString) {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		LOG.info("attempting to connect to ZK with connection string {}", connectionString);
		String[] connStringAndPrefix = connectionString.split("/", 2);
		CuratorFramework newCurator;
		if (connStringAndPrefix.length == 1) {
			newCurator = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
		} else {
			newCurator = CuratorFrameworkFactory.builder()
				 .namespace(connStringAndPrefix[1])
				 .connectString(connStringAndPrefix[0]).retryPolicy(retryPolicy)
				 .build();
		}
		newCurator.start();
		return newCurator;
	}
	
	public String zkPathPartitionAssignment(String partitionId) {
		return String.format("%s/%s", getZkRootPathPartitionAssignments(),  partitionId); 
	}

	/**
	 * @return the kaboomId
	 */
	public int getKaboomId() {
		return kaboomId;
	}

	/**
	 * @return the weight
	 */
	public int getWeight() {
		return weight;
	}

	/**
	 * @return the hadoopConfiguration
	 */
	public Configuration getHadoopConfiguration() {
		return hadoopConfiguration;
	}

	/**
	 * @return the kaboomCurator
	 */
	public CuratorFramework getKaBoomCurator() {
		return kaboomCurator;
	}

	/**
	 * @return the boomFilePerms
	 */
	public FsPermission getBoomFilePerms() {
		return boomFilePerms;
	}

	/**
	 * @return the loadBalancerType
	 */
	public String getLoadBalancerType() {
		return loadBalancerType;
	}

	/**
	 * @return the consumerConfiguration
	 */
	public ConsumerConfiguration getConsumerConfiguration() {
		return consumerConfiguration;
	}

	/**
	 * @return the runningConfig
	 */
	public RunningConfig getRunningConfig() {
		return runningConfig;
	}

	/**
	 * @return the deadWorkerMeter
	 */
	public Meter getDeadWorkerMeter() {
		return deadWorkerMeter;
	}

	/**
	 * @return the gracefulWorkerShutdownMeter
	 */
	public Meter getGracefulWorkerShutdownMeter() {
		return gracefulWorkerShutdownMeter;
	}

	/**
	 * @return the zkRootPathKaBoom
	 */
	public String getZkRootPathKaBoom() {
		return zkRootPathKaBoom;
	}

	/**
	 * @return the zkRootPathTopicConfigs
	 */
	public String getZkRootPathTopicConfigs() {
		return zkRootPathTopicConfigs;
	}

	/**
	 * @return the hostname
	 */
	public String getHostname() {
		return hostname;
	}

	/**
	 * @return the zkPathRunningConfig
	 */
	public String getZkPathRunningConfig() {
		return zkPathRunningConfig;
	}

	/**
	 * @return the zkRootPathClients
	 */
	public String getZkRootPathClients() {
		return zkRootPathClients;
	}

	/**
	 * @return the zkPathLeaderClientId
	 */
	public String getZkPathLeaderClientId() {
		return zkPathLeaderClientId;
	}

	/**
	 * @return the zkRootPathFlagAssignments
	 */
	public String getZkRootPathFlagAssignments() {
		return zkRootPathFlagAssignments;
	}

	/**
	 * @return the kafkaSeedBrokers
	 */
	public String getKafkaSeedBrokers() {
		return kafkaSeedBrokers;
	}

	/**
	 * @return the zkRootPathKafka
	 */
	public String getZkRootPathKafka() {
		return zkRootPathKafka;
	}

	/**
	 * @return the kafkaCurator
	 */
	public CuratorFramework getKafkaCurator() {
		return kafkaCurator;
	}

	/**
	 * @return the zkRootPathKafkaBrokers
	 */
	public String getZkRootPathKafkaBrokers() {
		return zkRootPathKafkaBrokers;
	}

	/**
	 * @return the zkRootPathPartitionAssignments
	 */
	public String getZkRootPathPartitionAssignments() {
		return zkRootPathPartitionAssignments;
	}

	/**
	 * @return the hadoopUrlPath
	 */
	public Path getHadoopUrlPath() {
		return hadoopUrlPath;
	}

}