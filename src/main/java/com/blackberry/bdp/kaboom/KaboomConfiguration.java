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

import com.blackberry.bdp.krackle.consumer.ConsumerConfiguration;
import com.blackberry.bdp.common.jmx.MetricRegistrySingleton;

import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import com.blackberry.bdp.common.props.Parser;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

/**
 *
 * @author dariens
 */
public class KaboomConfiguration
{
	private static final String defaultProperyFile = "kaboom.properties";
	private final Properties props;
	private final Parser propsParser;
	private final Object fsLock = new Object();
	private final Path hadoopUrlPath;
	private int kaboomId;
	private long fileRotateInterval;
	private int weight;
	private final CuratorFramework curator;	
	private final Map<String, String> topicToProxyUser = new HashMap<>();
	private final Map<String, FileSystem> proxyUserToFileSystem = new HashMap<>();
	private final Map<String, String> topicToHdfsRootDir = new HashMap<>();
	private final Map<String, Boolean> topicToSupportedStatus = new HashMap<>();
	private String kerberosPrincipal;
	private String kerberosKeytab;
	private String hostname;
	private String kaboomZkConnectionString;
	private String kafkaZkConnectionString;
	private Boolean allowOffsetOverrides;
	private Boolean sinkToHighWatermark;
	private ConsumerConfiguration consumerConfiguration;
	private Configuration hadoopConfiguration;
	private String kafkaSeedBrokers;
	private Integer readyFlagPrevHoursCheck;
	private final Boolean useTempOpenFileDirectory;
	private final Long periodicHdfsFlushInterval;
	private final Long periodicFileCloseInterval;
	private final Map<String, Meter> topicToBoomWritesMeter = new HashMap<>();
	private final Map<String, Timer> topicToHdfsFlushTimer = new HashMap<>();
	private final Map<String, Timer> topicToCompressionTimer = new HashMap<>();
	private final Map<String, Histogram> topicToCompressionRatioHistogram = new HashMap<>();
	private final Map<String, Short> topicToCompressionLevel = new HashMap<>();
	private final Meter totalBoomWritesMeter;
	private final Timer totalHdfsFlushTimer;
	private final Timer totalCompressionTimer;
	private final Histogram totalCompressionRatioHistogram;
	private final Boolean useNativeCompression;
	private final String loadBalancer;
	private long leaderSleepDurationMs = 10 * 60 * 1000;
	private short defaultCompressionLevel = 6;
	
	/**
	 * These are required for boom files
	 */
	
	private final FsPermission boomFilePerms = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);
	
	private int boomFileBufferSize = 16 * 1024;
	private short boomFileReplicas = 3;
	private long boomFileBlocksize = 256 * 1024 * 1024;		
	private String boomFileTmpPrefix = "_tmp_";	

	
	private static final Logger LOG = LoggerFactory.getLogger(KaboomConfiguration.class);
	
	public void logConfiguraton()
	{
		LOG.info(" *** start dumping configuration *** ");
		LOG.info("kaboomId: {}", getKaboomId());
		LOG.info("fileRotateInterval: {}", getFileRotateInterval());
		LOG.info("weight: {}", getWeight());
		LOG.info("kerberosPrincipal: {}", getKerberosKeytab());
		LOG.info("kerberosKeytab: {}", getKerberosKeytab());
		LOG.info("hostname: {}", getHostname());
		LOG.info("kaboomZkConnectionString: {}", getKaboomZkConnectionString());
		LOG.info("kafkaZkConnectionString: {}", getKafkaSeedBrokers());
		LOG.info("allowOffsetOverrides: {}", getAllowOffsetOverrides());
		LOG.info("sinkToHighWatermark: {}", getSinkToHighWatermark());
		LOG.info("kafkaSeedBrokers: {}", getKafkaSeedBrokers());
		LOG.info("readyFlagPrevHoursCheck: {}", getReadyFlagPrevHoursCheck());
		LOG.info("useTempOpenFileDirectory: {}", getUseTempOpenFileDirectory());
		LOG.info("periodicHdfsFlushInterval: {}", getPeriodicHdfsFlushInterval());		
		LOG.info("periodicFileCloseInterval: {}", getPeriodicFileCloseInterval());
		LOG.info("leaderSleepDurationMs: {}", getLeaderSleepDurationMs());
		LOG.info("defaultCompressionLevel: {}", getDefaultCompressionLevel());
		
		LOG.info("boomFileBufferSize: {}", getBoomFileBufferSize());
		LOG.info("boomFileReplicas: {}", getBoomFileReplicas());
		LOG.info("boomFileBlocksize: {}", getBoomFileBlocksize());
		LOG.info("useNativeCompression: {}", getUseNativeCompression());
		LOG.info("loadBalancer: {}", getLoadBalancer());
		LOG.info("Using kerberos uthentication.");
		LOG.info("Kerberos principal = {}", getKerberosPrincipal());
		LOG.info("Kerberos keytab = {}", getKerberosKeytab());
		
		for (Map.Entry<String, String> entry : getTopicToProxyUser().entrySet())
		{
			LOG.debug("topicToProxyUser: {} -> {}", entry.getKey(), entry.getValue());
		}
		
		for (Map.Entry<String, String> entry : getTopicToHdfsRoot().entrySet())
		{
			LOG.debug("topicToKrFlagPath: {} -> {}", entry.getKey(), entry.getValue());
		}

		LOG.info(" *** end dumping configuration *** ");
	}
	
	public KaboomConfiguration (Properties props) throws Exception
	{
		propsParser = new Parser(props);
		
		this.props = props;		
		
		hadoopUrlPath = new Path(propsParser.parseString("hadooop.fs.uri"));		
		totalBoomWritesMeter = MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:total:boom writes");
		totalHdfsFlushTimer = MetricRegistrySingleton.getInstance().getMetricsRegistry().timer("kaboom:total:hdfs flush timer");
		totalCompressionTimer = MetricRegistrySingleton.getInstance().getMetricsRegistry().timer("kaboom:total:compression timer");
		totalCompressionRatioHistogram = MetricRegistrySingleton.getInstance().getMetricsRegistry().histogram("kaboom:total:compression ratio");
		
		consumerConfiguration = new ConsumerConfiguration(props);
		kaboomId = propsParser.parseInteger("kaboom.id");
		fileRotateInterval = propsParser.parseLong("fileRotateInterval", 60L * 3L * 1000L);
		weight = propsParser.parseInteger("kaboom.weighting", Runtime.getRuntime().availableProcessors());
		allowOffsetOverrides = propsParser.parseBoolean("kaboom.allowOffsetOverrides", false);
		sinkToHighWatermark = propsParser.parseBoolean("kaboom.sinkToHighWatermark", false);
		kerberosKeytab = propsParser.parseString("kerberos.keytab");
		kerberosPrincipal = propsParser.parseString("kerberos.principal");
		hostname = propsParser.parseString("kaboom.hostname", InetAddress.getLocalHost().getHostName());
		kaboomZkConnectionString = propsParser.parseString("zookeeper.connection.string");
		kafkaZkConnectionString = propsParser.parseString("kafka.zookeeper.connection.string");
		kafkaSeedBrokers = propsParser.parseString("metadata.broker.list");
		readyFlagPrevHoursCheck = propsParser.parseInteger("kaboom.readyflag.prevhours", 24);
		useTempOpenFileDirectory = propsParser.parseBoolean("kaboom.useTempOpenFileDirectory", true);				
		useNativeCompression = propsParser.parseBoolean("kaboom.use.native.compression", false);
		loadBalancer = propsParser.parseString("kaboom.load.balancer.type", "even");
		leaderSleepDurationMs = propsParser.parseLong("leader.sleep.duration.ms", leaderSleepDurationMs);
		defaultCompressionLevel = propsParser.parseShort("kaboom.deflate.compression.level", defaultCompressionLevel);
		
		boomFileBufferSize = propsParser.parseInteger("boom.file.buffer.size", boomFileBufferSize);
		boomFileReplicas = propsParser.parseShort("boom.file.replicas", boomFileReplicas);
		boomFileBlocksize = propsParser.parseLong("boom.file.block.size", boomFileBlocksize);
		boomFileTmpPrefix = propsParser.parseString("boom.file.temp.prefix", boomFileTmpPrefix);
		periodicHdfsFlushInterval = propsParser.parseLong("boom.file.flush.interval", 30 * 1000l);
		periodicFileCloseInterval = propsParser.parseLong("boom.file.close.expired.interval", 60 * 1000l);
		
		mapTopicsToSupportedStatus();
		curator = buildCuratorFramework();		
		hadoopConfiguration = buildHadoopConfiguration();		
		mapTopicToProxyUser(props);
		mapProxyUserToHadoopFileSystem();
	}
	
	private void mapTopicsToSupportedStatus()
	{
		Pattern topicPathPattern = Pattern.compile("^topic\\.([^\\\\.]+)\\.hdfsRootDir");

		for (Map.Entry<Object, Object> e : props.entrySet())
		{
			Matcher m = topicPathPattern.matcher(e.getKey().toString());
			if (m.matches())
			{								
				String topic = m.group(1);
				
				if (!getTopicToSupportedStatus().containsKey(topic))
				{
					getTopicToSupportedStatus().put(topic, true);
				}

				String hdfsRootDir = hadoopUrlPath + props.getProperty(String.format("topic.%s.hdfsRootDir", topic));				
				
				if (!topicToHdfsRootDir.containsKey(topic))
				{					
					topicToHdfsRootDir.put(topic, hdfsRootDir);
				}
				
				if (!topicToBoomWritesMeter.containsKey(topic))
				{
					topicToBoomWritesMeter.put(topic, MetricRegistrySingleton.getInstance().getMetricsRegistry().meter("kaboom:topic:" + topic + ":boom writes"));
				}

				if (!topicToHdfsFlushTimer.containsKey(topic))
				{
					topicToHdfsFlushTimer.put(topic, MetricRegistrySingleton.getInstance().getMetricsRegistry().timer("kaboom:topic:" + topic + ":hdfs flush timer"));
				}
				
				if (!topicToCompressionTimer.containsKey(topic))
				{
					topicToCompressionTimer.put(topic, MetricRegistrySingleton.getInstance().getMetricsRegistry().timer("kaboom:topic:" + topic + ":compression timer"));
				}
				
				if (!topicToCompressionRatioHistogram.containsKey(topic))
				{
					topicToCompressionRatioHistogram.put(topic, MetricRegistrySingleton.getInstance().getMetricsRegistry().histogram("kaboom:topic:" + topic + ":compresssion ratio"));
				}
				
				String topicCompressionPropName = String.format("topic.%s.deflate.compression.level", topic);
				
				try
				{
					short topicCompressionLevel = defaultCompressionLevel;
					if (props.containsKey(topicCompressionPropName))
					{
						topicCompressionLevel = propsParser.parseShort(topicCompressionPropName);
						LOG.info("Topic {} configured with a non-default compression level: {} ({} is the default)", topic, topicCompressionLevel, defaultCompressionLevel);
					}
					topicToCompressionLevel.put(topic, topicCompressionLevel);
				}
				catch (Exception ex)
				{
					LOG.error("Failed to set the topic specific compression level for topic {}", topic, e);
				}
				
			}
		}
	}	
	
	/**
	 * Builds the topic to an HDFS paths Map
	 * 
	 * Sample property definition:
	 * 
	 * topic.devtest-test1.hdfsRootDir=hdfs://hadoop.log82.bblabs/service/82/devtest/logs/%y%M%d/%H/test1
	 * topic.devtest-test1.proxy.user=dariens
	 * topic.devtest-test1.hdfsDir.1=incoming/%l
	 * topic.devtest-test1.hdfsDir.2=hourly_temp
	 * topic.devtest-test1.hdfsDir.2.duration=3600
	 *
	 * @param topic
	 * @return ArrayList<TimeBasedHdfsOutputPath>

	 */
	public ArrayList<TimeBasedHdfsOutputPath> getHdfsPathsForTopic(String topic)
	{
		ArrayList<TimeBasedHdfsOutputPath> paths = new ArrayList<>();
		
		Pattern topicPathPattern = Pattern.compile("^topic\\." + topic + "\\.hdfsDir\\.(\\d+)");

		for (Map.Entry<Object, Object> e : props.entrySet())
		{
			Matcher m = topicPathPattern.matcher(e.getKey().toString());
			if (m.matches())
			{								
				String pathNumber = m.group(1);
				
				if (!props.containsKey(String.format("topic.%s.hdfsRootDir", topic)))
				{
					LOG.error("Topic {} configuration property missing", String.format("topic.%s.hdfsRootDir", topic));
					continue;
				}
				
				String hdfsRootDir = hadoopUrlPath + props.getProperty(String.format("topic.%s.hdfsRootDir", topic));				
				String directory = String.format("%s/%s", hdfsRootDir, e.getValue().toString());
				String durationProperty = String.format("topic.%s.hdfsDir.%d.duration", topic, Integer.parseInt(pathNumber));
				Integer duration = Integer.parseInt(props.getProperty(durationProperty, "180"));				

				LOG.debug("HDFS output path property matched topic: {} path number: {} duration: {} directory: {}", topic, pathNumber, duration, directory);					
				
				TimeBasedHdfsOutputPath path = new TimeBasedHdfsOutputPath(
					 getProxyUserToFileSystem().get(topicToProxyUser.get(topic)), 
					 topic,
					 this,
					 directory, 
					 duration);				
				
				paths.add(path);
				
				if (!getTopicToSupportedStatus().containsKey(topic))
				{
					getTopicToSupportedStatus().put(topic, true);
				}
			}
		}
		
		return paths;
	}

	/**
	 * Builds the topic to proxy users map
	 *
	 * @param props Properties to parse for topics and users
	 */
	private void  mapTopicToProxyUser(Properties props) 
	{
		Pattern topicProxyUsersPattern = Pattern.compile("^topic\\.([^\\.]+)\\.proxy.user$");

		for (Map.Entry<Object, Object> e : props.entrySet())
		{
			String proxyUser = e.getValue().toString();			
				 
			Matcher m = topicProxyUsersPattern.matcher(e.getKey().toString());
			if (m.matches())
			{				
				topicToProxyUser.put(m.group(1), proxyUser);
			}
		}		
	}
	
	/**
	 * Builds the proxy user to hadoop file system map
	 */
	private void mapProxyUserToHadoopFileSystem() 
	{
		Authenticator.getInstance().setKerbConfPrincipal(getKerberosPrincipal());
		Authenticator.getInstance().setKerbKeytab(getKerberosKeytab());
		
		for (Map.Entry<String, String> entry : topicToProxyUser.entrySet())
		{
			final String proxyUser = entry.getValue();
			
			if (getProxyUserToFileSystem().containsKey(proxyUser))
			{
				continue;
			}
			
			try
			{	
				LOG.info("Attempting to create file system {} for {}", hadoopUrlPath, proxyUser);
				Authenticator.getInstance().runPrivileged(proxyUser, new PrivilegedExceptionAction<Void>()
				 {
					 @Override
					 public Void run() throws Exception
					 {
						 synchronized (fsLock)
						 {							 
							 try
							 {
								 FileSystem fs = hadoopUrlPath.getFileSystem(hadoopConfiguration);
								 getProxyUserToFileSystem().put(proxyUser, fs);
								 								 
								 LOG.debug("Opening {} for proxy user {}", hadoopUrlPath, proxyUser);
							 }
							 catch (IOException ioe)
							 {
								 LOG.error("Error getting file system {} for proxy user {}", hadoopUrlPath, ioe);
							 }
						 }

						 return null;
					 }
				 });
			} 
			catch (Exception e)
			{
				LOG.error("Error creating file.", e);
			}
		}		
	}

	/**
	 * Instantiates properties from either the specified configuration file or the default for the class
	 *
	 * @return Properties
	 */
	public static Properties getProperties()
	{
		Properties props = new Properties();
		try
		{
			InputStream propsIn;
			
			if (System.getProperty("kaboom.configuration") != null)
			{
				propsIn = new FileInputStream(System.getProperty("kaboom.configuration"));
				props.load(propsIn);
			} 
			else
			{
				LOG.info("Loading configs from default properties file {}", KaBoom.class.getClassLoader().getResource(defaultProperyFile));
				propsIn = KaBoom.class.getClassLoader().getResourceAsStream(defaultProperyFile);
				props.load(propsIn);
			}
		}
		catch (Throwable t)
		{
			System.err.println("Error getting config file:");
			System.err.printf("stacktrace: %s" , t.getStackTrace().toString());
			System.exit(1);
		}

		return props;		
	}
	
	/**
	 * Instantiates properties from either the specified configuration file or
	 * the default for class
	 *
	 * @return Configuration
	 */
	private Configuration buildHadoopConfiguration() throws FileNotFoundException
	{		
		Configuration newHadoopConfiguration = new Configuration();
	
		if (new File("/etc/hadoop/conf/core-site.xml").exists())
		{
			newHadoopConfiguration.addResource(new FileInputStream("/etc/hadoop/conf/core-site.xml"));
		}
		
		if (new File("/etc/hadoop/conf/hdfs-site.xml").exists())
		{
			newHadoopConfiguration.addResource(new FileInputStream("/etc/hadoop/conf/hdfs-site.xml"));
		}
		
		// Adds any more standard configs we find in the classpath
		
		for (String file : new String[] { "core-site.xml", "hdfs-site.xml"	})
		{
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);
			
			if (in != null)
			{
				newHadoopConfiguration.addResource(in);
			}
		}
		
		newHadoopConfiguration.setBoolean("fs.automatic.close", false);
		
		return newHadoopConfiguration;
	}
	
	/**
	 * Returns an instantiated curator framework object
	 * 
	 * @return CuratorFramework
	 */
	private CuratorFramework buildCuratorFramework()
	{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		
		LOG.info("attempting to connect to ZK with connection string {}", kaboomZkConnectionString);
		
		String[] connStringAndPrefix = getKaboomZkConnectionString().split("/", 2);
		
		CuratorFramework newCurator;
		
		if (connStringAndPrefix.length == 1)
		{
			newCurator = CuratorFrameworkFactory.newClient(kaboomZkConnectionString, retryPolicy);
		}
		else
		{
			newCurator = CuratorFrameworkFactory.builder()
				 .namespace(connStringAndPrefix[1])
				 .connectString(connStringAndPrefix[0]).retryPolicy(retryPolicy)
				 .build();
		}
		
		newCurator.start();
		return newCurator;			
	}
	
	/**
	 * @return the kaboomId
	 */
	public int getKaboomId()
	{
		return kaboomId;
	}

	/**
	 * @param kaboomId the kaboomId to set
	 */
	public void setKaboomId(int kaboomId)
	{
		this.kaboomId = kaboomId;
	}

	/**
	 * @return the fileRotateInterval
	 */
	public long getFileRotateInterval()
	{
		return fileRotateInterval;
	}

	/**
	 * @param fileRotateInterval the fileRotateInterval to set
	 */
	public void setFileRotateInterval(long fileRotateInterval)
	{
		this.fileRotateInterval = fileRotateInterval;
	}

	/**
	 * @return the weight
	 */
	public int getWeight()
	{
		return weight;
	}

	/**
	 * @param weight the weight to set
	 */
	public void setWeight(int weight)
	{
		this.weight = weight;
	}

	/**
	 * @return the topicToProxyUser
	 */
	public Map<String, String> getTopicToProxyUser()
	{
		return topicToProxyUser;
	}

	/**
	 * @return the kerberosPrincipal
	 */
	public String getKerberosPrincipal()
	{
		return kerberosPrincipal;
	}

	/**
	 * @param kerberosPrincipal the kerberosPrincipal to set
	 */
	public void setKerberosPrincipal(String kerberosPrincipal)
	{
		this.kerberosPrincipal = kerberosPrincipal;
	}

	/**
	 * @return the kerberosKeytab
	 */
	public String getKerberosKeytab()
	{
		return kerberosKeytab;
	}

	/**
	 * @param kerberosKeytab the kerberosKeytab to set
	 */
	public void setKerberosKeytab(String kerberosKeytab)
	{
		this.kerberosKeytab = kerberosKeytab;
	}

	/**
	 * @return the hostname
	 */
	public String getHostname()
	{
		return hostname;
	}

	/**
	 * @param hostname the hostname to set
	 */
	public void setHostname(String hostname)
	{
		this.hostname = hostname;
	}

	/**
	 * @return the kaboomZkConnectionString
	 */
	public String getKaboomZkConnectionString()
	{
		return kaboomZkConnectionString;
	}

	/**
	 * @param kaboomZkConnectionString the kaboomZkConnectionString to set
	 */
	public void setKaboomZkConnectionString(String kaboomZkConnectionString)
	{
		this.kaboomZkConnectionString = kaboomZkConnectionString;
	}

	/**
	 * @return the allowOffsetOverrides
	 */
	public Boolean getAllowOffsetOverrides()
	{
		return allowOffsetOverrides;
	}

	/**
	 * @param allowOffsetOverrides the allowOffsetOverrides to set
	 */
	public void setAllowOffsetOverrides(Boolean allowOffsetOverrides)
	{
		this.allowOffsetOverrides = allowOffsetOverrides;
	}

	/**
	 * @return the sinkToHighWatermark
	 */
	public Boolean getSinkToHighWatermark()
	{
		return sinkToHighWatermark;
	}

	/**
	 * @param sinkToHighWatermark the sinkToHighWatermark to set
	 */
	public void setSinkToHighWatermark(Boolean sinkToHighWatermark)
	{
		this.sinkToHighWatermark = sinkToHighWatermark;
	}

	/**
	 * @return the consumerConfiguration
	 */
	public ConsumerConfiguration getConsumerConfiguration()
	{
		return consumerConfiguration;
	}

	/**
	 * @param consumerConfiguration the consumerConfiguration to set
	 */
	public void setConsumerConfiguration(ConsumerConfiguration consumerConfiguration)
	{
		this.consumerConfiguration = consumerConfiguration;
	}

	/**
	 * @return the hadoopConfiguration
	 */
	public Configuration getHadoopConfiguration()
	{
		return hadoopConfiguration;
	}

	/**
	 * @param hadoopConfiguration the hadoopConfiguration to set
	 */
	public void setHadoopConfiguration(Configuration hadoopConfiguration)
	{
		this.hadoopConfiguration = hadoopConfiguration;
	}

	/**
	 * @return the kafkaSeedBrokers
	 */
	public String getKafkaSeedBrokers()
	{
		return kafkaSeedBrokers;
	}

	/**
	 * @param kafkaSeedBrokers the kafkaSeedBrokers to set
	 */
	public void setKafkaSeedBrokers(String kafkaSeedBrokers)
	{
		this.kafkaSeedBrokers = kafkaSeedBrokers;
	}

	/**
	 * @return the readyFlagPrevHoursCheck
	 */
	public Integer getReadyFlagPrevHoursCheck()
	{
		return readyFlagPrevHoursCheck;
	}

	/**
	 * @param readyFlagPrevHoursCheck the readyFlagPrevHoursCheck to set
	 */
	public void setReadyFlagPrevHoursCheck(Integer readyFlagPrevHoursCheck)
	{
		this.readyFlagPrevHoursCheck = readyFlagPrevHoursCheck;
	}

	/**
	 * @return the kafkaZkConnectionString
	 */
	public String getKafkaZkConnectionString()
	{
		return kafkaZkConnectionString;
	}

	/**
	 * @param kafkaZkConnectionString the kafkaZkConnectionString to set
	 */
	public void setKafkaZkConnectionString(String kafkaZkConnectionString)
	{
		this.kafkaZkConnectionString = kafkaZkConnectionString;
	}

	/**
	 * @return the topicToHdfsRootDir
	 */
	public Map<String, String> getTopicToHdfsRoot()
	{
		return topicToHdfsRootDir;
	}

	/**
	 * @return the hadoopUrlPath
	 */
	public Path getHadoopUrlPath()
	{
		return hadoopUrlPath;
	}

	/**
	 * @return the topicToSupportedStatus
	 */
	public Map<String, Boolean> getTopicToSupportedStatus()
	{
		return topicToSupportedStatus;
	}

	/**
	 * @return the curator
	 */
	public CuratorFramework getCurator()
	{
		return curator;
	}

	/**
	 * @return the useTempOpenFileDirectory
	 */
	public Boolean getUseTempOpenFileDirectory()
	{
		return useTempOpenFileDirectory;
	}

	/**
	 * @return the periodicHdfsFlushInterval
	 */
	public Long getPeriodicHdfsFlushInterval()
	{
		return periodicHdfsFlushInterval;
	}

	/**
	 * @return the boomFilePerms
	 */
	public FsPermission getBoomFilePerms()
	{
		return boomFilePerms;
	}

	/**
	 * @return the boomFileBufferSize
	 */
	public int getBoomFileBufferSize()
	{
		return boomFileBufferSize;
	}

	/**
	 * @return the boomFileReplicas
	 */
	public short getBoomFileReplicas()
	{
		return boomFileReplicas;
	}

	/**
	 * @return the boomFileBlocksize
	 */
	public long getBoomFileBlocksize()
	{
		return boomFileBlocksize;
	}

	/**
	 * @return the boomFileTmpPrefix
	 */
	public String getBoomFileTmpPrefix()
	{
		return boomFileTmpPrefix;
	}

	/**
	 * @return the topicToBoomWritesMeter
	 */
	public Map<String, Meter> getTopicToBoomWrites()
	{
		return topicToBoomWritesMeter;
	}

	/**
	 * @return the totalBoomWritesMeter
	 */
	public Meter getTotalBoomWritesMeter()
	{
		return totalBoomWritesMeter;
	}

	/**
	 * @return the topicToHdfsFlushTimer
	 */
	public Map<String, Timer> getTopicToHdfsFlushTimer()
	{
		return topicToHdfsFlushTimer;
	}

	/**
	 * @return the totalHdfsFlushTimer
	 */
	public Timer getTotalHdfsFlushTimer()
	{
		return totalHdfsFlushTimer;
	}

	/**
	 * @return the periodicFileCloseInterval
	 */
	public Long getPeriodicFileCloseInterval()
	{
		return periodicFileCloseInterval;
	}

	/**
	 * @return the totalCompressionTimer
	 */
	public Timer getTotalCompressionTimer()
	{
		return totalCompressionTimer;
	}

	/**
	 * @return the useNativeCompression
	 */
	public Boolean getUseNativeCompression()
	{
		return useNativeCompression;
	}

	/**
	 * @return the loadBalancer
	 */
	public String getLoadBalancer()
	{
		return loadBalancer;
	}

	/**
	 * @return the leaderSleepDurationMs
	 */
	public long getLeaderSleepDurationMs()
	{
		return leaderSleepDurationMs;
	}

	/**
	 * @return the proxyUserToFileSystem
	 */
	public Map<String, FileSystem> getProxyUserToFileSystem()
	{
		return proxyUserToFileSystem;
	}

	/**
	 * @return the topicToCompressionRatioHistogram
	 */
	public Map<String, Histogram> getTopicToCompressionRatioHistogram()
	{
		return topicToCompressionRatioHistogram;
	}

	/**
	 * @return the topicToCompressionLevel
	 */
	public Map<String, Short> getTopicToCompressionLevel()
	{
		return topicToCompressionLevel;
	}

	/**
	 * @return the defaultCompressionLevel
	 */
	public short getDefaultCompressionLevel()
	{
		return defaultCompressionLevel;
	}

	/**
	 * @return the topicToCompressionTimer
	 */
	public Map<String, Timer> getTopicToCompressionTimer()
	{
		return topicToCompressionTimer;
	}

	/**
	 * @return the totalCompressionRatioHistogram
	 */
	public Histogram getTotalCompressionRatioHistogram()
	{
		return totalCompressionRatioHistogram;
	}
}
