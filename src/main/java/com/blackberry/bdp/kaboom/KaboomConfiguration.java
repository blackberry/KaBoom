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

import java.util.Map;
import com.blackberry.bdp.krackle.consumer.ConsumerConfiguration;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import com.blackberry.bdp.common.utils.props.Parser;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author dariens
 */
public class KaboomConfiguration
{
	private static final String defaultProperyFile = "kaboom.properties";
	private final Object fsLock = new Object();
	private final Path hadoopUrlPath;
	private int kaboomId;
	private long fileRotateInterval;
	private int weight;
	private final CuratorFramework curator;
	private final Map<String, ArrayList<TimeBasedHdfsOutputPath>> topicToHdfsPaths = new HashMap<>();
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
		
		for (Map.Entry<String, String> entry : getTopicToProxyUser().entrySet())
		{
			LOG.info("topicToProxyUser: {} -> {}", entry.getKey(), entry.getValue());
		}
		
		for (Map.Entry<String, String> entry : getTopicToHdfsRoot().entrySet())
		{
			LOG.info("topicToKrFlagPath: {} -> {}", entry.getKey(), entry.getValue());
		}

		for (Map.Entry<String, ArrayList<TimeBasedHdfsOutputPath>> entry : getTopicToHdfsPaths().entrySet())
		{
			ArrayList<TimeBasedHdfsOutputPath> outputPaths = entry.getValue();
				 
			for (TimeBasedHdfsOutputPath outputPath : outputPaths)
			{	
				LOG.info("topic: {} -> {}", entry.getKey(), outputPath);
			}
		}

		LOG.info(" *** end dumping configuration *** ");
	}
	
	public KaboomConfiguration (Properties props) throws Exception
	{
		Parser propsParser = new Parser(props);
		
		hadoopUrlPath = new Path(propsParser.parseString("hadooop.fs.uri"));		
		periodicHdfsFlushInterval = propsParser.parseLong("kabom.boomWriter.periodicHdfsFlushInterval", 0l);
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
		curator = buildCuratorFramework();
		
		hadoopConfiguration = buildHadoopConfiguration();
		
		mapTopicToProxyUser(props);
		mapProxyUserToHadoopFileSystem();		
		mapTopicToHdfsPathFromProps(props);
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
	 * @param props Properties to parse for topics and paths
	 * @return Map<String, String>
	 */
	private void mapTopicToHdfsPathFromProps(Properties props)
	{
		Pattern topicPathPattern = Pattern.compile("^topic\\.([^\\.]+)\\.hdfsDir\\.(\\d+)");

		for (Map.Entry<Object, Object> e : props.entrySet())
		{
			Matcher m = topicPathPattern.matcher(e.getKey().toString());
			if (m.matches())
			{								
				String topic = m.group(1);				
				String pathNumber = m.group(2);
				
				if (!props.containsKey(String.format("topic.%s.hdfsRootDir", topic)))
				{
					LOG.error("Topic {} configuration property missing", String.format("topic.%s.hdfsRootDir", topic));
					continue;
				}
				
				String hdfsRootDir = hadoopUrlPath + props.getProperty(String.format("topic.%s.hdfsRootDir", topic));				
				
				if (!topicToHdfsRootDir.containsKey(topic))
				{					
					topicToHdfsRootDir.put(topic, hdfsRootDir);
				}
				
				String directory = String.format("%s/%s", hdfsRootDir, e.getValue().toString());
				String durationProperty = String.format("topic.%s.hdfsDir.%d.duration", topic, Integer.parseInt(pathNumber));
				Integer duration = Integer.parseInt(props.getProperty(durationProperty, "180"));				

				LOG.info("HDFS output path property matched topic: {} path number: {} duration: {} directory: {}", topic, pathNumber, duration, directory);					
				
				TimeBasedHdfsOutputPath path = new TimeBasedHdfsOutputPath(proxyUserToFileSystem.get(topicToProxyUser.get(topic)), directory, duration);
				
				path.setPeriodicHdfsFlushInterval(periodicHdfsFlushInterval);
				
				ArrayList<TimeBasedHdfsOutputPath> paths = topicToHdfsPaths.get(topic);
				
				if (paths == null)
				{
					paths = new ArrayList<>();
					paths.add(path);
					topicToHdfsPaths.put(topic, paths);
				}
				else
				{
					paths.add(path);
				}
				
				if (false == getTopicToSupportedStatus().containsKey(topic))
				{
					getTopicToSupportedStatus().put(topic, true);
				}
			}
		}
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
		for (Map.Entry<String, String> entry : topicToProxyUser.entrySet())
		{
			final String proxyUser = entry.getValue();
			
			if (proxyUserToFileSystem.containsKey(proxyUser))
			{
				continue;
			}
			
			try
			{
				LOG.info("Using kerberos uthentication.");
				LOG.info("Kerberos principal = {}", getKerberosPrincipal());
				LOG.info("Kerberos keytab = {}", getKerberosKeytab());			
			
				Authenticator.getInstance().setKerbConfPrincipal(getKerberosPrincipal());
				Authenticator.getInstance().setKerbKeytab(getKerberosKeytab());
				
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
								 proxyUserToFileSystem.put(proxyUser, fs);
								 								 
								 LOG.info("Opening {} for proxy user {}", hadoopUrlPath, proxyUser);
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
	 * @return the topicToHdfsPaths
	 */
	public Map<String, ArrayList<TimeBasedHdfsOutputPath>> getTopicToHdfsPaths()
	{
		return topicToHdfsPaths;
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
}
