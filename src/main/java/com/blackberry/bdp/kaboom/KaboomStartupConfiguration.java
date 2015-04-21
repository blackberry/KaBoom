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

import org.apache.curator.retry.ExponentialBackoffRetry;
import com.blackberry.bdp.common.props.Parser;
import com.blackberry.bdp.kaboom.api.KaBoomRunningConfiguration;
import com.blackberry.bdp.krackle.consumer.ConsumerConfiguration;
import org.apache.curator.framework.CuratorFrameworkFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 *
 * @author dariens
 */
public class KaboomStartupConfiguration
{
	private static final Logger LOG = LoggerFactory.getLogger(KaboomStartupConfiguration.class);
	
	private static final String defaultProperyFile = "kaboom.properties";
	private final Properties props;
	private final Parser propsParser;
	private final Object fsLock = new Object();
	
	private ConsumerConfiguration consumerConfiguration;
	private Configuration hadoopConfiguration;
	private String kafkaSeedBrokers;
	private final Path hadoopUrlPath;
	private int kaboomId;
	private int weight;
	private final CuratorFramework curator;	
	private String kerberosPrincipal;
	private String kerberosKeytab;
	private String hostname;
	private String kaboomZkConnectionString;
	private String kafkaZkConnectionString;
	private final String loadBalancer;
	private final KaBoomRunningConfiguration runningConfig;
	private final String runningConfigZkPath;
	
	//private final Map<String, String> topicToProxyUser = new HashMap<>();
	//private final Map<String, FileSystem> proxyUserToFileSystem = new HashMap<>();
	//private final Map<String, String> topicToHdfsRootDir = new HashMap<>();
	//private final Map<String, Boolean> topicToSupportedStatus = new HashMap<>();	
	
	/**
	 * POSIX style
	 * NONE("---"),
	 * EXECUTE("--x"),
	 * WRITE("-w-"),
	 * WRITE_EXECUTE("-wx"),
	 * READ("r--"),
	 * READ_EXECUTE("r-x"),
	 * READ_WRITE("rw-"),
	 * ALL("rwx");
	 */
	
	private final FsPermission boomFilePerms = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);

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
		LOG.info("kafkaSeedBrokers: {}", getKafkaSeedBrokers());		
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
	
	public KaboomStartupConfiguration (Properties props) throws Exception
	{
		propsParser = new Parser(props);
		
		this.props = props;
		
		/**
		 * Static configuration items: read once and remain fixed until Kaboom is restarted
		 */
		
		consumerConfiguration = new ConsumerConfiguration(props);
		kaboomId = propsParser.parseInteger("kaboom.id");
		hadoopConfiguration = buildHadoopConfiguration();
		curator = buildCuratorFramework();
		hadoopUrlPath = new Path(propsParser.parseString("hadooop.fs.uri"));
		weight = propsParser.parseInteger("kaboom.weighting", Runtime.getRuntime().availableProcessors());
		kerberosKeytab = propsParser.parseString("kerberos.keytab");
		kerberosPrincipal = propsParser.parseString("kerberos.principal");
		hostname = propsParser.parseString("kaboom.hostname", InetAddress.getLocalHost().getHostName());
		kaboomZkConnectionString = propsParser.parseString("zookeeper.connection.string");
		kafkaZkConnectionString = propsParser.parseString("kafka.zookeeper.connection.string");
		kafkaSeedBrokers = propsParser.parseString("metadata.broker.list");
		loadBalancer = propsParser.parseString("kaboom.load.balancer.type", "even");
		runningConfigZkPath = propsParser.parseString("kaboom.runningConfig.zkPath", "/kaboom/config");
		
		/**
		 * Build the maps we need to associate configuration
		 */
		
		mapTopicsToSupportedStatus();
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
	 * @return the boomFilePerms
	 */
	public FsPermission getBoomFilePerms()
	{
		return boomFilePerms;
	}

	/**
	 * @return the loadBalancer
	 */
	public String getLoadBalancer()
	{
		return loadBalancer;
	}

	/**
	 * @return the proxyUserToFileSystem
	 */
	public Map<String, FileSystem> getProxyUserToFileSystem()
	{
		return proxyUserToFileSystem;
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
	public KaBoomRunningConfiguration getRunningConfig() {
		return runningConfig;
	}

	/**
	 * @return the runningConfigZkPath
	 */
	public String getRunningConfigZkPath() {
		return runningConfigZkPath;
	}

}