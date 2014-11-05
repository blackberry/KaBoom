package com.blackberry.kaboom;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
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
import com.blackberry.common.props.Parser;
import java.io.FileNotFoundException;

public class KaBoom
{
	private static final String defaultProperyFile = "kaboom.properties";
	private static final Logger LOG = LoggerFactory.getLogger(KaBoom.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");	
	boolean shutdown = false;	
	private KaboomConfiguration config = new KaboomConfiguration();

	public static void main(String[] args) throws Exception
	{
		new KaBoom().run();
	}
	
	public KaBoom() throws Exception
	{		
	}
	
	/**
	 * Creates the topic to HDFS paths Map
	 *
	 * @param props Properties to parse for topics and paths
	 * @return Map<String, String>
	 */
	private Map<String, String> getTopicToHdfsPathFromProps(Properties props)
	{
		Pattern topicPathPattern = Pattern.compile("^topic\\.([^\\.]+)\\.path$");
		Map<String, String> topicFileLocation = new HashMap<String, String>();
		for (Entry<Object, Object> e : props.entrySet())
		{
			Matcher m = topicPathPattern.matcher(e.getKey().toString());
			if (m.matches())
			{
				topicFileLocation.put(m.group(1), e.getValue().toString());
			}
		}
		
		return topicFileLocation;
	}
	
	/**
	 * Creates the topic to proxy user Map
	 *
	 * @param props Properties to parse for topics and users
	 * @return Map<String, String>
	 */
	private Map<String, String> getTopicToProxyUserFromProps(Properties props) 
	{
		Pattern topicProxyUsersPattern = Pattern.compile("^topic\\.([^\\.]+)\\.proxy.user$");
		Map<String, String> topicProxyUsers = new HashMap<String, String>();

		for (Entry<Object, Object> e : props.entrySet())
		{
			Matcher m = topicProxyUsersPattern.matcher(e.getKey().toString());
			if (m.matches())
			{
				topicProxyUsers.put(m.group(1), e.getValue().toString());
			}
		}
		
		return topicProxyUsers;
	}
	
	/**
	 * Instantiates properties from either the specified configuration file or the default for the class
	 *
	 * @param props Properties to parse for topics and users
	 * @return Properties
	 */
	private Properties getProperties()
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
	public Configuration getHadoopConfiguration() throws FileNotFoundException
	{		
		final Configuration hadoopConfiguration = new Configuration();
	
		if (new File("/etc/hadoop/conf/core-site.xml").exists())
		{
			hadoopConfiguration.addResource(new FileInputStream("/etc/hadoop/conf/core-site.xml"));
		}
		
		if (new File("/etc/hadoop/conf/hdfs-site.xml").exists())
		{
			hadoopConfiguration.addResource(new FileInputStream("/etc/hadoop/conf/hdfs-site.xml"));
		}
		
		// Adds any more standard configs we find in the classpath
		
		for (String file : new String[] { "core-site.xml", "hdfs-site.xml"	})
		{
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);
			
			if (in != null)
			{
				hadoopConfiguration.addResource(in);
			}
		}
		
		hadoopConfiguration.setBoolean("fs.automatic.close", false);
		
		return hadoopConfiguration;
	}
	
	/**
	 * Returns an instantiated curator framework object
	 * 
	 * @return CuratorFramework
	 */
	public CuratorFramework getCuratorFramework()
	{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		
		LOG.info("attempting to connect to ZK with connection string {}", config.getKaboomZkConnectionString());
		
		String[] connStringAndPrefix = config.getKaboomZkConnectionString().split("/", 2);
		
		final CuratorFramework curator;
		
		if (connStringAndPrefix.length == 1)
		{
			curator = CuratorFrameworkFactory.newClient(config.getKaboomZkConnectionString(), retryPolicy);
		}
		else
		{
			curator = CuratorFrameworkFactory.builder()
				 .namespace(connStringAndPrefix[1])
				 .connectString(connStringAndPrefix[0]).retryPolicy(retryPolicy)
				 .build();
		}
		
		curator.start();
		return curator;			
	}
	
	private void run() throws Exception
	{

		if (Boolean.parseBoolean(System.getProperty("metrics.to.console", "false").trim()))
		{
			MetricRegistrySingleton.getInstance().enableConsole();
		}
		
		{
			Properties props = getProperties();
			Parser propsParser = new Parser(props);	
			
			if (propsParser.parseBoolean("configuration.authority.zk", false))
			{
				// TODO: ZK
			}
			else
			{
				LOG.info("Configuration authority is file based");		

				try
				{
					config.setConsumerConfiguration(new ConsumerConfiguration(props));				
					config.setKaboomId(propsParser.parseInteger("kaboom.id"));
					config.setFileRotateInterval(propsParser.parseLong("fileRotateInterval", 60L * 3L * 1000L));
					config.setWeight(propsParser.parseInteger("kaboom.weighting", Runtime.getRuntime().availableProcessors()));
					config.setAllowOffsetOverrides(propsParser.parseBoolean("kaboom.allowOffsetOverrides", false));
					config.setSinkToHighWatermark(propsParser.parseBoolean("kaboom.sinkToHighWatermark", false));
					config.setTopicToHdfsPath(getTopicToHdfsPathFromProps(props));
					config.setTopicToProxyUser(getTopicToProxyUserFromProps(props));
					config.setKerberosKeytab(propsParser.parseString("kerberos.keytab"));
					config.setKerberosPrincipal(propsParser.parseString("kerberos.principal"));
					config.setHadoopConfiguration(getHadoopConfiguration());
					config.setHostname(propsParser.parseString("kaboom.hostname", InetAddress.getLocalHost().getHostName()));
					config.setKaboomZkConnectionString(propsParser.parseString("zookeeper.connection.string"));
					config.setKafkaZkConnectionString(propsParser.parseString("kafka.zookeeper.connection.string"));
					config.setKafkaSeedBrokers(propsParser.parseString("metadata.broker.list"));					
					config.setReadyFlagPrevHoursCheck(propsParser.parseInteger("kaboom.readyflag.prevhours", 24));
				}
				catch (Exception e)
				{
					LOG.error("an error occured while building configuration object: ", e);
					throw e;
				}

			}		

		}
		
		/*
		 *
		 * Configure Kerkberos... This used to be optional as perhaps it was skipped when 
		 * folks were testing locally on their workstations, however considering how even
		 * our labs have thee ability to roll with Kerberos, let's make it required
		 *
		 */
		
		try
		{
			LOG.info("using kerberos authentication.");
			LOG.info("kerberos principal = {}", config.getKerberosPrincipal());
			LOG.info("kerberos keytab = {}", config.getKerberosKeytab());			
			
			Authenticator.getInstance().setKerbConfPrincipal(config.getKerberosPrincipal());
			Authenticator.getInstance().setKerbKeytab(config.getKerberosKeytab());
		}
		catch (Exception e)
		{
			LOG.error("there was an error configuring kerberos configuration: ", e);
		}

		MetricRegistrySingleton.getInstance().enableJmx();
		
		/**
		 * 
		 * Instantiate the ZK curator and ensure that the required nodes exist
		 * 
		 */
		
		final CuratorFramework curator = getCuratorFramework();
		
		for (String path : new String[] {"/kaboom/leader", "/kaboom/clients", "/kaboom/assignments"})
		{
			if (curator.checkExists().forPath(path) == null)
			{
				try
				{
					LOG.warn("the path {} was not found in ZK and needs to be created", path);
					curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
					LOG.warn("path {} created in ZK", path);
				} 
				catch (Exception e)
				{
					LOG.error("Error creating ZooKeeper node {} ", path, e);
				}
			}			
		}
		
		// Register my existence
		{
			Yaml yaml = new Yaml();
			ByteArrayOutputStream nodeOutputStream = new ByteArrayOutputStream();
			OutputStreamWriter writer = new OutputStreamWriter(nodeOutputStream);
			
			KaBoomNodeInfo data = new KaBoomNodeInfo();
			data.setHostname(config.getHostname());
			data.setWeight(config.getWeight());
			yaml.dump(data, writer);
			writer.close();
			byte[] nodeContents = nodeOutputStream.toByteArray();
			long backoff = 1000;
			long retries = 8;
			
			for (int i = 0; i < retries; i++)				
			{
				try
				{
					curator.create().withMode(CreateMode.EPHEMERAL).forPath("/kaboom/clients/" + config.getKaboomId(), nodeContents);
					break;
				} 
				catch (Exception e)
				{
					if (i <= retries)
					{
						LOG.warn("Failed attempt {}/{} to register with ZooKeeper.  Retrying in {} seconds", i, retries, (backoff / 1000), e);
						Thread.sleep(backoff);
						backoff *= 2;
					} 
					else
					{
						throw new Exception("Failed to register with ZooKeeper, no retries left--giving up", e);
					}
				}
			}
		}

		// Start leader election thread.  The leader assigns work to each instance
		
		LoadBalancer loadBalancer = new LoadBalancer(config);
		final LeaderSelector leaderSelector = new LeaderSelector(curator, "/kaboom/leader", loadBalancer);
		leaderSelector.autoRequeue();
		leaderSelector.start();		
		final List<Worker> workers = new ArrayList<Worker>();
		final List<Thread> threads = new ArrayList<Thread>();
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				shutdown();
				
				for (Worker w : workers)
				{
					w.stop();
				}
				for (Thread t : threads)
				{
					try
					{
						t.join();
					} 
					catch (InterruptedException e)
					{
						LOG.error("Interrupted joining thread.", e);
					}
				}
				
				try
				{
					FileSystem.get(config.getHadoopConfiguration()).close();
				} 
				catch (Throwable t)
				{
					LOG.error("Error closing Hadoop filesystem", t);
				}
				
				try
				{
					curator.delete().forPath("/kaboom/clients/" + config.getKaboomId());
				} 
				catch (Exception e)
				{
					LOG.error("Error deleting /kaboom/clients/{}", config.getKaboomId(), e);
				}
				
				leaderSelector.close();
				curator.close();
			}
		}));

		Pattern topicPartitionPattern = Pattern.compile("^(.*)-(\\d+)$");
		
		while (shutdown == false)
		{
			workers.clear();
			threads.clear();
			
			for (String node : curator.getChildren().forPath("/kaboom/assignments"))
			{
				String assignee = new String(curator.getData().forPath("/kaboom/assignments/" + node), UTF8);
				
				if (assignee.equals(Integer.toString(config.getKaboomId())))
				{
					LOG.info("Running data pull for {}", node);
					Matcher m = topicPartitionPattern.matcher(node);
					
					if (m.matches())
					{
						String topic = m.group(1);
						int partition = Integer.parseInt(m.group(2));						
						String path = config.getTopicToHdfsPath().get(topic);
						
						if (path == null)
						{
							LOG.error("Topic has no configured output path: {}", topic);
							continue;
						}
						
						String proxyUser = config.getTopicToProxyUser().get(topic);
						if (proxyUser == null)
						{
							proxyUser = "";
						}						
						
						Worker worker = new Worker(
							 config.getConsumerConfiguration(), 
							 config.getHadoopConfiguration(), 
							 curator, 
							 topic, 
							 partition, 
							 config.getFileRotateInterval(), 
							 path, 
							 proxyUser, 
							 config.getAllowOffsetOverrides(),
							 config.getSinkToHighWatermark()
						);
						
						workers.add(worker);
						Thread t = new Thread(worker);
						threads.add(t);
						t.start();
						
					} 
					else
					{
						LOG.error("Could not get topic and partition from node name. ({})", node);
					}
				}
			}
			
			if (threads.size() > 0)
			{
				for (Thread t : threads)
				{
					t.join();
				}
			}
			else
			{
				Thread.sleep(10000);
			}
		}
	}
	
	public void shutdown()
	{
		shutdown = true;
	}
}
