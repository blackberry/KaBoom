package com.blackberry.kaboom;

import java.util.Map;
import com.blackberry.krackle.consumer.ConsumerConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author dariens
 */
public class KaboomConfiguration
{
	private int kaboomId;
	private long fileRotateInterval;
	private int weight;
	private Map<String, String> topicToHdfsPath;
	private Map<String, String> topicToProxyUser;
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
	 * @return the topicToHdfsPath
	 */
	public Map<String, String> getTopicToHdfsPath()
	{
		return topicToHdfsPath;
	}

	/**
	 * @param topicToHdfsPath the topicToHdfsPath to set
	 */
	public void setTopicToHdfsPath(Map<String, String> topicToHdfsPath)
	{
		this.topicToHdfsPath = topicToHdfsPath;
	}

	/**
	 * @return the topicToProxyUser
	 */
	public Map<String, String> getTopicToProxyUser()
	{
		return topicToProxyUser;
	}

	/**
	 * @param topicToProxyUser the topicToProxyUser to set
	 */
	public void setTopicToProxyUser(Map<String, String> topicToProxyUser)
	{
		this.topicToProxyUser = topicToProxyUser;
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
}
