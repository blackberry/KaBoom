/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this dirTemplate file, choose Tools | Templates
 * and open the dirTemplate in the editor.
 */
package com.blackberry.bdp.kaboom;

import com.blackberry.bdp.common.utils.conversion.Converter;
import com.codahale.metrics.Timer;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dariens
 */
public class TimeBasedHdfsOutputPath
{
	private static final Logger LOG = LoggerFactory.getLogger(TimeBasedHdfsOutputPath.class);

	private final KaboomConfiguration config;
	private final String topic;
	private final FileSystem fileSystem;
	private String partitionId = "unknown-partitionId";
	private final String dirTemplate;
	private final Integer durationSeconds;	
	private final Timer topicFlushTimer;
	private final Timer totalFlushTimer;
	private long lastPeriodicClosePollTime  = System.currentTimeMillis();

	private final Map<Long, OutputFile> outputFileMap = new HashMap<>();	
	
	/**
	 * Reusable objects only exist as class attributes because they are needed very frequently
	 * Instead of continually re-instantiating transient objects in the critical message path they 
	 * are created once and long lived	 
	 */
	
	private long reusableRequestedStartTime;
	private OutputFile reusableRequestedOutputFile;
	
	public TimeBasedHdfsOutputPath(FileSystem fileSystem, String topic, KaboomConfiguration kaboomConfig, String pathTemplate, Integer durationSeconds)
	{
		this.fileSystem = fileSystem;
		this.topic = topic;
		this.config = kaboomConfig;
		this.durationSeconds = durationSeconds;
		this.dirTemplate = pathTemplate;
		this.totalFlushTimer = config.getTotalHdfsFlushTimer();		
		this.topicFlushTimer = config.getTopicToHdfsFlushTimer().get(this.topic);
	}
	
	private static String dateString(Long ts)
	{		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");		
		Date now = new Date();
		String strDate = sdf.format(ts);
		return strDate;		
	}	
	
	public FastBoomWriter getBoomWriter(long ts, String filename) throws IOException, Exception
	{		
		periodicCloseExpiredPoll();
		
		reusableRequestedStartTime = ts - ts % (this.durationSeconds * 1000);
		reusableRequestedOutputFile = outputFileMap.get(reusableRequestedStartTime);
		
		if (reusableRequestedOutputFile == null)
		{			
			reusableRequestedOutputFile = new OutputFile(
				 filename, 
				 reusableRequestedStartTime, 
				 System.currentTimeMillis() + durationSeconds * 1000, 
				 config.getUseTempOpenFileDirectory());
			
			outputFileMap.put(reusableRequestedStartTime, reusableRequestedOutputFile);
		}
		
		return reusableRequestedOutputFile.getBoomWriter();
	}
	
	public void abortAll()
	{
		for (Map.Entry<Long, OutputFile> entry : outputFileMap.entrySet())
		{
			entry.getValue().abort();
		}
	}
	
	public void closeAll()
	{
		for (Map.Entry<Long, OutputFile> entry : outputFileMap.entrySet())
		{
			entry.getValue().close();
		}
	}
	
	public void periodicCloseExpiredPoll()
	{
		if (lastPeriodicClosePollTime > System.currentTimeMillis() - config.getPeriodicFileCloseInterval())
		{
			return;
		}
		
		Iterator<Map. Entry<Long,OutputFile>> iter = outputFileMap.entrySet().iterator();
		
		while (iter.hasNext())
		{
			Map.Entry<Long, OutputFile> entry = iter.next();

			if (entry.getValue().closeTime < System.currentTimeMillis() - 30 * 1000)
			{
				entry.getValue().close();
				iter.remove();
				LOG.info("[{}] expired open file has been closed: {}  ({} files still open): {}", partitionId, entry.getValue().openFilePath, outputFileMap.size());
			}
			LOG.trace("[{}] {} does not expire until {}", partitionId, entry.getValue().openFilePath, dateString(entry.getValue().closeTime));
		}
		
		lastPeriodicClosePollTime = System.currentTimeMillis();
	}

	/**
	 * @param partitionId the partitionId to set
	 */
	public void setPartitionId(String partitionId)
	{
		this.partitionId = partitionId;
	}

	private class OutputFile
	{
		private String dir;
		private String openFileDirectory;

		private String filename;
		private Path finalPath;
		private Path openFilePath;
		private FastBoomWriter boomWriter;
		private FSDataOutputStream fsDataOut;
		private Long startTime;
		private Long closeTime;
		private Boolean useTempOpenFileDir;

		public OutputFile(String filename, Long startTime, Long closeTime, Boolean useTempOpenFileDir)
		{
			this.filename = filename;
			this.startTime = startTime;
			this.closeTime = closeTime;
			this.useTempOpenFileDir = useTempOpenFileDir;
			
			dir = Converter.timestampTemplateBuilder(startTime, dirTemplate);			
			finalPath = new Path(dir + "/" + filename);
			
			openFileDirectory = dir;
			openFilePath = finalPath;
			
			if (useTempOpenFileDir)
			{
				openFileDirectory = String.format("%s/%s%s", dir, config.getBoomFileTmpPrefix(), this.filename);
				openFilePath = new Path(openFileDirectory + "/" + filename);
			}
			
			try
			{
				 if (fileSystem.exists(openFilePath))
				 {
					 fileSystem.delete(openFilePath, false);
					 LOG.info("Removing file from HDFS because it already exists: {}", openFilePath);
				 }

				 fsDataOut = fileSystem.create(
					  openFilePath, 
					  config.getBoomFilePerms(), 
					  false, 
					  config.getBoomFileBufferSize(), 
					  config.getBoomFileReplicas(),
					  config.getBoomFileBlocksize(), 
					  null);
				 
				 boomWriter = new FastBoomWriter(fsDataOut, partitionId, topicFlushTimer, totalFlushTimer, config.getTotalCompressionTimer());
				 boomWriter.setPeriodicHdfsFlushInterval(config.getPeriodicHdfsFlushInterval());				 
				 boomWriter.setUseNativeCompression(config.getUseNativeCompression());

			} 
			catch (Exception e)
			{
				LOG.error("Error creating file {}", openFilePath, e);
			}
		}
		
		@Override
		public String toString()
		{
			return String.format("%s:%n"
				 + "\ttmpPath: %s%n"
				 + "\tfinalPath: %s%n"
				 + "\tstarts: %s (%s)%n"
				 + "\tcloses: %s (%s)%n",
				 getClass().getName(), 
				 this.openFilePath, 
				 this.finalPath,
				 this.startTime, dateString(this.startTime),
				 this.closeTime, dateString(this.closeTime));
		}		

		public void abort()
		{
			LOG.info("Aborting output file: {}", openFilePath);
			
			try
			{
				boomWriter.close();
			} 
			catch (IOException e)
			{
				LOG.error(" Error closing boom writer: {}", openFilePath, e);
			}
			
			try
			{
				fsDataOut.close();
			} 
			catch (IOException e)
			{
				LOG.error("Error closing boom writer output file: {}", openFilePath, e);
			}
			
			try
			{
				fileSystem.delete(new Path(openFileDirectory), true);
				LOG.info("Deleted open file: {}", openFilePath);
			} 
			catch (IOException e)
			{
				LOG.error("Error deleting open file: {}", openFilePath, e);
			}
		}

		public void close()
		{
			LOG.info("Closing {}", openFilePath);
			
			try
			{
				boomWriter.close();
				LOG.info("Boom writer closed for {}", openFilePath);
				fsDataOut.close();	
				LOG.info("Output stream closed for {}", openFilePath);
			}
			catch (IOException ioe)
			{
				LOG.error("Error closing up boomWriter {}:", openFilePath, ioe);
			}				 

			if (useTempOpenFileDir)
			{
				try
				{
					LOG.info("Moving {} to {}", openFilePath, finalPath);
					fileSystem.rename(openFilePath, finalPath);								
				} 
				catch (Exception e)
				{
					LOG.error("Error moving {} to {}", openFilePath, finalPath, e);
					abort();
				}

				try
				{
					fileSystem.delete(new Path(openFileDirectory), true);
					LOG.info("Deleted temp open file directory: {}", openFilePath);
				} 
				catch (IllegalArgumentException | IOException e)
				{
					LOG.error("Error deleting temp open file direcrory {}", openFilePath, e);
				}
			}
		}
		
		public Long getStartTime()
		{
			return startTime;
		}

		public FastBoomWriter getBoomWriter()
		{
			return boomWriter;
		}
	}
	
	@Override
	public String toString()
	{
		return String.format("%s:%n"
			 + "\tseconds: %s%n"
			 + "\tpathTemplate: %s%n",
			 getClass().getName(), 
			 this.durationSeconds, 
			 this.dirTemplate);
	}
	
}
