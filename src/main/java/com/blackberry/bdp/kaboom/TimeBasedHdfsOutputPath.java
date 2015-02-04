/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this dirTemplate file, choose Tools | Templates
 * and open the dirTemplate in the editor.
 */
package com.blackberry.bdp.kaboom;

import com.blackberry.bdp.common.utils.conversion.Converter;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.javaapi.PartitionMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dariens
 */
public class TimeBasedHdfsOutputPath
{
	private static final Object fsLock = new Object();
	private FileSystem fileSystem;
	private final FsPermission permissions = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);
	
	private final String dirTemplate;
	private String filename;
	private String proxyUser;
	
	private final Integer durationSeconds;
	//private Long timestampExpires;
	//private Long timestampStarts;
	
	private Configuration hadoopConfiguration;
	private final int bufferSize = 16 * 1024;
	private final short replicas = 3;
	private final long blocksize = 256 * 1024 * 1024;	
	
	private final String tmpPrefix = "_tmp_";

	private OutputFile outputFile;
	private final Map<Long, OutputFile> outputFileMap = new HashMap<>();
	
	private boolean configured = false;
	
	private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
	
	public TimeBasedHdfsOutputPath(FileSystem fileSystem, String pathTemplate, Integer durationSeconds)
	{
		this.fileSystem = fileSystem;
		this.durationSeconds = durationSeconds;
		this.dirTemplate = pathTemplate;
	}
	
	public void configure(String filename)
	{
		this.filename = filename;
		
		this.configured = true;		
	}
	
	public boolean isConfigured()
	{
		return configured;
	}
	
	private static String dateString(Long ts)
	{		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");		
		Date now = new Date();
		String strDate = sdf.format(ts);
		return strDate;		
	}	
	
	private long calculateStartTime(Long ts)
	{
		return ts - ts % (this.durationSeconds * 1000);
	}
	
	private long calculateEndTime(Long ts)
	{
		return calculateStartTime(ts) + durationSeconds * 1000;
	}
	
	public FastBoomWriter getBoomWriter(long timestamp) throws IOException, Exception
	{		
		if (!configured)
		{
			throw new Exception("Cannot call getBoomWriter on non-configured TimeBasedHdfsOutputPath: " + this.toString(), null);
		}
		
		Long startTime = calculateStartTime(timestamp);
				
		outputFile = outputFileMap.get(startTime);
		
		if (outputFile == null)
		{
			Long endTime = calculateEndTime(timestamp);
			outputFile = new OutputFile(startTime, endTime);
			outputFileMap.put(startTime, outputFile);
		}

		return outputFile.getBoomWriter();
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

	
	private class OutputFile
	{
		private String dir;
		private String tmpdir;

		private Path finalPath;
		private Path tmpPath;
		private FastBoomWriter boomWriter;
		private OutputStream out;
		private Long startTime;
		private Long endTime;

		public OutputFile(long startTime, Long endTime)
		{
			this.startTime = startTime;
			this.endTime = endTime;
			dir = Converter.timestampTemplateBuilder(startTime, dirTemplate);
			tmpdir = String.format("%s/%s%s", dir, tmpPrefix, filename);
			finalPath = new Path(dir + "/" + filename);			
			tmpPath = new Path(tmpdir + "/" + filename);
			
			try
			{
				 if (fileSystem.exists(tmpPath))
				 {
					 fileSystem.delete(tmpPath, false);
					 LOG.info("Removing file from HDFS because it already exists: {}", tmpPath);
				 }

				 out = fileSystem.create(tmpPath, permissions, false, bufferSize, replicas, blocksize, null);
				 boomWriter = new FastBoomWriter(out);						 
			} 
			catch (Exception e)
			{
				LOG.error("Error creating file.", e);
			}
		}

		public void abort()
		{
			LOG.info("Aborting output file: {}", tmpPath);
			
			try
			{
				boomWriter.close();
			} 
			catch (IOException e)
			{
				LOG.error(" Error closing boom writer: {}", tmpPath, e);
			}
			
			try
			{
				out.close();
			} 
			catch (IOException e)
			{
				LOG.error("Error closing boom writer output file: {}", tmpPath, e);
			}
			
			synchronized (fsLock)
			{
				try
				{
					fileSystem.delete(new Path(tmpdir), true);
					LOG.info("Deleted temp file: {}", tmpPath);
				} 
				catch (IOException e)
				{
					LOG.error("Error deleting temp files: {}", tmpPath, e);
				}
			}
		}

		public void close()
		{
			LOG.info("Closing {}", tmpPath);
			
			try
			{
				boomWriter.close();
				out.close();			
			}
			catch (IOException ioe)
			{
				LOG.error("Error closing up boomWriter {}:", tmpPath, ioe);
			}				 

			try
			{
				LOG.info("Moving {} to {}", tmpPath, finalPath);
				fileSystem.rename(tmpPath, finalPath);
			} 
			catch (Exception e)
			{
				LOG.error("Error moving {} to {}", tmpPath, finalPath, e);
				abort();
			}

			try
			{
				fileSystem.delete(new Path(tmpdir), true);
			} 
			catch (IllegalArgumentException | IOException e)
			{
				LOG.error("Error deleting file {}", tmpPath, e);
			}
		}
		
		public Long getStartTime()
		{
			return startTime;
		}

		public Long getEndTime()
		{
			return endTime;
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
			 + "\tpathTemplate: %s%n"
			 //+ "\tdurationExpiresTs: %s (%s)%n"
			 //+ "\tdurationStartsTs: %s (%s)%n"
			 + "\timestamp: %s%n", 
			 getClass().getName(), 
			 this.durationSeconds, 
			 this.dirTemplate);
			 //this.timestampExpires, dateString(this.timestampExpires),
			 //this.timestampStarts, dateString(this.timestampStarts));
	}
	
}
