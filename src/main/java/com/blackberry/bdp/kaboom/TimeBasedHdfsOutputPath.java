/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this dirTemplate file, choose Tools | Templates
 * and open the dirTemplate in the editor.
 */
package com.blackberry.bdp.kaboom;

import com.blackberry.bdp.common.utils.conversion.Converter;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
	private final FileSystem fileSystem;
	private final FsPermission permissions = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);	
	private final String dirTemplate;
	private final Integer durationSeconds;	
	private final int bufferSize = 16 * 1024;
	private final short replicas = 3;
	private final long blocksize = 256 * 1024 * 1024;		
	private final String tmpPrefix = "_tmp_";
	private OutputFile outputFile;
	private final Map<Long, OutputFile> outputFileMap = new HashMap<>();	
	
	private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
	
	public TimeBasedHdfsOutputPath(FileSystem fileSystem, String pathTemplate, Integer durationSeconds)
	{
		this.fileSystem = fileSystem;
		this.durationSeconds = durationSeconds;
		this.dirTemplate = pathTemplate;
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
	
	public FastBoomWriter getBoomWriter(long timestamp, String filename) throws IOException, Exception
	{		
		Long startTime = calculateStartTime(timestamp);
				
		outputFile = outputFileMap.get(startTime);
		
		if (outputFile == null)
		{			
			outputFile = new OutputFile(filename, startTime, System.currentTimeMillis() + durationSeconds * 1000);
			
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
	
	public void closeExpired()
	{
		Iterator<Map. Entry<Long,OutputFile>> iter = outputFileMap.entrySet().iterator();
		
		while (iter.hasNext())
		{
			Map.Entry<Long, OutputFile> entry = iter.next();

			if (entry.getValue().closeTime < System.currentTimeMillis() - 60 * 1000)
			{
				entry.getValue().close();
				iter.remove();
				LOG.info("Expired output file closed and removed from mapping ({} files still open): {}", outputFileMap.size(), entry.getValue());
			}
		}
	}
	
	private class OutputFile
	{
		private String dir;
		private String tmpdir;

		private String filename;
		private Path finalPath;
		private Path tmpPath;
		private FastBoomWriter boomWriter;
		private OutputStream out;
		private Long startTime;
		private Long closeTime;

		public OutputFile(String filename, Long startTime, Long closeTime)
		{
			this.filename = filename;
			this.startTime = startTime;
			this.closeTime = closeTime;
			
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
				 LOG.info("Created {}", this);
			} 
			catch (Exception e)
			{
				LOG.error("Error creating file {}", tmpPath, e);
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
				 this.tmpPath, 
				 this.finalPath,
				 this.startTime, dateString(this.startTime),
				 this.closeTime, dateString(this.closeTime));
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

		public void close()
		{
			LOG.info("Closing {}", tmpPath);
			
			try
			{
				boomWriter.close();
				LOG.info("Boom writer closed for {}", tmpPath);
				out.close();	
				LOG.info("Output stream closed for {}", tmpPath);
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
				LOG.info("Deleted tmp path: {}", tmpPath);
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
