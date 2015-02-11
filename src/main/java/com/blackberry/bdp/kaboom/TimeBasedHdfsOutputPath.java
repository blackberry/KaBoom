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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
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
	
	public FastBoomWriter getBoomWriter(long timestamp, String filename, Boolean useTempOpenFileDir) throws IOException, Exception
	{		
		Long startTime = calculateStartTime(timestamp);				
		outputFile = outputFileMap.get(startTime);
		
		if (outputFile == null)
		{			
			outputFile = new OutputFile(filename, startTime, System.currentTimeMillis() + durationSeconds * 1000, useTempOpenFileDir);
			
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

	public void pollPeriodicHdfsFlush(Long maxMsSinceLastFlush) throws IOException
	{
		Iterator<Map. Entry<Long,OutputFile>> iter = outputFileMap.entrySet().iterator();
		
		while (iter.hasNext())
		{
			Map.Entry<Long, OutputFile> entry = iter.next();

			entry.getValue().getBoomWriter().periodicHdfsFlushPoll(maxMsSinceLastFlush);
		}
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
				openFileDirectory = String.format("%s/%s%s", dir, tmpPrefix, filename);
				openFilePath = new Path(openFileDirectory + "/" + filename);
			}
			
			try
			{
				 if (fileSystem.exists(openFilePath))
				 {
					 fileSystem.delete(openFilePath, false);
					 LOG.info("Removing file from HDFS because it already exists: {}", openFilePath);
				 }

				 fsDataOut = fileSystem.create(openFilePath, permissions, false, bufferSize, replicas, blocksize, null);
				 //fsDataOut = new FSDataOutputStream(out, fsDataStats);
				 
				 boomWriter = new FastBoomWriter(fsDataOut);						 
				 LOG.info("Created {}", this);
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
