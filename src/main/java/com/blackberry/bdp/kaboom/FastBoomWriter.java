/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.blackberry.bdp.kaboom;

import com.blackberry.bdp.common.jmx.MetricRegistrySingleton;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.zip.Deflater;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastBoomWriter
{
	private static final Logger LOG = LoggerFactory.getLogger(FastBoomWriter.class);
	private static final Charset UTF8 = Charset.forName("UTF8");
	private long lastHdfsFlushTimestamp = System.currentTimeMillis();	
	private long numAvroBlocksWritten = 0l;
	private long numHdfsFlushedAVroBlocks = 0l;
	private String topic = null;
	private KaboomConfiguration config = null;
	private final String partitionId;
	private Long periodicHdfsFlushInterval = null;	
	private final Timer hdfsFlushTimerTopic;
	private final Timer hdfsFlushTimerTotal;
	private final Timer hdfsFlushTimer;
	private final Timer compressionTimerTotal;
	private final Timer compressionTimerTopic;
	private boolean useNativeCompression = false;
	private final Histogram compressionRatioHistogramTopic;
	private final Histogram compressionRatioHistogramTotal;
	
	private int compressedSize;
	private byte[] compressedBlockBytes = new byte[256 * 1024];
	private final Deflater deflater;
	
	private long avroBlockRecordCount = 0L;
	private final byte[] avroBlockBytes = new byte[2 * 1024 * 1024];
	private final ByteBuffer avroBlockBuffer = ByteBuffer.wrap(avroBlockBytes);

	private long ms;
	private long second;

	private long blockNumber = 0L;

	private long logBlockSecond = 0L;
	private final byte[] logBlockBytes = new byte[1024 * 1024];
	private final ByteBuffer logBlockBuffer = ByteBuffer.wrap(logBlockBytes);

	private long logLineCount;
	private final byte[] logLinesBytes = new byte[logBlockBytes.length - 41];
	private final ByteBuffer logLinesBuffer = ByteBuffer.wrap(logLinesBytes);
	
	private final byte[] longBytes = new byte[10];
	private final ByteBuffer longBuffer = ByteBuffer.wrap(longBytes);
	
	private static final byte[] MAGIC_NUMBER = new byte[]		 
	{
		'O', 'b', 'j', 1		 
	};
	
	public void loadNativeDeflateLib()
	{
		System.loadLibrary("NativeDeflate");
	}

	public native byte[] compress(byte[] bytesIn, int position, int compressionLevel);
	
	/**
	 * Boom Avro Schema:
	 * 
		{
		  "type": "record",
		  "name": "logBlock",
		  "fields": [
			 { "name": "second",      "type": "long" },
			 { "name": "createTime",  "type": "long" },
			 { "name": "blockNumber", "type": "long" },
			 { "name": "logLines", "type": {
				"type": "array",
				  "items": {
					 "type": "record",
					 "name": "messageWithMillis",
					 "fields": [ 
						{ "name": "ms", "type": "long" },
						{ "name": "eventId", "type": "int", "default": 0 },
						{ "name": "message", "type": "string" }
					 ]
				  }
			 }}
		  ]
		}
	*/
	
	private static final String SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"logBlock\","
		 + "\"fields\":["
		 + "{\"name\":\"second\",\"type\":\"long\"},"
		 + "{\"name\":\"createTime\",\"type\":\"long\"},"
		 + "{\"name\":\"blockNumber\",\"type\":\"long\"},"
		 + "{\"name\":\"logLines\",\"type\":"
		 + "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"messageWithMillis\",\"fields\":["
		 + "{\"name\":\"ms\",\"type\":\"long\"},"
		 + "{\"name\":\"message\",\"type\":\"string\"}]}}}]}";
	
	private static final byte[] SCHEMA_BYTES = SCHEMA_STRING.getBytes(UTF8);

	private final byte[] syncMarker;

	private final FSDataOutputStream  fsDataOut;

	public FastBoomWriter(FSDataOutputStream out,  String topic, int partition, KaboomConfiguration config) throws IOException
	{
		this.fsDataOut = out;		
		this.topic = topic;		
		this.partitionId = topic + "-" + partition;
		this.config = config;		
		this.hdfsFlushTimerTopic = config.getTopicToHdfsFlushTimer().get(topic);
		this.hdfsFlushTimerTotal = config.getTotalHdfsFlushTimer();		
		this.compressionTimerTotal = config.getTotalCompressionTimer();
		this.compressionTimerTopic = config.getTopicToCompressionTimer().get(topic);
		this.compressionRatioHistogramTopic = config.getTopicToCompressionRatioHistogram().get(topic);		
		this.compressionRatioHistogramTotal = config.getTotalCompressionRatioHistogram();
		this.hdfsFlushTimer = MetricRegistrySingleton.getInstance().getMetricsRegistry().timer("kaboom:partitions:" + this.partitionId + ":flush timer");		
		this.deflater = new Deflater(config.getTopicToCompressionLevel().get(topic), true);		
		
		LOG.info("[{}] FastBoomWriter instantiated with compression level {}", partitionId, config.getTopicToCompressionLevel().get(topic));
		
		Random rand = new Random();
		syncMarker = new byte[16];
		rand.nextBytes(syncMarker);

		writeHeader();
	}
	
	private Long msSinceLastHdfsFlush() 
	{		
		return System.currentTimeMillis() - lastHdfsFlushTimestamp;
	}
	
	public void periodicHdfsFlushPoll() throws IOException
	{
		if (getPeriodicHdfsFlushInterval() == null 
			 || getPeriodicHdfsFlushInterval() == 0
			 || msSinceLastHdfsFlush() < getPeriodicHdfsFlushInterval())
		{
			return;
		}		
				
		final Timer.Context timerContextTopic = hdfsFlushTimerTopic.time();
		final Timer.Context timerContext = hdfsFlushTimer.time();
		final Timer.Context timerContextTotal = hdfsFlushTimerTotal.time();
		
		try
		{
			Boolean logBlockBufferWritten = false;

			if (logBlockBuffer.position() > 0)
			{
				LOG.trace("Log block write forced during periodic HDFS flush since buffer position is {}", logBlockBuffer.position());
				writeLogBlock();				
				logBlockBufferWritten = true;
			}
			else
			{
				LOG.trace("Skipping forced log block write in periodic HDFS flush since log block buffer position is {}", logBlockBuffer.position());					 
			}

			// Need to check time since last avro write again as writing the log block could call the avro block write

			if (avroBlockBuffer.position() > 0)
			{
				LOG.trace("Avro block write forced during periodic HDFS flush since buffer position is {}", avroBlockBuffer.position());
				writeAvroBlock();
			}
			else
			{
				if (logBlockBufferWritten == true)
				{
					LOG.trace("A log block write was forced and likely incured a call to write the avro block because the avro block buffer position is now {}", avroBlockBuffer.position());
				}

				LOG.trace("Skipping forced avro block write since avro block buffer position is {}", avroBlockBuffer.position());					 
			}

			if (numAvroBlocksWritten == 0 || numHdfsFlushedAVroBlocks == numAvroBlocksWritten)
			{
				LOG.trace("Skipping forced HDFS flush as there haven't been any new avro blocks written");
				return;
			}		

			fsDataOut.hflush();			
			LOG.trace("Flushed putput file for {}", partitionId);
			numHdfsFlushedAVroBlocks = numAvroBlocksWritten;
			lastHdfsFlushTimestamp = System.currentTimeMillis();
		}
		finally
		{
			timerContext.stop();
			timerContextTopic.stop();
			timerContextTotal.stop();
		}
	}

	private void writeHeader() throws IOException
	{
		fsDataOut.write(MAGIC_NUMBER);

		// 2 entries in the metadata
		encodeLong(2L);
		fsDataOut.write(longBytes, 0, longBuffer.position());

		// Write schema
		writeBytes("avro.schema".getBytes(UTF8));
		writeBytes(SCHEMA_BYTES);

		// Write codec
		writeBytes("avro.codec".getBytes(UTF8));
		writeBytes("deflate".getBytes(UTF8));

		// End the map
		encodeLong(0L);
		fsDataOut.write(longBytes, 0, longBuffer.position());

		fsDataOut.write(syncMarker);
	}

	private void writeBytes(byte[] bytes) throws IOException
	{
		encodeLong(bytes.length);
		fsDataOut.write(longBytes, 0, longBuffer.position());
		fsDataOut.write(bytes);
	}

	private void encodeLong(long n)
	{
		longBuffer.clear();
		n = (n << 1) ^ (n >> 63);
		if ((n & ~0x7FL) != 0)
		{
			longBuffer.put((byte) ((n | 0x80) & 0xFF));
			n >>>= 7;
			if (n > 0x7F)
			{
				longBuffer.put((byte) ((n | 0x80) & 0xFF));
				n >>>= 7;
				if (n > 0x7F)
				{
					longBuffer.put((byte) ((n | 0x80) & 0xFF));
					n >>>= 7;
					if (n > 0x7F)
					{
						longBuffer.put((byte) ((n | 0x80) & 0xFF));
						n >>>= 7;
						if (n > 0x7F)
						{
							longBuffer.put((byte) ((n | 0x80) & 0xFF));
							n >>>= 7;
							if (n > 0x7F)
							{
								longBuffer.put((byte) ((n | 0x80) & 0xFF));
								n >>>= 7;
								if (n > 0x7F)
								{
									longBuffer.put((byte) ((n | 0x80) & 0xFF));
									n >>>= 7;
									if (n > 0x7F)
									{
										longBuffer.put((byte) ((n | 0x80) & 0xFF));
										n >>>= 7;
										if (n > 0x7F)
										{
											longBuffer.put((byte) ((n | 0x80) & 0xFF));
											n >>>= 7;
										}
									}
								}
							}
						}
					}
				}
			}
		}
		longBuffer.put((byte) n);
	}

	public void writeLine(long timestamp, byte[] message, int offset, int length) throws IOException
	{
		ms = timestamp % 1000l;
		second = timestamp / 1000l;

		// If the buffer is too full to hold it, or the second has changed, then
		// write out the block first.
		if ((logBlockBuffer.position() > 0 && second != logBlockSecond)
			 || logLinesBytes.length - logLinesBuffer.position() < 10 + 10 + length)
		{
			if (logBlockBuffer.position() > 0 && second != logBlockSecond)
			{
				LOG.debug("[{}] New Block ({} lines) (second changed from {} to {})", partitionId, logLineCount, logBlockSecond, second);
			} 
			else
			{
				if (logLinesBytes.length - logLinesBuffer.position() < 10 + 10 + length)
				{
					LOG.trace("[{}] New Block. ({} lines) (buffer full)", partitionId, logLineCount);
				} 
				else
				{
					LOG.trace("[{}] New Block. ({} lines)", partitionId, logLineCount);
				}
			}

			writeLogBlock();
		}

		if (logBlockBuffer.position() == 0)
		{
			logBlockBuffer.clear();
			logLinesBuffer.clear();
			logLineCount = 0;
			logBlockSecond = second;

			// second
			encodeLong(second);
			logBlockBuffer.put(longBytes, 0, longBuffer.position());

			// createTime
			encodeLong(System.currentTimeMillis());
			logBlockBuffer.put(longBytes, 0, longBuffer.position());

			// block number
			encodeLong(blockNumber++);
			logBlockBuffer.put(longBytes, 0, longBuffer.position());
		}

		/*
		 * aryder: added try-catch back in to catch errors
		 */
		try
		{
			LOG.debug("[{}] logBlockBuffer: CurPosition {}, InsertMessage-Offset {}, InsertMessage Length {}", partitionId, logBlockBuffer.position(), offset, length);

			
			encodeLong(ms);
			logLinesBuffer.put(longBytes, 0, longBuffer.position());

			encodeLong(length);
			logLinesBuffer.put(longBytes, 0, longBuffer.position());
			logLinesBuffer.put(message, offset, length);
		} 
		catch (Exception e)
		{
			LOG.info("[{}] Exception! Buffer:{}, Length:{}", partitionId, logLinesBuffer, length, e);
			
			LOG.info("[{}] ???.  {} - {} < 10 + 10 + {}", partitionId, logBlockBytes.length,
				 logBlockBuffer.position(), length);
		}
		logLineCount++;
		periodicHdfsFlushPoll();
	}

	

	private void writeLogBlock() throws IOException
	{
		// We need room for the logBlockBuffer, the number of records in
		// logLinesBuffer (up to 10) and the logLinesBuffer. If not, then we need to flush.
		
		if (avroBlockBytes.length - avroBlockBuffer.position() < logBlockBuffer
			 .position() + 10 + logLinesBuffer.position())
		{
			writeAvroBlock();
		}
		
		LOG.debug("[{}] avroBlockBuffer adding logBlockBytes: CurPosition {}, insert length {}", partitionId, avroBlockBuffer.position(), logBlockBuffer.position());

		avroBlockBuffer.put(logBlockBytes, 0, logBlockBuffer.position());
		
		encodeLong(logLineCount);
		avroBlockBuffer.put(longBytes, 0, longBuffer.position());
		
		LOG.debug("[{}] avroBlockBuffer adding logLineBytes: CurPosition {}, insert length {}", partitionId, avroBlockBuffer.position(), logLinesBuffer.position());
		
		avroBlockBuffer.put(logLinesBytes, 0, logLinesBuffer.position());
		
		encodeLong(0L);		
		avroBlockBuffer.put(longBytes, 0, longBuffer.position());

		avroBlockRecordCount++;

		logBlockBuffer.clear();
		logLineCount = 0L;
		logLinesBuffer.clear();
	}
	
	private void writeAvroBlock() throws IOException
	{
		final Timer.Context timerTotal = compressionTimerTotal.time();
		final Timer.Context timerTopic = compressionTimerTopic.time();
		
		try
		{
			LOG.debug("[{}] Writing Avro Block ({} bytes)", partitionId, avroBlockBuffer.position());
			encodeLong(avroBlockRecordCount);
			fsDataOut.write(longBytes, 0, longBuffer.position());
			
			long start = System.currentTimeMillis();			
				 
			if (useNativeCompression)
			{
				compressedBlockBytes = new byte[256 * 1024];
				compressedBlockBytes = compress(avroBlockBytes, avroBlockBuffer.position(), config.getTopicToCompressionLevel().get(topic));
				compressedSize = compressedBlockBytes.length;	
								
				LOG.debug("[{}] Natively compressed {} bytes to {} bytes ({}% reduction), compression level {}", 
					 partitionId, 
					 avroBlockBuffer.position(), 
					 compressedSize, 
					 Math.round(100 - (100.0 * compressedSize / avroBlockBuffer.position())),
					 config.getTopicToCompressionLevel().get(topic));
			}
			else
			{
				while (true)
				{
					deflater.reset();
					deflater.setInput(avroBlockBytes, 0, avroBlockBuffer.position());
					deflater.finish();

					compressedSize = deflater.deflate(compressedBlockBytes, 0, compressedBlockBytes.length);

					if (compressedSize == compressedBlockBytes.length)
					{
						// it probably didn't actually compress all of it. Expand and retry
						LOG.trace("[{}] Expanding compression buffer {} -> {}", partitionId, compressedBlockBytes.length, compressedBlockBytes.length * 2);
						compressedBlockBytes = new byte[compressedBlockBytes.length * 2];
					}
					else
					{						
						break;
					}
				}
			}
			
			LOG.debug("[{}] Compressed {} bytes to {} bytes ({}% reduction) in {} ms (native={}, compression level={})", 
				 partitionId,
				 avroBlockBuffer.position(), 
				 compressedSize, 
				 Math.round(100 - (100.0 * compressedSize / avroBlockBuffer.position())),
				 System.currentTimeMillis() - start,
				 useNativeCompression,
				 config.getTopicToCompressionLevel().get(topic));			
			
			compressionRatioHistogramTopic.update(Math.round(100 - (100.0 * compressedSize / avroBlockBuffer.position())));
			compressionRatioHistogramTotal.update(Math.round(100 - (100.0 * compressedSize / avroBlockBuffer.position())));
			
			encodeLong(compressedSize);

			fsDataOut.write(longBytes, 0, longBuffer.position());
			fsDataOut.write(compressedBlockBytes, 0, compressedSize);
			fsDataOut.write(syncMarker);						
		}
		catch (Exception e)
		{
			LOG.error("[{}] error occured either compressing or writing the avro block: ", partitionId, e);			
		}
		finally
		{
			timerTopic.stop();
			timerTotal.stop();
			
			avroBlockBuffer.clear();
			avroBlockRecordCount = 0L;
			numAvroBlocksWritten++;
		}		
	}

	public void close() throws IOException
	{
		if (logBlockBuffer.position() > 0)
		{
			writeLogBlock();
		}
		
		if (avroBlockBuffer.position() > 0)
		{
			writeAvroBlock();
		}
		fsDataOut.close();		
	}

	/**
	 * @return the periodicHdfsFlushInterval
	 */
	public Long getPeriodicHdfsFlushInterval()
	{
		return periodicHdfsFlushInterval;
	}

	/**
	 * @param periodicHdfsFlushInterval the periodicHdfsFlushInterval to set
	 */
	public void setPeriodicHdfsFlushInterval(Long periodicHdfsFlushInterval)
	{
		this.periodicHdfsFlushInterval = periodicHdfsFlushInterval;
	}

	/**
	 * @return the useNativeCompression
	 */
	public boolean isUseNativeCompression()
	{
		return useNativeCompression;
	}

	/**
	 * @param useNativeCompression the useNativeCompression to set
	 */
	public void setUseNativeCompression(boolean useNativeCompression)
	{
		this.useNativeCompression = useNativeCompression;
	}

}