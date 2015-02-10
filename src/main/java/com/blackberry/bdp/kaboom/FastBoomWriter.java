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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.zip.Deflater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastBoomWriter
{
	private static final Logger LOG = LoggerFactory.getLogger(FastBoomWriter.class);
	private static final Charset UTF8 = Charset.forName("UTF8");
	private Long lastAvroBlockWriteTimestamp = System.currentTimeMillis();	

	private static final byte[] MAGIC_NUMBER = new byte[]
	{
		'O', 'b', 'j', 1		 
	};
	
	/**
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
						{ "name": "ms",      "type": "long" },
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

	private final OutputStream out;

	public FastBoomWriter(OutputStream out) throws IOException
	{
		this.out = out;

		Random rand = new Random();
		syncMarker = new byte[16];
		rand.nextBytes(syncMarker);

		writeHeader();
	}
	
	private Long msSinceLastAvroBlockWrite() 
	{		
		return System.currentTimeMillis() - lastAvroBlockWriteTimestamp;
	}
	
	public void unwrittenAvroBlockLifetimePoll(Long maxUnwrittenAvroBlockLifetimeMs) throws IOException
	{
		if (maxUnwrittenAvroBlockLifetimeMs == null || lastAvroBlockWriteTimestamp == null)
		{
			return;
		}
		
		if (msSinceLastAvroBlockWrite() >= maxUnwrittenAvroBlockLifetimeMs)
		{
			Boolean logBlockBufferWritten = false;
			
			if (logBlockBuffer.position() > 0)
			{
				LOG.info("Log block write forced since It's been {} ms since last avro block was written and the log block buffer position is {}", 
					 msSinceLastAvroBlockWrite(), logBlockBuffer.position());

				writeLogBlock();
				logBlockBufferWritten = true;
			}
			else
			{
				LOG.info("Skipping forced log block write since log block buffer position is", logBlockBuffer.position());					 
			}
				
			// Need to check time since last avro write again as writing the log block could call the avro block write
				
			if (msSinceLastAvroBlockWrite() >= maxUnwrittenAvroBlockLifetimeMs)				
			{
				// Check the position to be doubly sure?

				if (avroBlockBuffer.position() > 0)
				{
					LOG.info("Avro block write force since It's been {} ms since last avro block was written and the avro block buffer position is {}",
						 msSinceLastAvroBlockWrite(), avroBlockBuffer.position());

					writeAvroBlock();
				}
				else
				{
					LOG.info("Skipping forced avro block write since avro block buffer position is", avroBlockBuffer.position());					 
				}
			}
			else
			{
				if (logBlockBufferWritten == true)
				{
					LOG.info("A log block write was forced and likely incured a call to write the avro block as it's only been {} ms since last avro block write", msSinceLastAvroBlockWrite());
				}
			}
		}
		else
		{
			LOG.info("Last written avro block was {} ms  ago", msSinceLastAvroBlockWrite());
		}
	}

	private void writeHeader() throws IOException
	{
		out.write(MAGIC_NUMBER);

		// 2 entries in the metadata
		encodeLong(2L);
		out.write(longBytes, 0, longBuffer.position());

		// Write schema
		writeBytes("avro.schema".getBytes(UTF8));
		writeBytes(SCHEMA_BYTES);

		// Write codec
		writeBytes("avro.codec".getBytes(UTF8));
		writeBytes("deflate".getBytes(UTF8));

		// End the map
		encodeLong(0L);
		out.write(longBytes, 0, longBuffer.position());

		out.write(syncMarker);
	}

	private void writeBytes(byte[] bytes) throws IOException
	{
		encodeLong(bytes.length);
		out.write(longBytes, 0, longBuffer.position());
		out.write(bytes);
	}

	private byte[] longBytes = new byte[10];
	private ByteBuffer longBuffer = ByteBuffer.wrap(longBytes);

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

	private long ms;
	private long second;

	private long blockNumber = 0L;

	private long logBlockSecond = 0L;
	private final byte[] logBlockBytes = new byte[1024 * 1024];
	private final ByteBuffer logBlockBuffer = ByteBuffer.wrap(logBlockBytes);

	private long logLineCount;
	private final byte[] logLinesBytes = new byte[logBlockBytes.length - 41];
	private final ByteBuffer logLinesBuffer = ByteBuffer.wrap(logLinesBytes);

	public void writeLine(long timestamp, byte[] message, int offset, int length)
		 throws IOException
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
				LOG.debug("New Block ({} lines) (second changed from {} to {})", logLineCount, logBlockSecond, second);
			} 
			else
			{
				if (logLinesBytes.length - logLinesBuffer.position() < 10 + 10 + length)
				{
					LOG.debug("New Block. ({} lines) (buffer full)", logLineCount);
				} 
				else
				{
					LOG.debug("New Block. ({} lines)", logLineCount);
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
			encodeLong(ms);
			logLinesBuffer.put(longBytes, 0, longBuffer.position());

			encodeLong(length);
			logLinesBuffer.put(longBytes, 0, longBuffer.position());
			logLinesBuffer.put(message, offset, length);
		} 
		catch (Exception e)
		{
			LOG.info("Exception! Buffer:{}, Length:{}", logLinesBuffer, length, e);
			LOG.info("???.  {} - {} < 10 + 10 + {}", logBlockBytes.length,
				 logBlockBuffer.position(), length);
		}
		logLineCount++;
	}

	private long avroBlockRecordCount = 0L;
	private final byte[] avroBlockBytes = new byte[2 * 1024 * 1024];
	private final ByteBuffer avroBlockBuffer = ByteBuffer.wrap(avroBlockBytes);

	private void writeLogBlock() throws IOException
	{
		// We need room for the logBlockBuffer, the number of records in
		// logLinesBuffer (up to 10) and the logLinesBuffer. If not, then we need to flush.
		
		if (avroBlockBytes.length - avroBlockBuffer.position() < logBlockBuffer
			 .position() + 10 + logLinesBuffer.position())
		{
			writeAvroBlock();
		}

		avroBlockBuffer.put(logBlockBytes, 0, logBlockBuffer.position());
		encodeLong(logLineCount);
		avroBlockBuffer.put(longBytes, 0, longBuffer.position());
		avroBlockBuffer.put(logLinesBytes, 0, logLinesBuffer.position());
		encodeLong(0L);
		avroBlockBuffer.put(longBytes, 0, longBuffer.position());

		avroBlockRecordCount++;

		logBlockBuffer.clear();
		logLineCount = 0L;
		logLinesBuffer.clear();
	}

	private int compressedSize;
	private byte[] compressedBlockBytes = new byte[256 * 1024];
	private final Deflater deflater = new Deflater(6, true);

	private void writeAvroBlock() throws IOException
	{
		LOG.debug("Writing Avro Block ({} bytes)", avroBlockBuffer.position());
		encodeLong(avroBlockRecordCount);
		out.write(longBytes, 0, longBuffer.position());

		while (true)
		{
			deflater.reset();
			deflater.setInput(avroBlockBytes, 0, avroBlockBuffer.position());
			deflater.finish();
			
			compressedSize = deflater.deflate(compressedBlockBytes, 0, compressedBlockBytes.length);
			
			if (compressedSize == compressedBlockBytes.length)
			{
				// it probably didn't actually compress all of it. Expand and retry
				LOG.debug("Expanding compression buffer {} -> {}", compressedBlockBytes.length, compressedBlockBytes.length * 2);
				compressedBlockBytes = new byte[compressedBlockBytes.length * 2];
			}
			else
			{
				LOG.debug("Compressed {} bytes to {} bytes ({}% reduction)",
					 avroBlockBuffer.position(), compressedSize, Math.round(100 - (100.0 * compressedSize / avroBlockBuffer.position())));
				break;
			}
		}

		encodeLong(compressedSize);
		
		out.write(longBytes, 0, longBuffer.position());
		out.write(compressedBlockBytes, 0, compressedSize);
		out.write(syncMarker);

		avroBlockBuffer.clear();
		avroBlockRecordCount = 0L;
		
		lastAvroBlockWriteTimestamp = System.currentTimeMillis();
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
		out.close();		
	}
}
