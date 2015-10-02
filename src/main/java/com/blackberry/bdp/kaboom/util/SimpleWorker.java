/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.blackberry.bdp.kaboom.util;

import com.blackberry.bdp.kaboom.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.common.conversion.Converter;
import com.blackberry.bdp.common.jmx.MetricRegistrySingleton;
import com.blackberry.bdp.krackle.consumer.BrokerUnavailableException;
import com.blackberry.bdp.krackle.consumer.Consumer;
import java.io.IOException;

public class SimpleWorker {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleWorker.class);

	private final String partitionId;
	private Consumer consumer;
	private long offset;
	private long timestamp;
	private final StartupConfig startupConfig;
	private final String topic;
	private final int partition;
	private final long startOffset;
	private final long endOffset;
	private String hostname;
	private long startTime;
	private long messagesWritten = 0;
	private boolean stopping = false;
	private final FastBoomWriter boomWriter;	

	public SimpleWorker(StartupConfig startupConfig, 
		 String topic, 
		 int partition, 
		 long startOffset, 
		 long endOffset,
		 FastBoomWriter boomWriter) throws Exception {
		this.startupConfig = startupConfig;
		this.topic = topic;
		this.partition = partition;
		this.startOffset = startOffset;
		this.endOffset = endOffset;
		this.messagesWritten = 0;
		this.boomWriter = boomWriter;

		offset = startOffset;
		partitionId = topic + "-" + partition;

		LOG.info("[{}] Created simple worker.", partitionId);
		//this.run();
	}

	public void run() throws Exception {
		try {
			try {
				hostname = InetAddress.getLocalHost().getCanonicalHostName();
			} catch (UnknownHostException e) {
				LOG.error("[{}] Can't determine local hostname", partitionId);
				hostname = "unknown.host";
			}

			String clientId = "kaboom-" + hostname;

			consumer = new Consumer(startupConfig.getConsumerConfiguration(), clientId, topic, partition, startOffset, MetricRegistrySingleton.getInstance().getMetricsRegistry());

			LOG.info("[{}] Created simple worker.  Starting at offset {}.", partitionId, startOffset);

			byte[] bytes = new byte[1024 * 1024];
			int length;
			byte version;
			int pos;
			PriParser pri = new PriParser();
			VersionParser ver = new VersionParser();
			TimestampParser tsp = new TimestampParser();

			while (stopping == false) {
				try {
					if (offset > endOffset) {
						LOG.info("[{}] offset {} is past end offset of {}, stopping", partitionId, offset, endOffset);
						stop();
						continue;
					}
					if (Thread.interrupted()) {
						throw new Exception("This simple worker has been interrupted");
					}

					length = consumer.getMessage(bytes, 0, bytes.length);

					if (length == -1) {
						continue;
					}

					// offset always refers to the next offset we expect to 
					// since we just called consumer.getMessage() let's see
					// if the offset of the last message is what we expected
					// and handle the fun edge cases when it's not
					if (offset != consumer.getLastOffset()) {
						long highWatermark = consumer.getHighWaterMark();

						if (offset > consumer.getLastOffset()) {
							if (offset < highWatermark) {
								/* 
								 * When using SNAPPY compression in Krackle's consumer there will be messages received
								 * that are in the snappy block that are from earlier than our requested offset.  When this
								 * happens the consumer will continue to send us messages from within that block so we
								 * should just be patient until the offsets are from where we want.  
								 */
								continue;
							} else {
								if (offset > highWatermark) {
									throw new Exception(String.format("[%s] offset %d is greater than high watermark %d", partitionId, offset, highWatermark));
								}
							}
						} else {
							String error = String.format("[%s] Offset anomaly! Expected: %d, Got %d, Consumer high watermark %d, latest %d, earliest %d",
								 partitionId,
								 offset,
								 consumer.getLastOffset(),
								 consumer.getHighWaterMark(),
								 consumer.getLatestOffset(),
								 consumer.getEarliestOffset());
							throw new Exception(error);
						}
					}

					offset = consumer.getNextOffset();

					// (byte) 0xFE: -2
					// (byte) 0x00: 0
					// (byte) 0xFF: -1
					// Check for version
					if (bytes[0] == (byte) 0xFE) {
						version = bytes[1];

						if (version == (byte) 0x00) {
							// Version 0 has a timestamp in the front, so we can skip that for now. Come back if we need it.
							pos = 10;
						} else {
							LOG.warn("[{}] Unrecognized encoding version: {}", partitionId, version);
							pos = 0;
						}
					} else {
						// version -1 is a raw log
						version = (byte) 0xFF;
						pos = 0;
					}

					// Optional PRI at the start of the line.
					if (pri.parsePri(bytes, pos, length)) {
						pos += pri.getPriLength();
					}

					// On the off chance that someone is following RFC5424 and has
					// inserted a version in the log line.
					if (ver.parseVersion(bytes, pos, length - pos)) {
						// Skip the length of the version and the following space.
						pos += ver.getVersionLength() + 1;
					}

					tsp.parse(bytes, pos, length - pos);

					if (tsp.getError() == TimestampParser.NO_ERROR) {
						timestamp = tsp.getTimestamp();
						// Move position to the end of the timestamp						
						pos += tsp.getLength();

						// mbruce: occasionally we get a line that is truncated partway through the timestamp,
						// however we still have the rest of the last message in the byte buffer and parsing the 
						// timestamp will push us past then end of the line
						if (pos > length) {
							LOG.error("Error: parsing timestamp has went beyond length of the message");
							continue;
						}
						// If the next char is a space, skip that too.
						if (pos < length && bytes[pos] == ' ') {
							pos++;
						}
					} else {
						if (version == (byte) 0x00) {
							LOG.debug("[{}] Failed to parse timestamp.  Using stored timestamp", partitionId);
							timestamp = Converter.longFromBytes(bytes, 2);
						} else {
							LOG.error("[{}] Error parsing timestamp.", partitionId);
							timestamp = System.currentTimeMillis();
						}
					}

					if ((length - pos) < 0) {
						LOG.info("[{}] Skipping offset as length - Offset is < 0: timestamp: {}, pos: {}, length: {}", partitionId, timestamp, pos, length);
						continue;
					}

					boomWriter.writeLine(timestamp, bytes, pos, length - pos);
					messagesWritten++;

				} catch (Exception e) {
					LOG.error("[{}] Error processing message: ", partitionId, e);
					throw e;
				}
			}

			LOG.info("[{}] Simple KaBoom client shutting down and closing all output files.", partitionId);

			boomWriter.close();
			
			LOG.info("[{}] Worker stopped. (Read {} lines.  Next offset is {})", partitionId, messagesWritten, offset);
		} catch (BrokerUnavailableException | IOException e) {
			LOG.error("[{}] An exception occured while setting up this worker thread giving up and returing => error : {} ", partitionId, e);
		} 
	}

	public void stop() {
		LOG.info("[{}] Stop request received", partitionId);
		stopping = true;
	}

	public long getMsgWrittenPerSec() {
		return messagesWritten / ((System.currentTimeMillis() - startTime) / 1000);
	}
}
