/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.blackberry.bdp.kaboom.timestamps;

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
import java.nio.charset.Charset;
import java.util.HashMap;

public class TimestampWorker {

	private static final Logger LOG = LoggerFactory.getLogger(TimestampWorker.class);

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
	private long messagesConsumed = 0;
	private boolean stopping = false;
	String parsedDate;
	String template = "%y-%M-%d-%H";
	long thisOffset;
	byte[] bytes = new byte[1024 * 1024];
	int length;
	private final Charset UTF8 = Charset.forName("UTF-8");
	int pos;
	
	public TimestampWorker(StartupConfig startupConfig, 
		 String topic, 
		 int partition, 
		 long startOffset, 
		 long endOffset) throws Exception {
		this.startupConfig = startupConfig;
		this.topic = topic;
		this.partition = partition;
		this.startOffset = startOffset;
		this.endOffset = endOffset;
		this.messagesConsumed = 0;

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

			byte version;
			PriParser pri = new PriParser();
			VersionParser ver = new VersionParser();
			TimestampParser tsp = new TimestampParser();

			HashMap<String, Long> msgCounters = new HashMap<>();			
			HashMap<String, Long> injectedCounters = new HashMap<>();			
			long tsParseErrors = 0;
			
			while (stopping == false) {
				try {
					timestamp = 0;
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
					
					thisOffset = offset;
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
						count(msgCounters, Converter.timestampTemplateBuilder(timestamp, template));
					} else {
						if (version == (byte) 0x00) {
							LOG.debug("[{}] Failed to parse timestamp.  Using stored timestamp", partitionId);
							timestamp = Converter.longFromBytes(bytes, 2);
							count(injectedCounters, Converter.timestampTemplateBuilder(timestamp, template));
							tsParseErrors++;
						} else {
							LOG.debug("[{}] Error parsing timestamp.", partitionId);														
						}
					}

					if ((length - pos) < 0) {
						LOG.info("[{}] Skipping offset as length - Offset is < 0: timestamp: {}, pos: {}, length: {}", partitionId, timestamp, pos, length);
						continue;
					}

				} catch (Exception e) {
					LOG.error("[{}] Error processing message: ", partitionId, e);
					throw e;
				}
			}
			
			LOG.info("total counts for parsed timestamps within messages");
			for (String parsed : msgCounters.keySet()) {				
				long total = msgCounters.get(parsed);
				LOG.info("parsed from messages: string: {}, total:{}", parsed, total);
			}
			
			LOG.info("total counts for parsed timestamps from what klogger injected");
			for (String parsed : injectedCounters.keySet()) {				
				long total = injectedCounters.get(parsed);
				LOG.info("parsed from klogger's injected timestamp: {}, total:{}", parsed, total);
			}
			LOG.info("there were {} total timestamp parsing errors", tsParseErrors);

			LOG.info("[{}] Timestamp Counter Thingy version of KaBoom client shutting down.", partitionId);
			
			LOG.info("[{}] Worker stopped. (Read {} lines.  Next offset is {})", partitionId, messagesConsumed, offset);
		} catch (BrokerUnavailableException | IOException e) {
			LOG.error("[{}] An exception occured while setting up this worker thread giving up and returing => error : {} ", partitionId, e);
		} 
	}
	
	private void count(HashMap<String, Long> counter, String parsedDate) {
		long val;
		if (counter.containsKey(parsedDate)) {
			val = counter.get(parsedDate);
			val++;
			counter.put(parsedDate, val);
		} else {
			LOG.info("offset {} introduced date {} with message: {}" , thisOffset, parsedDate, new String(bytes, pos, length - pos, UTF8));
			val = 1;
			counter.put(parsedDate, val);
		}		
	}

	public void stop() {
		LOG.info("[{}] Stop request received", partitionId);
		stopping = true;
	}

	public long getMsgWrittenPerSec() {
		return messagesConsumed / ((System.currentTimeMillis() - startTime) / 1000);
	}
}
