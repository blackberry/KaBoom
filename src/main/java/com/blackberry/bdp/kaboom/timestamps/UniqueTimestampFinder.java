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

import com.blackberry.bdp.kaboom.StartupConfig;
import com.blackberry.bdp.kaboom.api.RunningConfig;
import java.io.IOException;

import java.nio.charset.Charset;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kohsuke.args4j.ExampleMode.ALL;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.IntOptionHandler;
import org.kohsuke.args4j.spi.LongOptionHandler;

public class UniqueTimestampFinder {

	private final Object fsLock = new Object();
	private static final Logger LOG = LoggerFactory.getLogger(UniqueTimestampFinder.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");
	boolean shutdown = false;
	private TimestampWorker worker;
	private StartupConfig startupConfig;
	private RunningConfig runningConfig;

	@Option(name = "-topic", usage = "The topic to consume", metaVar = "<topic name>")
	private String topic;

	@Option(name = "-partition", usage = "The partition to consume", handler = IntOptionHandler.class, metaVar = "<partition number>")
	private Integer partition;

	@Option(name = "-startOffset", usage = "The offset to start consuming from", handler = LongOptionHandler.class, metaVar = "<starting offset>")
	private Long startOffset;

	@Option(name = "-endOffset", usage = "The last offset to consume and end on", handler = LongOptionHandler.class, metaVar = "<ending offset>")
	private Long endOffset;

	public UniqueTimestampFinder() throws Exception {
	}

	public static void main(String[] args) throws Exception, IOException {
		LOG.info("****************************************************");
		LOG.info("***         Unique Timestamp Finder Thingy         ***");
		LOG.info("****************************************************");
		new UniqueTimestampFinder().run(args);
	}

	private void run(String[] args) throws Exception {
		CmdLineParser parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
			if (topic == null
				 || partition == null
				 || startOffset == null
				 || endOffset == null) {
				throw new CmdLineException(parser, "There was a missing required command ling argument");
			}

			LOG.info("Topic: {}", topic);
			LOG.info("Partition: {}", partition);
			LOG.info("Start offset: {}", startOffset);
			LOG.info("End offset: {}", endOffset);

		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			System.err.println();
			System.err.println("  Usage: java <java options> " + this.getClass() + " " + parser.printExample(ALL));
			return;
		}

		LOG.info("Consuming topic {}, starting at {} ending on {} and writing {} as user {}",
			 this.topic,
			 this.startOffset,
			 this.endOffset);

		try {
			startupConfig = new StartupConfig();
			runningConfig = startupConfig.getRunningConfig();
			startupConfig.logConfiguraton();			
		} catch (Exception e) {
			LOG.error("an error occured while building configuration objects: ", e);
			throw e;
		}

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				worker.stop();
				try {
					//FileSystem.get(startupConfig.getHadoopConfiguration()).close();
				} catch (Throwable t) {
					LOG.error("Error closing Hadoop filesystem", t);
				}
				startupConfig.getKaBoomCurator().close();
			}

		}));

		try {
			worker = new TimestampWorker(startupConfig, topic, partition, startOffset, endOffset);
		} catch (Exception e) {
			LOG.error("An error occured setting up our simple worker: ", e);
		}

		try {
			worker.run();
			LOG.info("All finished");
		} catch (Exception e) {
			LOG.error("There was an error while the simple worker was running, deleting all output files");
		}
	}

}
