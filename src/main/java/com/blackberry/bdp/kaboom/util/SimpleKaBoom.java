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

import com.blackberry.bdp.kaboom.Authenticator;
import com.blackberry.bdp.kaboom.FastBoomWriter;
import com.blackberry.bdp.kaboom.StartupConfig;
import com.blackberry.bdp.kaboom.api.RunningConfig;
import java.io.IOException;

import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.fs.Path;

import static org.kohsuke.args4j.ExampleMode.ALL;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.IntOptionHandler;
import org.kohsuke.args4j.spi.LongOptionHandler;

public class SimpleKaBoom {

	private final Object fsLock = new Object();
	private static final Logger LOG = LoggerFactory.getLogger(SimpleKaBoom.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");
	boolean shutdown = false;
	private SimpleWorker worker;
	private StartupConfig startupConfig;
	private RunningConfig runningConfig;
	FileSystem fs;
	HdfsDataOutputStream hdfsDataOut;
	FastBoomWriter boomWriter;

	@Option(name = "-proxyUser", usage = "The user to create the boom file as", metaVar = "<username>")
	private String proxyUser;

	@Option(name = "-topic", usage = "The topic to consume", metaVar = "<topic name>")
	private String topic;

	@Option(name = "-partition", usage = "The partition to consume", handler = IntOptionHandler.class, metaVar = "<partition number>")
	private Integer partition;

	@Option(name = "-startOffset", usage = "The offset to start consuming from", handler = LongOptionHandler.class, metaVar = "<starting offset>")
	private Long startOffset;

	@Option(name = "-endOffset", usage = "The last offset to consume and end on", handler = LongOptionHandler.class, metaVar = "<ending offset>")
	private Long endOffset;

	@Option(name = "-boomFile", usage = "The absolute path to the boom file to write", metaVar = "</path/to/filename.bm>")
	private String boomFile;

	public SimpleKaBoom() throws Exception {
	}

	public static void main(String[] args) throws Exception, IOException {
		LOG.info("**********************************************");
		LOG.info("***         SIMPLE KABOOM CONSUMER         ***");
		LOG.info("**********************************************");
		new SimpleKaBoom().run(args);
	}

	private void run(String[] args) throws Exception {
		CmdLineParser parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
			if (proxyUser == null
				 || topic == null
				 || partition == null
				 || startOffset == null
				 || endOffset == null
				 || boomFile == null) {
				throw new CmdLineException(parser, "There was a missing required command ling argument");
			}

			LOG.info("Proxy user: {}", proxyUser);
			LOG.info("Topic: {}", topic);
			LOG.info("Partition: {}", partition);
			LOG.info("Start offset: {}", startOffset);
			LOG.info("End offset: {}", endOffset);
			LOG.info("Boom file: {}", boomFile);

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
			 this.endOffset,
			 this.boomFile,
			 this.proxyUser);

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
					FileSystem.get(startupConfig.getHadoopConfiguration()).close();
				} catch (Throwable t) {
					LOG.error("Error closing Hadoop filesystem", t);
				}
				startupConfig.getKaBoomCurator().close();
			}

		}));

		try {
			fs = startupConfig.authenticatedFsForProxyUser(proxyUser);
			if (fs == null) {
				LOG.warn("There is no topic configured with the proxy user {} within the KaBoom configuration file", proxyUser);
				LOG.info("Attempting to obtain an authenticated file system manually", proxyUser);
				try {
					LOG.info("Attempting to create file system {} for {}", startupConfig.getHadoopUrlPath(), proxyUser);
					Authenticator.getInstance().runPrivileged(proxyUser, new PrivilegedExceptionAction<Void>() {
						@Override
						public Void run() throws Exception {
							synchronized (fsLock) {
								try {
									fs = startupConfig.getHadoopUrlPath().getFileSystem(startupConfig.getHadoopConfiguration());
									LOG.info("Filesystem object instantiated for {}", proxyUser);
								} catch (Exception e) {
									LOG.error("Error getting file system {} for proxy user {}", startupConfig.getHadoopUrlPath(), e);
								}
							}
							return null;
						}
					});					
				} catch (IOException | InterruptedException e) {
					LOG.error("Error creating file system.", e);
					return;
				}
			}			
			if (fs == null) {
				LOG.error("Cannot proceed without a file system");
				return;
			}			
			if (fs.exists(new Path(boomFile))) {
				fs.delete(new Path(boomFile), true);
				LOG.info("Deleted {} as it already exists", boomFile);
			}
			hdfsDataOut = (HdfsDataOutputStream) fs.create(
				 new Path(boomFile),
				 startupConfig.getBoomFilePerms(),
				 false,
				 runningConfig.getBoomFileBufferSize(),
				 runningConfig.getBoomFileReplicas(),
				 runningConfig.getBoomFileBlocksize(),
				 null);			
			boomWriter = new FastBoomWriter(
				 hdfsDataOut,
				 topic,
				 partition,
				 startupConfig);			
			boomWriter.setPeriodicHdfsFlushInterval(runningConfig.getPeriodicHdfsFlushInterval());
			boomWriter.setUseNativeCompression(runningConfig.getUseNativeCompression());
			worker = new SimpleWorker(startupConfig, topic, partition, startOffset, endOffset, boomWriter);
		} catch (Exception e) {
			LOG.error("An error occured setting up our simple worker: ", e);
		}

		try {
			worker.run();
			hdfsDataOut.close();
			LOG.info("All finished");
		} catch (Exception e) {
			LOG.error("There was an error while the simple worker was running, deleting all output files");
			hdfsDataOut.close();
			fs.delete(new Path(boomFile), true);
			LOG.info("Deleted {}", boomFile);
		}
	}

}
