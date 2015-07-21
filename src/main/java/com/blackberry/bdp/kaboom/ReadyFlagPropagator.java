/**
 * Copyright 2013 BlackBerry, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.blackberry.bdp.kaboom;

import com.blackberry.bdp.common.jmx.MetricRegistrySingleton;
import com.blackberry.bdp.kaboom.api.KaBoomTopicConfig;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import java.io.IOException;

import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.Deque;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadyFlagPropagator implements Runnable {
	private  final Logger LOG = LoggerFactory.getLogger(ReadyFlagPropagator.class);
	private final String READY_MARKER = "_READY";
	private String DATA_DIRECTORY;
	private String ARCHIVE_DIRECTORY;	
	private final KaBoomTopicConfig topicConfig;
	private final Path rootPath;
	private final StartupConfig config;
	private final Timer propgatorTimer;
	private final Meter propagatorPathsChecked;
	private final Meter flagsWritten;
	
	public ReadyFlagPropagator(KaBoomTopicConfig topicConfig, StartupConfig config) throws Exception {
		this.topicConfig = topicConfig;
		this.config = config;
		this.propgatorTimer = MetricRegistrySingleton.getInstance().
			 getMetricsRegistry().timer("kaboom:topic:" + this.topicConfig.getId() + ":flag propagator timer");
		this.propagatorPathsChecked = MetricRegistrySingleton.getInstance().
			 getMetricsRegistry().meter("kaboom:topic:" + this.topicConfig.getId() + ":flag propagator paths checked");
		this.flagsWritten = MetricRegistrySingleton.getInstance().
			 getMetricsRegistry().meter("kaboom:topic:" + this.topicConfig.getId() + ":flags written");
		this.rootPath = ReadyFlagController.getRootFromPathTemplate(this.topicConfig.getHdfsRootDir());
	}

	private void writeReadyWhenSubsAreReady(FileSystem fs, 
		 Path currentPath, 
		 FileStatus[] children) throws IOException {
		if (fs.exists(new Path(currentPath, READY_MARKER)) == false) {
			boolean ready = true;
			for (FileStatus childStatus : children) {
				if (childStatus.isDirectory() 
					 && (fs.exists(new Path(childStatus.getPath(), READY_MARKER)) == false)) {
					ready = false;
					LOG.debug("Can't write {} in {} because sub-directory {} doesn't have a {}",
						 READY_MARKER,
						 currentPath.toUri().getPath(),
						 childStatus.getPath().toUri().getPath(),
						 READY_MARKER);
					break;
				}
			}
			if (ready) {
				if (config.getRunningConfig().isPropagateReadyFlags()) {
					fs.createNewFile(new Path(currentPath, READY_MARKER));
					flagsWritten.mark();
					LOG.info("Wrote {} in {} since all sub directories contain {}", 
						 READY_MARKER, 
						 currentPath.toUri().getPath(),
						 READY_MARKER);
				} else {
					LOG.info("{} should exist in {} since all sub directories contain {}", 
						 READY_MARKER, 
						 currentPath.toUri().getPath(),
						 READY_MARKER);
				}
			}
		}
	}
	
	private void traverseRootPath() throws InterruptedException {
		
		Pattern datePathPattern = Pattern.compile(rootPath
			 + "/" + "(\\d{8})");

		Pattern hourPathPattern = Pattern.compile(rootPath
			 + "/" + "(\\d{8})"
			 + "/" + "(\\d{2})");

		Pattern dataPathPattern = Pattern.compile(rootPath
			 + "/" + "(\\d{8})"
			 + "/" + "(\\d{2})"
			 + "/" + "([^/]+)"
			 + "/" + Pattern.quote(DATA_DIRECTORY));

		Pattern archivePathPattern = Pattern.compile(rootPath
			 + "/" + "(\\d{8})"
			 + "/" + "(\\d{2})"
			 + "/" + "([^/]+)"
			 + "/" + Pattern.quote(ARCHIVE_DIRECTORY));

		Pattern workingPathPattern = Pattern.compile(rootPath
			 + "/" + "(\\d{8})"
			 + "/" + "(\\d{2})"
			 + "/" + "([^/]+)"
			 + "/" + "working"
			 + "/" + "([^/]+)_(\\d+)");
		
		Deque<Path> paths = new ArrayDeque<>();		
		paths.push(rootPath);
		Calendar start = Calendar.getInstance();
		start.setTimeInMillis(System.currentTimeMillis());
		String proxyUser = topicConfig.getProxyUser();
		FileSystem fs = config.authenticatedFsForProxyUser(proxyUser);

		try {
			toNextPath: while (paths.size() > 0) {
				Path currentPath = paths.pop();
				FileStatus[] children = fs.listStatus(currentPath);
				boolean addChildren = true;
				
				LOG.trace("Checking {}", currentPath.toUri().getPath());
				
				// The path is /<root>/<dc>/<service>/<logdir>/<date>
				Matcher dateMatcher = datePathPattern.matcher(currentPath.toString());
				if (dateMatcher.matches()) {
					LOG.trace("Checking date directory: {}", currentPath);					
					if (fs.exists(new Path(currentPath + "/" + READY_MARKER))) {
						LOG.debug("[{}] Skipping {} because {} exists", 
							 topicConfig.getId(), currentPath.toUri().getPath(), READY_MARKER);
						continue toNextPath;
					}					
					String matchedDate = dateMatcher.group(1);
					Calendar dirCal = Calendar.getInstance();
					dirCal.setTimeInMillis(0L);
					dirCal.set(Calendar.YEAR, Integer.parseInt(matchedDate.substring(0, 4)));
					dirCal.set(Calendar.MONTH, Integer.parseInt(matchedDate.substring(4, 6)) - 1);
					dirCal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(matchedDate.substring(6, 8)));
					dirCal.add(Calendar.DAY_OF_MONTH, 1);

					if (start.after(dirCal)) {
						writeReadyWhenSubsAreReady(fs, currentPath, children);
					}
				}

				// /<root>/<dc>/<service>/<logdir>/<YYYYMMDD>/<HH>
				Matcher hourMatcher = hourPathPattern.matcher(currentPath.toString());
				if (hourMatcher.matches()) {
					LOG.trace("Checking hour directory: {}", currentPath);		
					if (fs.exists(new Path(currentPath + "/" + READY_MARKER))) {
						LOG.debug("[{}] Skipping {} because {} exists", 
							 topicConfig.getId(), currentPath.toUri().getPath(), READY_MARKER);
						continue toNextPath;
					}
					String matchedDate = hourMatcher.group(1);
					String matchedHour = hourMatcher.group(2);
					Calendar dirCal = Calendar.getInstance();
					dirCal.setTimeInMillis(0L);
					dirCal.set(Calendar.YEAR, Integer.parseInt(matchedDate.substring(0, 4)));
					dirCal.set(Calendar.MONTH, Integer.parseInt(matchedDate.substring(4, 6)) - 1);
					dirCal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(matchedDate.substring(6, 8)));
					dirCal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(matchedHour));
					dirCal.add(Calendar.HOUR_OF_DAY, 1);

					if (start.after(dirCal)) {
						writeReadyWhenSubsAreReady(fs, currentPath, children);
					}
				}

				// /<root>/<dc>/<service>/<logdir>/<YYYYMMDD>/<HH>/<topic>/data
				if (dataPathPattern.matcher(currentPath.toString()).matches()) {
					addChildren = false;
				}

				// /<root>/<dc>/<service>/<logdir>/<YYYYMMDD>/<HH>/<topic>/archive
				if (archivePathPattern.matcher(currentPath.toString()).matches()) {
					addChildren = false;
				}

				// /<root>/<dc>/<service>/<logdir>/<YYYYMMDD>/<HH>/<topic>/working/blah
				if (workingPathPattern.matcher(currentPath.toString()).matches()) {
					addChildren = false;
				}

				if (addChildren) {
					for (int i = children.length - 1; i >= 0; i--) {
						FileStatus child = children[i];
						if (child.isDirectory()) {
							paths.push(child.getPath());
						}
					}
				}
				
				propagatorPathsChecked.mark();
				if (config.getRunningConfig().getPropateReadyFlagDelayBetweenPathsMs() > 0) {
					Thread.sleep(config.getRunningConfig().getPropateReadyFlagDelayBetweenPathsMs());
				}
				
			}			
		} catch (IOException | NumberFormatException e) {
			LOG.error("There was an error traversing the root path {} for ready flag propagation", rootPath, e);
		}
	}

	@Override
	public void run() {
		final Timer.Context timerContext = propgatorTimer.time();		
		try {			
			LOG.info("[{}] Starting to propagate flags", topicConfig.getId());
			DATA_DIRECTORY = "data";
			ARCHIVE_DIRECTORY = "archive";
			traverseRootPath();
			LOG.info("[{}] Finished propagating flags", topicConfig.getId());
		} catch (InterruptedException ie) {
			LOG.error("The propagator thread was interrupted: ", ie);
		} finally {
			timerContext.stop();
		}
		
	}
}
