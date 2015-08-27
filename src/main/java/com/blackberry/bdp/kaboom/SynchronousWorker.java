/*
 * Copyright 2015 BlackBerry Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.blackberry.bdp.kaboom;

import com.blackberry.bdp.kaboom.exception.LockNotAcquiredException;
import com.blackberry.bdp.kaboom.exception.NotAssignedException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SynchronousWorker {

	private static final Logger LOG = LoggerFactory.getLogger(SynchronousWorker.class);

	protected boolean paused = false;
	protected final CuratorFramework curator;

	private final String zkAssignmentPath;	
	private final byte[] assigneeBytes;	
	private final String workerName;
	private final static String lockRoot = "/_LOCKS_";
	private final long waitTimeMs;

	protected abstract void stop();
	protected abstract void abort();

	protected SynchronousWorker(CuratorFramework curator,		 
		 String workerName,
		 byte[] assigneeBytes,
		 String zkAssignmentPath,
		 long waitTimeMs) throws Exception {

		this.curator = curator;
		this.workerName = workerName;
		this.assigneeBytes = assigneeBytes;
		this.zkAssignmentPath = zkAssignmentPath;		
		this.waitTimeMs = waitTimeMs;

		if (!isAssigned())
			throw new NotAssignedException(
				 String.format("%s cannot setup worker when not assigned to %s",
					  workerName, zkAssignmentPath));

		//this.lock = new InterProcessMutex(curator, zkPathToLock());
		
	}

	public void aquireAssignment(InterProcessMutex lock) throws Exception {
		if (!isAssigned())
			throw new NotAssignedException(
				 String.format("%s will not attempt to aquire lock when not assigned to %s",
					  workerName, zkAssignmentPath));

		LOG.info("Worker {} trying to obtain lock on {} (waiting up to {} ms)...",
			 workerName, zkAssignmentPath, waitTimeMs);

		if (lock.acquire(waitTimeMs, TimeUnit.MILLISECONDS)) {
			watchAssignment();
			watchConnection();
			LOG.info("{} now holds the lock on {}",
				 workerName, zkAssignmentPath);
			logInfo();
		} else {
			throw new LockNotAcquiredException(String.format(
				 "%s failed to obtain lock on %s after waiting %d ms",
				 workerName, zkAssignmentPath, waitTimeMs));
		}	
	}
	
	private void logInfo() throws Exception {
		LOG.info("My session ID is: {}", curator.getZookeeperClient().getZooKeeper().getSessionId());
		Stat stat = curator.checkExists().forPath(zkPathToLock());
		LOG.info("The ephemeral node owner of the lock is: {}", stat.getEphemeralOwner());
	}
	
	protected void releaseAssignment(InterProcessMutex lock) throws Exception {
		logInfo();
		lock.release();
	}

	private boolean isAssigned() throws Exception {
		return Arrays.equals(curator.getData().forPath(zkAssignmentPath), assigneeBytes);
	}

	private void watchConnection() {
		curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				if (newState == ConnectionState.SUSPENDED) {
					paused = true;
					LOG.warn("Worker {} paused during suspended ZK connection", workerName);
				} else {
					if (newState == ConnectionState.RECONNECTED) {
						try {
							if (isAssigned()) {
								paused = false;
								LOG.info("Worker {} unpaused after ZK reconnected", workerName);
							} else {
								LOG.warn("Worker {} no longer assigned {} after ZK reconnected", workerName, zkAssignmentPath);
								stop();	
							}
						} catch (Exception ex) {
							LOG.error("Worker {} cannot determine if still assigned {} after connection reconnected, stopping",
								 workerName,
								 zkAssignmentPath, 
								 ex);
							stop();
						}
					} else {
						if (newState == ConnectionState.LOST) {
							LOG.error("Worker {} lost connection to ZK, aborting assignment", workerName);
							abort();
						}
					}
				}
			}
		});
	}

	private void watchAssignment() throws Exception {
		NodeCache nodeCache = new NodeCache(curator, zkAssignmentPath);
		nodeCache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				if (!isAssigned()) {
					LOG.info("{} is no longer assigned to {}", workerName, zkAssignmentPath);
					stop();
				}
				LOG.info("We have a node changed!");
				logInfo();				
			}
		});
		nodeCache.start();
	}

	public final String zkPathToLock() {
		//return zkAssignmentPath;
		return String.format("%s%s", lockRoot, zkAssignmentPath);
	}

}
