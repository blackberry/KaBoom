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
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It would have been swell to have the lock created and acquired in the 
 * constructor thus reducing the implementers responsibility to only calling
 * super() but the lock is tied to the thread and the instantiation of the 
 * implemented object is always within the parent thread creating it.
 * Instead we'll have the lock provided during aquireAssignment() and the
 * best we can do is check if we're assigned during instantiation.
 */

public abstract class AsyncAssignee implements Runnable{

	private static final Logger LOG = LoggerFactory.getLogger(AsyncAssignee.class);

	protected boolean paused = false;
	protected final CuratorFramework curator;

	private final String zkAssignmentPath;	
	private final byte[] assigneeBytes;	
	private final String workerName;
	private final static String lockRoot = "/_LOCKS_";
	private final long waitTimeMs;
	private InterProcessMutex lock;

	protected abstract void stop();
	protected abstract void abort();
	
	private NodeCache assignmentNodeCache;
	private ConnectionStateListener connectionListener;

	protected AsyncAssignee(CuratorFramework curator,		 
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
	}

	public void aquireAssignment() throws Exception {
		this.lock = new InterProcessMutex(curator, zkPathToLock());
		if (!isAssigned())
			throw new NotAssignedException(
				 String.format("%s will not attempt to aquire lock when not assigned to %s",
					  workerName, zkAssignmentPath));

		LOG.debug("Worker {} trying to obtain lock on {} (waiting up to {} ms)...",
			 workerName, zkAssignmentPath, waitTimeMs);

		if (lock.acquire(waitTimeMs, TimeUnit.MILLISECONDS)) {
			watchAssignment();
			watchConnection();
			LOG.info("{} now holds the lock on {}",
				 workerName, zkAssignmentPath);
		} else {
			throw new LockNotAcquiredException(String.format(
				 "%s failed to obtain lock on %s after waiting %d ms",
				 workerName, zkAssignmentPath, waitTimeMs));
		}	
	}
	
	protected void releaseAssignment() {
		try {
			if (assignmentNodeCache != null) {
				assignmentNodeCache.close();
				LOG.info("assignment node cache closed");
			} else {
				LOG.warn("assignment node cache listener is null and cannot be closed");
			}
		} catch (Exception e) {
			LOG.error("Failed to close off assignment node cache: ", e);
		}
		
		try {
			if (connectionListener != null) {
				curator.getConnectionStateListenable().removeListener(connectionListener);
				LOG.info("removed the connection state listener on the curator connection");
			} else {
				LOG.warn("connection state listener is null and cannot be removed");
			}
		} catch (Exception e) {
			LOG.error("Failed to remove the connction state listener on the curator connection: ", e);
		}
		
		try {
			lock.release();		
			LOG.info("released lock on {}", zkPathToLock());
		} catch (IllegalMonitorStateException  imse) {
			LOG.error("Failed to release lock on {} for reason: ", zkPathToLock(), imse.getMessage());
		} catch (Exception e) {
			LOG.error("unknown error trying to relase the lock {}: ", zkPathToLock(), e);
		}
	}

	private boolean isAssigned() throws Exception {
		try {
			return Arrays.equals(curator.getData().forPath(zkAssignmentPath), assigneeBytes);
		} catch (NoNodeException nne) {
			return false;
		}		
	}

	private void watchConnection() {		
		connectionListener = new ConnectionStateListener() {
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
		};
		curator.getConnectionStateListenable().addListener(connectionListener);
	}

	private void watchAssignment() throws Exception {
		assignmentNodeCache = new NodeCache(curator, zkAssignmentPath);
		assignmentNodeCache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				if (!isAssigned()) {
					LOG.info("{} is no longer assigned to {}", workerName, zkAssignmentPath);
					stop();
				}
			}
		});
		assignmentNodeCache.start();
	}

	public final String zkPathToLock() {
		return String.format("%s%s", lockRoot, zkAssignmentPath);
	}

}