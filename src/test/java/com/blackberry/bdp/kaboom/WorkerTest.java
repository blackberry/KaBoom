/*
 * Copyright 2015 BlackBerry, Limited.
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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blackberry.bdp.common.conversion.Converter;

public class WorkerTest {

	private static final Logger LOG = LoggerFactory.getLogger(WorkerTest.class);
	private static CuratorFramework curator;
	private static LocalZkServer zk;

	private static CuratorFramework buildCuratorFramework() {
		String connectionString = "localhost:21818";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		LOG.info("attempting to connect to ZK with connection string {}", "localhost:21818");
		CuratorFramework newCurator = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
		newCurator.start();
		return newCurator;
	}

	@BeforeClass
	public static void setup() throws Exception {
		zk = new LocalZkServer();
		curator = buildCuratorFramework();
	}

	@AfterClass
	public static void cleanup() throws Exception {
		curator.close();
		zk.shutdown();
	}

	@Test
	public void testStoringOffsetZero() throws Exception {
		String zkPath = "/zero";
		long value1 = 0;
		curator.create().withMode(CreateMode.PERSISTENT).forPath(zkPath, Converter.getBytes(value1));
		long value2 = Converter.longFromBytes(curator.getData().forPath(zkPath));
		Assert.assertEquals(value1, value2);
		LOG.info("Testing conversion of zero {} equals {}", value1, value2);
	}

	@Test(expected = NullPointerException.class)
	public void testStoringOffsetNull() throws Exception {
		String zkPath = "/zero";
		Long value1 = null;
		curator.create().withMode(CreateMode.PERSISTENT).forPath(zkPath, Converter.getBytes(value1));
		Long value2 = Converter.longFromBytes(curator.getData().forPath(zkPath));
		Assert.assertEquals(value1, value2);
		LOG.info("Testing conversion of null {} equals {}", value1, value2);
	}

	public void testStoringOffsetUnitialized() throws Exception {
		String zkPath = "/zero";
		long value1 = 1;
		curator.create().withMode(CreateMode.PERSISTENT).forPath(zkPath, Converter.getBytes(value1));
		long value2 = Converter.longFromBytes(curator.getData().forPath(zkPath));
		Assert.assertEquals(value1, value2);
		LOG.info("Testing conversion of null {} equals {}", value1, value2);
	}

}
