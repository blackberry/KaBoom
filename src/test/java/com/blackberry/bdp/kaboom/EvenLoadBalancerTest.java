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

import com.blackberry.bdp.kaboom.api.KaBoomClient;
import java.nio.charset.Charset;
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

public class EvenLoadBalancerTest {

	private static final Logger LOG = LoggerFactory.getLogger(EvenLoadBalancerTest.class);
	private static CuratorFramework curator;
	private static LocalZkServer zk;
	protected static final Charset UTF8 = Charset.forName("UTF-8");

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
	public void testAssignmentConversion() throws Exception {		
		KaBoomClient client = new KaBoomClient();
		client.setId(1001);
		
		String zkPath = "/assignment";
		curator.create().withMode(CreateMode.PERSISTENT).forPath(zkPath, String.valueOf(client.getId()).getBytes(UTF8));
		
		String assignmentFound = new String(curator.getData().forPath(zkPath), UTF8);
		int assignedClientId = new Integer(assignmentFound);
		Assert.assertEquals(assignedClientId, client.getId());
		LOG.info("Testing assignment conversion and {} equals {}", assignedClientId, client.getId());
	}

}
