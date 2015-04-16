/*
 * Copyright 2015 dariens.
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
package com.blackberry.bdp.kaboom.api;

import java.util.ArrayList;
import java.util.List;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class Util {
	/**
	 * 
	 * @param curator
	 * @param zkPath
	 * @return
	 * @throws Exception
	 */
	public static List<String> childrenInZkPath(CuratorFramework curator, 
		 String zkPath) throws Exception {		
		List<String> children = new ArrayList<>();
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);		
		for (String node : curator.getChildren().forPath(zkPath)) {
			children.add(node);
		}		
		return children;
	}	
}
