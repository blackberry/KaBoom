package com.blackberry.bdp.simplekaboom;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import com.blackberry.bdp.common.conversion.Converter;
import static com.blackberry.bdp.common.conversion.Converter.getBytes;
import java.util.Arrays;

public class ResetAllOffsets {

	private static CuratorFramework curator;

	public static void main(String[] args) {
		try {
			String zookeeperConnectionString = args[0];
			String topicRoot = args[1];
			if (zookeeperConnectionString == null || topicRoot == null) {
				System.out.println("Usage java " + ResetAllOffsets.class + " <zk conn string> <topic root>");
				System.exit(1);
			}
			createCurator(zookeeperConnectionString);
			for (String topic : curator.getChildren().forPath(topicRoot)) {
				for (String partition : curator.getChildren().forPath(topicRoot + "/" + topic)) {
					String offsetPath = String.format("%s/%s/%s", topicRoot, topic, partition);
					try {
						if (curator.checkExists().forPath(offsetPath) != null) {
							long offset = Converter.longFromBytes(curator.getData().forPath(offsetPath));
							curator.setData().forPath(offsetPath, getBytes(0l));
							System.out.println("Reset" + offsetPath + "=" + offset + "to zero");
						} else {
							System.out.println("Missing offset at " + offsetPath);
						}
					} catch (Throwable t) {
						System.out.printf("error: %s\n", t.getStackTrace().toString());
						t.printStackTrace();
					}
				}
			}
		} catch (Throwable t) {
			System.out.printf("error: %s\n", t.getStackTrace().toString());
			t.printStackTrace();
		}

	}

	private static void createCurator(String zookeeperConnectionString) {
		String[] connStringAndPrefix = zookeeperConnectionString.split("/", 2);

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

		ResetAllOffsets.curator = CuratorFrameworkFactory.builder()
			 .namespace(connStringAndPrefix[1])
			 .connectString(connStringAndPrefix[0]).retryPolicy(retryPolicy)
			 .build();

		ResetAllOffsets.curator.start();
	}

}
