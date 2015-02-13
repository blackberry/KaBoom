/**
 * Copyright 2014 BlackBerry, Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blackberry.bdp.kaboom;

import com.blackberry.bdp.krackle.MetricRegistrySingleton;
import com.codahale.metrics.Timer;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastBoomWriterTest {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory
			.getLogger(FastBoomWriterTest.class);
	private static final Charset UTF8 = Charset.forName("UTF8");
	private static final Random rand = new Random();
	private final Timer timer = MetricRegistrySingleton.getInstance().getMetricsRegistry().timer("boom writes");

	@Test
	public void testWriteFile() throws IOException {		
		FileSystem.Statistics fsDataStats = null;
		FileOutputStream out = new FileOutputStream("/tmp/test2.bm");		
		FSDataOutputStream fsDataOut = new FSDataOutputStream(out, fsDataStats);
		FastBoomWriter writer = new FastBoomWriter(fsDataOut, "unknown-partitionId1", timer, timer);


		byte[] message = "This is a test.  Let's make the line a bit longer by writing some stuff here."
				.getBytes(UTF8);

		writer.writeLine(1397268894000L, message, 0, message.length);

		writer.close();
	}

	@Test
	public void testWriteBigFile() throws IOException {
		FileSystem.Statistics fsDataStats = null;
		FileOutputStream out = new FileOutputStream("/tmp/test2.bm");		
		FSDataOutputStream fsDataOut = new FSDataOutputStream(out, fsDataStats);
		FastBoomWriter writer = new FastBoomWriter(fsDataOut, "unknown-partitionId2", timer, timer);

		byte[] chars = "abc".getBytes(UTF8);
		List<byte[]> messages = new ArrayList<>();
		for (int i = 0; i < 151; i++) {
			StringBuilder sb = new StringBuilder("This is a test. ");
			int extra = rand.nextInt() % 500;
			for (int j = 0; j < extra; j++) {
				sb.append(chars[rand.nextInt(chars.length)]);
			}
			messages.add(sb.toString().getBytes(UTF8));
		}

		byte[] message;
		for (int i = 0; i < 100000; i++) {
			message = messages.get(i % messages.size());
			writer.writeLine(System.currentTimeMillis(), message, 0, message.length);
		}

		writer.close();
	}
}
