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

import static org.junit.Assert.*;

import org.junit.Test;

import com.blackberry.bdp.kaboom.PriParser;

public class PriParserTest {
	@Test
	public void testSkipPri() {
		PriParser pri = new PriParser();

		// Test all good values
		for (int i = 0; i <= 191; i++) {
			byte[] msg = ("<" + i + ">This is a test.").getBytes();
			assertTrue(pri.parsePri(msg, 0, msg.length));
			assertEquals(("" + i).length() + 2, pri.getPriLength());
		}

		// Various bad values
		assertFalse(pri.parsePri("This is a test".getBytes(), 0, 15));
		assertFalse(pri.parsePri("<>This is a test".getBytes(), 0, 15));

		assertFalse(pri.parsePri("<192>This is a test".getBytes(), 0, 15));
		assertFalse(pri.parsePri("<200>This is a test".getBytes(), 0, 15));
		assertFalse(pri.parsePri("<01>This is a test".getBytes(), 0, 15));

		assertFalse(pri.parsePri("<0".getBytes(), 0, 2));
		assertFalse(pri.parsePri("<1".getBytes(), 0, 2));
		assertFalse(pri.parsePri("<2".getBytes(), 0, 2));
		assertFalse(pri.parsePri("<10".getBytes(), 0, 3));
		assertFalse(pri.parsePri("<20".getBytes(), 0, 3));
		assertFalse(pri.parsePri("<100".getBytes(), 0, 4));
		assertFalse(pri.parsePri("<191".getBytes(), 0, 4));
	}
}
