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

public class VersionParserTest {
	@Test
	public void testSkipPri() {
		VersionParser ver = new VersionParser();

		byte[] msg;

		// Test all good values
		for (int i = 1; i <= 999; i++) {
			msg = (i + " This is a test.").getBytes();
			assertTrue(ver.parseVersion(msg, 0, msg.length));
			assertEquals(("" + i).length(), ver.getVersionLength());

			msg = ("<123>" + i + " This is a test.").getBytes();
			assertTrue(ver.parseVersion(msg, 5, msg.length));
			assertEquals(("" + i).length(), ver.getVersionLength());
		}

		// Various bad values
		assertFalse(ver.parseVersion("12This is a test".getBytes(), 0, 15));
		assertFalse(ver.parseVersion("1234 This is a test".getBytes(), 0, 15));

		assertFalse(ver.parseVersion("01 This is a test".getBytes(), 0, 15));
		assertFalse(ver.parseVersion("0 This is a test".getBytes(), 0, 15));
	}
}
