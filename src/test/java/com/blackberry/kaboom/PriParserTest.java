package com.blackberry.kaboom;

import static org.junit.Assert.*;

import org.junit.Test;

import com.blackberry.kaboom.PriParser;

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
