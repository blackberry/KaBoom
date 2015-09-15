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

import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Test;

public class TimestampParserTest {

	@Test
	public void testNonZeroOffet() throws UnsupportedEncodingException {
		String testString = "12345678902014-06-02T17:56:12.219+0000 this is a test";
		TimestampParser tsp = new TimestampParser();
		tsp.parse(testString.getBytes("UTF-8"), 10, testString.length() - 10);
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1401731772219L, tsp.getTimestamp());
	}

	@Test
	public void testParser() {
		// we need the current year for some of this
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		c.setTimeInMillis(System.currentTimeMillis());
		int currentYear = c.get(Calendar.YEAR);

		TimestampParser tsp = new TimestampParser();
		String timestamp;

		// Start off easy
		timestamp = "2014-05-07T17:05:08.123 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399482308123L, tsp.getTimestamp());
		assertEquals(23, tsp.getLength());

		timestamp = "2014-05-07T17:05:08.123000 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399482308123L, tsp.getTimestamp());
		assertEquals(26, tsp.getLength());

		timestamp = "2014-05-07T17:05:08.12 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399482308120L, tsp.getTimestamp());
		assertEquals(22, tsp.getLength());

		timestamp = "2014-05-07T17:05:08.1 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399482308100L, tsp.getTimestamp());
		assertEquals(21, tsp.getLength());

		timestamp = "2014-05-07T17:05:08. This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399482308000L, tsp.getTimestamp());
		assertEquals(20, tsp.getLength());

		timestamp = "2014-05-07T17:05:08 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399482308000L, tsp.getTimestamp());
		assertEquals(19, tsp.getLength());

		timestamp = "May 07 17:05:08.123 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		cal.setTimeInMillis(1399482308123L);
		cal.set(Calendar.YEAR, currentYear);
		assertEquals(cal.getTimeInMillis(), tsp.getTimestamp());
		assertEquals(19, tsp.getLength());

		//IPGBD-3830 Test Whitespace padded days
		timestamp = "May  7 17:05:08.123 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		Calendar cal2 = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		cal2.setTimeInMillis(1399482308123L);
		cal2.set(Calendar.YEAR, currentYear);
		assertEquals(cal2.getTimeInMillis(), tsp.getTimestamp());
		assertEquals(19, tsp.getLength());
		
		timestamp = "<1>1 2014-05-07T17:05:08 This is a test";
		tsp.parse(timestamp.getBytes(), 5, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399482308000L, tsp.getTimestamp());
		assertEquals(19, tsp.getLength());

		timestamp = "2014-05-07T17:05:08Z This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399482308000L, tsp.getTimestamp());
		assertEquals(20, tsp.getLength());

		timestamp = "2014-05-07T17:05:08+01 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399478708000L, tsp.getTimestamp());
		assertEquals(22, tsp.getLength());

		timestamp = "2014-05-07T17:05:08+0130 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399476908000L, tsp.getTimestamp());
		assertEquals(24, tsp.getLength());

		timestamp = "2014-05-07T17:05:08+01:30 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399476908000L, tsp.getTimestamp());
		assertEquals(25, tsp.getLength());

		timestamp = "2014-05-07T17:05:08-01:30 This is a test";
		tsp.parse(timestamp.getBytes(), 0, timestamp.length());
		assertEquals(TimestampParser.NO_ERROR, tsp.getError());
		assertEquals(1399487708000L, tsp.getTimestamp());
		assertEquals(25, tsp.getLength());
	}
}
