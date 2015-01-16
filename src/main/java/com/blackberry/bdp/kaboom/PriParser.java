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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriParser {
	private static final Logger LOG = LoggerFactory.getLogger(PriParser.class);

	private IntParser intParser = new IntParser();

	private int priLength;
	private int pri;

	public int getPriLength() {
		return priLength;
	}

	public int getPri() {
		return pri;
	}

	public int getFacility() {
		return pri / 8;
	}

	public int getPriority() {
		return pri % 8;
	}

	private int digit1;

	public boolean parsePri(byte[] bytes, int pos, int length) {
		try {
			if (bytes[pos] == '<') {
				// Looks promising!

				digit1 = bytes[pos + 1];
				if (digit1 == '0') {
					// This is only valid id the whole thing is "<0>"
					if (bytes[pos + 2] == '>') {
						pri = 0;
						priLength = 3;
						return true;

					} else {
						// Invalid
						return false;
					}
				} else if (digit1 >= '2' && digit1 <= '9') {
					// The max value is 191, so this is only valid for 1 or 2 digit
					// results
					if (bytes[pos + 2] == '>') {
						// one digit
						pri = intParser.intFromBytes(bytes, pos + 1, 1);
						priLength = 3;
						return true;
					} else if (bytes[pos + 2] >= '0' && bytes[pos + 2] <= '9'
							&& bytes[pos + 3] == '>') {
						// two digit
						pri = intParser.intFromBytes(bytes, pos + 1, 2);
						priLength = 4;
						return true;
					} else {
						// Invalid
						return false;
					}
				} else if (digit1 == '1') {
					if (bytes[pos + 2] == '>') {
						// one digit
						pri = intParser.intFromBytes(bytes, pos + 1, 1);
						priLength = 3;
						return true;
					} else if (bytes[pos + 2] >= '0' && bytes[pos + 2] <= '9'
							&& bytes[pos + 3] == '>') {
						// two digit
						pri = intParser.intFromBytes(bytes, pos + 1, 2);
						priLength = 4;
						return true;
					} else if (bytes[pos + 2] >= '0' && bytes[pos + 2] <= '8'
							&& bytes[pos + 3] >= '0' && bytes[pos + 3] <= '9'
							&& bytes[pos + 4] >= '>') {
						// Three digits starting with 18
						pri = intParser.intFromBytes(bytes, pos + 1, 3);
						priLength = 5;
						return true;
					} else if (bytes[pos + 2] == '9' && bytes[pos + 3] >= '0'
							&& bytes[pos + 3] <= '1' && bytes[pos + 4] >= '>') {
						// 190 or 191
						pri = intParser.intFromBytes(bytes, pos + 1, 3);
						priLength = 5;
						return true;
					} else {
						// Invalid
						return false;
					}
				}
			}
		} catch (Throwable t) {
			LOG.error("Error parsing PRI.", t);
			return false;
		}

		return false;
	}
}
