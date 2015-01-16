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

public class VersionParser {
	private static final Logger LOG = LoggerFactory
			.getLogger(VersionParser.class);

	private IntParser intParser = new IntParser();

	private int version;
	private int versionLength;

	public boolean parseVersion(byte[] bytes, int pos, int length) {
		// Look for a number, up to 3 digits, followed by a space. If it's there,
		// return true.
		try {
			if (bytes[pos] >= '1' && bytes[pos] <= '9') {
				if (bytes[pos + 1] == ' ') {
					version = intParser.intFromBytes(bytes, pos, 1);
					versionLength = 1;
					return true;
				}

				if (bytes[pos + 1] >= '0' && bytes[pos + 1] <= '9') {
					if (bytes[pos + 2] == ' ') {
						version = intParser.intFromBytes(bytes, pos, 2);
						versionLength = 2;
						return true;
					}

					if (bytes[pos + 2] >= '0' && bytes[pos + 2] <= '9'
							&& bytes[pos + 3] == ' ') {
						version = intParser.intFromBytes(bytes, pos, 3);
						versionLength = 3;
						return true;
					}
				}
			}
		} catch (Throwable t) {
			LOG.error("Error parsing version.", t);
			return false;
		}
		return false;
	}

	public int getVersion() {
		return version;
	}

	public int getVersionLength() {
		return versionLength;
	}

}
