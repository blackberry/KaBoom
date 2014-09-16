package com.blackberry.kaboom;

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
