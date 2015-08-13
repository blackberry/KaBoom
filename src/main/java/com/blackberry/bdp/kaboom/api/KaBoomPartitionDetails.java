/*
 * Copyright 2015 BlackBerry, Limited.
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

public class KaBoomPartitionDetails {

	private final int partitionId;
	private final long offset;
	private final long offset_timestamp;
	private final int assignedKaBoomClientId;

	public KaBoomPartitionDetails(int partitionId,
		 long offset,
		 long offset_timestamp,
		 int assignedKaBoomClientId) {
		this.partitionId = partitionId;
		this.offset = offset;
		this.offset_timestamp = offset_timestamp;
		this.assignedKaBoomClientId = assignedKaBoomClientId;
	}

	public int getPartitionId() {
		return this.partitionId;
	}

	public long getOffset() {
		return this.offset;
	}

	public long getOffsetTimestamp() {
		return this.offset_timestamp;
	}

	public int getAssignedKaBoomClientId() {
		return this.assignedKaBoomClientId;
	}

}
