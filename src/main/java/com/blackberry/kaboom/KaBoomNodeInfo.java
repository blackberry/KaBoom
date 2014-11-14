/**
 * Copyright 2014 BlackBerry, Inc.
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

package com.blackberry.kaboom;

public class KaBoomNodeInfo {
	private String hostname;
	private int weight;

	private int load = 0;
	private double targetLoad = 0.0;

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public int getLoad() {
		return load;
	}

	public void setLoad(int load) {
		this.load = load;
	}

	public double getTargetLoad() {
		return targetLoad;
	}

	public void setTargetLoad(double targetLoad) {
		this.targetLoad = targetLoad;
	}

	@Override
	public String toString() {
		return "KaBoomNodeInfo [hostname=" + hostname + ", weight=" + weight + "]";
	}

}
