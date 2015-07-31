/*
 * Copyright 2015 dariens.
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

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicFilter {
	private static final Logger LOG = LoggerFactory.getLogger(TopicFilter.class);
	public enum FilterType {STRING_MATCH, REGEX};
	
	@Getter @Setter public int number = 0;
	@Getter @Setter public String name = "<FILTER NAME>";
	@Getter @Setter public FilterType type = FilterType.STRING_MATCH;
	@Getter @Setter public boolean filterIntentionIsToMatch = true;
	@Getter @Setter public String filter = "<TEXT TO MATCH>";
	@Getter @Setter public long duration = 3600;
	@Getter @Setter public String directory = "<SUBDIR>";	
	
	public TopicFilter() { }
	
}
