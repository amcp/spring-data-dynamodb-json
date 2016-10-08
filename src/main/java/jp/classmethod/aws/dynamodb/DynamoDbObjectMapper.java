/*
 * Copyright 2016 Classmethod, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package jp.classmethod.aws.dynamodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.xebia.jacksonlombok.JacksonLombokAnnotationIntrospector;

/**
 * {@link ObjectMapper} configured for the DynamoDbRepository
 *
 * <ul>
 *   <li>Support Java 8Language specs (Optional, etc.)</li>
 *   <li>Support lombok</li>
 * </ul>
 *
 * @author Daisuke Miyamoto
 * @since #version#
 */
@SuppressWarnings("serial")
public class DynamoDbObjectMapper extends ObjectMapper {
	/**
	 * Create instance.
	 *
	 * @since #version#
	 */
	public DynamoDbObjectMapper() {
		registerModule(new Jdk8Module());
		setAnnotationIntrospector(new JacksonLombokAnnotationIntrospector());
	}
}
