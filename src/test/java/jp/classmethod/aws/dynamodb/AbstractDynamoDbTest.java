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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.shared.mapper.DynamoDBObjectMapper;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.fasterxml.jackson.annotation.JsonInclude;
import jp.classmethod.aws.infrastructure.BookDynamoDbRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

/**
 * abstract base class for tests of the spring and spar-wings BaseRepository interfaces
 *
 * @author Alexander Patrikalakis
 * @since #version#
 */
public abstract class AbstractDynamoDbTest {
	@ClassRule
	public static DynamoDbLocalRule dynamoDBLocalRule = new DynamoDbLocalRule();

	BookDynamoDbRepository sut;

	@Before
	public void setup() throws Exception {
		AmazonDynamoDB ddb = dynamoDBLocalRule.getAmazonDynamoDB();

		DynamoDBObjectMapper objectMapper = new DynamoDBObjectMapper();
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		sut = new BookDynamoDbRepository(new ProvisionedThroughput(1L, 1L), ddb, objectMapper);
		sut.open();
	}

	@After
	public void cleanup() {
		sut.deleteAll();
	}
}
