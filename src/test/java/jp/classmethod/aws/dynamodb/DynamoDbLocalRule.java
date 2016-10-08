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
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import lombok.Getter;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * JUnit {@link TestRule} to start DynamoDBLocal before test.
 *
 * @author Daisuke Miyamoto
 * @since #version#
 */
public class DynamoDbLocalRule extends ExternalResource {

	@Getter
	private AmazonDynamoDB amazonDynamoDB;

	static {
		// This one should be copied during test-compile time. If project's basedir does not contains a folder
		// named './build/libs' please try '$ ./mvn copy-dependencies' from command line first
		System.setProperty("sqlite4java.library.path", "./target/dependencies");
	}


	@Override
	public void before() throws Throwable {
		amazonDynamoDB = DynamoDBEmbedded.create().amazonDynamoDB();
	}

	@Override
	public void after() {
		amazonDynamoDB.shutdown();
	}
}
