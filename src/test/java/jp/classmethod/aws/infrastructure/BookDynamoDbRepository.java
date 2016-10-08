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
package jp.classmethod.aws.infrastructure;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import jp.classmethod.aws.dynamodb.DynamoDbRepository;
import jp.classmethod.aws.model.Book;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A repository of Book domain models to use for testing the library
 *
 * @author Alexander Patrikalakis
 * @since #version#
 */
public class BookDynamoDbRepository extends DynamoDbRepository<Book, String> {
	/** cart table suffix */
	public static final String TABLE_NAME = "mst_book";

	private static final Map<String, ScalarAttributeType> ATTRIBUTE_TYPE_MAP = new HashMap<>();
	static {
		ATTRIBUTE_TYPE_MAP.put(Book.BOOK_ID, ScalarAttributeType.S);
	}

	/**
	 * Create instance.
	 *
	 * @param amazonDynamoDB the dynamodb client
	 * @param objectMapper the object mapper to use
	 * @since #version#
	 */
	@Autowired
	public BookDynamoDbRepository(ProvisionedThroughput throughput, AmazonDynamoDB amazonDynamoDB,
								  ObjectMapper objectMapper) {
		super(null /*prefix*/, TABLE_NAME, amazonDynamoDB, ImmutableMap.of(TABLE_NAME, throughput),
				objectMapper, Book.class, ATTRIBUTE_TYPE_MAP, Collections.singletonList(Book.BOOK_ID),
				null /*gsi list*/);
	}

	@Override
	public String getId(Book book) {
		return book.getBookId();
	}
}
