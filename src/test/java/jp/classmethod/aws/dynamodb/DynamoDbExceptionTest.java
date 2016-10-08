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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.local.shared.mapper.DynamoDBObjectMapper;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Lists;
import jp.classmethod.aws.infrastructure.BookDynamoDbRepository;
import jp.classmethod.aws.model.Book;
import jp.xet.sparwings.spring.data.chunk.ChunkRequest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Matchers;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.NonTransientDataAccessResourceException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exceptional case tests of the spring and spar-wings BaseRepository interfaces
 *
 * @author Alexander Patrikalakis
 * @since #version#
 */
public class DynamoDbExceptionTest {
	@ClassRule
	public static DynamoDbLocalRule dynamoDBLocalRule = new DynamoDbLocalRule();

	AmazonDynamoDB dynamoDb;

	BookDynamoDbRepository sut;

	@Before
	public void setup() throws Exception {
		dynamoDb = mock(AmazonDynamoDBClient.class);
		DynamoDBObjectMapper objectMapper = new DynamoDBObjectMapper();
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		sut = new BookDynamoDbRepository(new ProvisionedThroughput(1L, 1L), dynamoDb, objectMapper);
	}

	@Test(expected = NullPointerException.class)
	public void testChunkableNPE() {
		ChunkRequest req = null;
		sut.findAll(req);
	}

	@Test(expected = QueryTimeoutException.class)
	public void testChunkableThrottle() {
		when(dynamoDb.scan(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.findAll(new ChunkRequest(Sort.Direction.ASC));
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testChunkableClientException() {
		when(dynamoDb.scan(anyObject())).thenThrow(new AmazonClientException("asdf"));
		sut.findAll(new ChunkRequest(Sort.Direction.ASC));
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testChunkableServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.scan(anyObject())).thenThrow(ase);
		sut.findAll(new ChunkRequest(Sort.Direction.ASC));
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testChunkableServiceExceptionNotValidation() {
		when(dynamoDb.scan(anyObject())).thenThrow(new AmazonServiceException("asdf"));
		sut.findAll(new ChunkRequest(Sort.Direction.ASC));
	}

	@Test(expected = NullPointerException.class)
	public void testBatchNPE() {
		List<String> req = null;
		sut.findAll(req);
	}

	@Test(expected = QueryTimeoutException.class)
	public void testBatchThrottle() {
		when(dynamoDb.batchGetItem(Matchers.<BatchGetItemRequest>anyObject()))
				.thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.findAll(Lists.newArrayList("a"));
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testBatchClientException() {
		when(dynamoDb.batchGetItem(Matchers.<BatchGetItemRequest>anyObject()))
				.thenThrow(new AmazonClientException("asdf"));
		sut.findAll(Lists.newArrayList("a"));
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testBatchServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.batchGetItem(Matchers.<BatchGetItemRequest>anyObject())).thenThrow(ase);
		sut.findAll(Lists.newArrayList("a"));
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testBatchServiceExceptionNotValidation() {
		when(dynamoDb.batchGetItem(Matchers.<BatchGetItemRequest>anyObject()))
				.thenThrow(new AmazonServiceException("asdf"));
		sut.findAll(Lists.newArrayList("a"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFindOneNPE() {
		sut.findOne(null);
	}

	@Test(expected = QueryTimeoutException.class)
	public void testFindOneThrottle() {
		when(dynamoDb.getItem(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.findOne("a");
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testFindOneClientException() {
		when(dynamoDb.getItem(anyObject())).thenThrow(new AmazonClientException("asdf"));
		sut.findOne("a");
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testFindOneServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.getItem(anyObject())).thenThrow(ase);
		sut.findOne("a");
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testFindOneServiceExceptionNotValidation() {
		when(dynamoDb.getItem(anyObject())).thenThrow(new AmazonServiceException("asdf"));
		sut.findOne("a");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExistsNPE() {
		sut.exists(null);
	}

	@Test(expected = QueryTimeoutException.class)
	public void testExistsThrottle() {
		when(dynamoDb.getItem(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.exists("a");
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testExistsClientException() {
		when(dynamoDb.getItem(anyObject())).thenThrow(new AmazonClientException("asdf"));
		sut.exists("a");
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testExistsServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.getItem(anyObject())).thenThrow(ase);
		sut.exists("a");
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testExistsServiceExceptionNotValidation() {
		when(dynamoDb.getItem(anyObject())).thenThrow(new AmazonServiceException("asdf"));
		sut.exists("a");
	}

	@Test(expected = QueryTimeoutException.class)
	public void testCreateThrottle() {
		when(dynamoDb.putItem(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.create(new Book("asdf"));
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testCreateClientException() {
		when(dynamoDb.putItem(anyObject())).thenThrow(new AmazonClientException("asdf"));
		sut.create(new Book("asdf"));
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testCreateServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.putItem(anyObject())).thenThrow(ase);
		sut.create(new Book("asdf"));
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testCreateServiceExceptionNotValidation() {
		when(dynamoDb.putItem(anyObject())).thenThrow(new AmazonServiceException("asdf"));
		sut.create(new Book("asdf"));
	}

	@Test(expected = DuplicateKeyException.class)
	public void testCreateDuplicateKeyException() {
		when(dynamoDb.putItem(anyObject())).thenThrow(new ConditionalCheckFailedException("asdf"));
		sut.create(new Book("asdf"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeleteEntityNull() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		Book book = null;
		sut.delete(book);
	}

	@Test(expected = QueryTimeoutException.class)
	public void testDeleteEntityThrottle() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.delete(new Book("asdf"));
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testDeleteEntityClientException() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new AmazonClientException("asdf"));
		sut.delete(new Book("asdf"));
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testDeleteEntityServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.deleteItem(anyObject())).thenThrow(ase);
		sut.delete(new Book("asdf"));
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testDeleteEntityServiceExceptionNotValidation() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new AmazonServiceException("asdf"));
		sut.delete(new Book("asdf"));
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeleteEntityDoesntExist() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new ConditionalCheckFailedException("asdf"));
		sut.delete(new Book("asdf"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeleteIdNull() {
		String id = null;
		sut.delete(id);
	}

	@Test(expected = QueryTimeoutException.class)
	public void testDeleteIdThrottle() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.delete("id");
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testDeleteIdClientException() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new AmazonClientException("asdf"));
		sut.delete("id");
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testDeleteIdServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.deleteItem(anyObject())).thenThrow(ase);
		sut.delete("id");
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testDeleteIdServiceExceptionNotValidation() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new AmazonServiceException("asdf"));
		sut.delete("id");
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeleteIdDoesntExist() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new ConditionalCheckFailedException("asdf"));
		sut.delete("id");
	}

	@Test(expected = NullPointerException.class)
	public void testPatchNull() {
		sut.update("key", null /*patch*/, false /*increment*/, -1L);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPatchEmpty() {
		sut.update("key", new JsonPatch(new ArrayList<>()), false /*increment*/, -1L);
	}

	@Test(expected = QueryTimeoutException.class)
	public void testPatchThrottle() {
		when(dynamoDb.updateItem(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.update("key", new JsonPatch(new ArrayList<>()), true /*increment*/, -1L);
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testPatchClientException() {
		when(dynamoDb.updateItem(anyObject())).thenThrow(new AmazonClientException("asdf"));
		sut.update("key", new JsonPatch(new ArrayList<>()), true /*increment*/, -1L);
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testPatchServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.updateItem(anyObject())).thenThrow(ase);
		sut.update("key", new JsonPatch(new ArrayList<>()), true /*increment*/, -1L);
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testPatchServiceExceptionNotValidation() {
		when(dynamoDb.updateItem(anyObject())).thenThrow(new AmazonServiceException("asdf"));
		sut.update("key", new JsonPatch(new ArrayList<>()), true /*increment*/, -1L);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchDoesntExist() {
		when(dynamoDb.updateItem(anyObject())).thenThrow(new ConditionalCheckFailedException("asdf"));
		when(dynamoDb.getItem(anyObject())).thenReturn(new GetItemResult());
		sut.update("key", new JsonPatch(new ArrayList<>()), true /*increment*/, -1L);
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testPatchVersionLockFailed() {
		when(dynamoDb.updateItem(anyObject())).thenThrow(new ConditionalCheckFailedException("asdf"));
		when(dynamoDb.getItem(anyObject())).thenReturn(new GetItemResult().withItem(new HashMap<>()));
		sut.update("key", new JsonPatch(new ArrayList<>()), true /*increment*/, 0L);
	}

	@Test(expected = NullPointerException.class)
	public void testConditionedUpdateNull() {
		sut.update(null /*entity*/, null /*versionConditions*/);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConditionedUpdateEmpty() {
		sut.update(new Book(null /*bookId - hash key*/, "name", 0L) /*entity*/, null /*versionConditions*/);
	}

	@Test(expected = QueryTimeoutException.class)
	public void testConditionedUpdateThrottle() {
		when(dynamoDb.putItem(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.update(new Book("name") /*entity*/, null /*versionConditions*/);
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testConditionedUpdateClientException() {
		when(dynamoDb.putItem(anyObject())).thenThrow(new AmazonClientException("asdf"));
		sut.update(new Book("name") /*entity*/, null /*versionConditions*/);
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testConditionedUpdateServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.putItem(anyObject())).thenThrow(ase);
		sut.update(new Book("name") /*entity*/, null /*versionConditions*/);
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testConditionedUpdateServiceExceptionNotValidation() {
		when(dynamoDb.putItem(anyObject())).thenThrow(new AmazonServiceException("asdf"));
		sut.update(new Book("name") /*entity*/, null /*versionConditions*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testConditionedUpdateDoesntExist() {
		when(dynamoDb.putItem(anyObject())).thenThrow(new ConditionalCheckFailedException("asdf"));
		when(dynamoDb.getItem(anyObject())).thenReturn(new GetItemResult());
		sut.update(new Book("name") /*entity*/, null /*versionConditions*/);
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testConditionedUpdateVersionLockFailed() {
		when(dynamoDb.putItem(anyObject())).thenThrow(new ConditionalCheckFailedException("asdf"));
		when(dynamoDb.getItem(anyObject())).thenReturn(new GetItemResult().withItem(new HashMap<>()));
		sut.update(new Book("name") /*entity*/, null /*versionConditions*/);
	}

	@Test(expected = NullPointerException.class)
	public void testGetDeleteNull() {
		sut.getAndDelete(null /*key*/, -1L);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetDeleteBadVersion() {
		sut.getAndDelete("" /*key*/, -2L);
	}

	@Test(expected = QueryTimeoutException.class)
	public void testGetDeleteThrottle() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new ProvisionedThroughputExceededException("asdf"));
		sut.getAndDelete("" /*key*/, -1L);
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testGetDeleteClientException() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new AmazonClientException("asdf"));
		sut.getAndDelete("" /*key*/, -1L);
	}

	@Test(expected = InvalidDataAccessResourceUsageException.class)
	public void testGetDeleteServiceExceptionValidation() {
		AmazonServiceException ase = new AmazonServiceException("asdf");
		ase.setErrorCode("ValidationException");
		when(dynamoDb.deleteItem(anyObject())).thenThrow(ase);
		sut.getAndDelete("" /*key*/, -1L);
	}

	@Test(expected = DynamoDbServiceException.class)
	public void testGetDeleteServiceExceptionNotValidation() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new AmazonServiceException("asdf"));
		sut.getAndDelete("" /*key*/, -1L);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testGetDeleteDoesntExist() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new ConditionalCheckFailedException("asdf"));
		when(dynamoDb.getItem(anyObject())).thenReturn(new GetItemResult());
		sut.getAndDelete("" /*key*/, -1L);
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testGetDeleteVersionLockFailed() {
		when(dynamoDb.deleteItem(anyObject())).thenThrow(new ConditionalCheckFailedException("asdf"));
		when(dynamoDb.getItem(anyObject())).thenReturn(new GetItemResult().withItem(new HashMap<>()));
		sut.getAndDelete("" /*key*/, 0L);
	}

	@Test(expected = NonTransientDataAccessResourceException.class)
	public void testDeleteTableDoesntExist() {
		when(dynamoDb.deleteTable(Matchers.<String>anyObject())).thenThrow(new ResourceNotFoundException("asdf"));
		sut.deleteAll();
	}
}
