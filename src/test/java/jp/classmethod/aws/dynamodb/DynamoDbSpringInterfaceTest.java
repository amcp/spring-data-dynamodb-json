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

import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.fge.jackson.jsonpointer.JsonPointer;
import com.github.fge.jackson.jsonpointer.JsonPointerException;
import com.github.fge.jsonpatch.AddOperation;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import jp.classmethod.aws.infrastructure.BookDynamoDbRepository;
import jp.classmethod.aws.model.Book;
import jp.xet.sparwings.spring.data.chunk.Chunk;
import jp.xet.sparwings.spring.data.chunk.ChunkRequest;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.NonTransientDataAccessResourceException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Functional tests of the spring and spar-wings BaseRepository interfaces
 *
 * @author Alexander Patrikalakis
 * @since #version#
 */
public class DynamoDbSpringInterfaceTest extends AbstractDynamoDbTest {
	private static final String BOOK_NAME = "The Great Gatsby";
	private static final String SECOND_BOOK_NAME = "Great Expectations";

	@Test(expected = IllegalArgumentException.class)
	public void testReadableExistsNull() {
		sut.exists(null);
	}

	@Test
	public void testReadableExistsBookNotCreated() {
		Book book = new Book("v");
		assertThat(sut.exists(book.getBookId()), is(false));
	}

	@Test
	public void testReadableExistsBookCreated() {
		Book book = new Book("v");
		sut.create(book);
		assertThat(sut.exists(book.getBookId()), is(true));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReadableFindOneNull() {
		sut.findOne(null);
	}

	@Test
	public void testReadableFindOneBookNotCreated() {
		Book book = new Book("v");
		Book actual = sut.findOne(book.getBookId());
		assertThat(actual, is(nullValue()));
	}

	@Test
	public void testReadableFindOneBookCreated() {
		Book book = new Book("v");
		sut.create(book);
		Book found = sut.findOne(book.getBookId());
		assertThat(found, is(notNullValue()));
		assertThat(found.getBookId(), is(book.getBookId()));
	}

	@Test(expected = NullPointerException.class)
	public void testReadableGetIdNull() {
		sut.getId(null);
	}

	@Test
	public void testReadableGetIdBookNotCreated() {
		Book book = new Book("v");
		assertThat(sut.getId(book), is(notNullValue()));
		assertThat(sut.getId(book), is(book.getBookId()));
	}

	@Test
	public void testReadableGetIdBookCreated() {
		Book book = new Book("v");
		sut.create(book);
		Book found = sut.findOne(book.getBookId());
		assertThat(found.getBookId(), is(book.getBookId()));
	}

	@Test(expected = NullPointerException.class)
	public void testChunkableFindAllNull() {
		ChunkRequest req = null;
		sut.findAll(req);
	}

	@Test
	public void testChunkableFindAllDefaultNoBooks() {
		ChunkRequest req = new ChunkRequest();
		Chunk<Book> books = sut.findAll(req);
		assertThat(books.getContent().isEmpty(), is(true));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testChunkableFindAllDescendingNoBooks() {
		ChunkRequest req = new ChunkRequest(Sort.Direction.DESC);
		sut.findAll(req);
	}

	@Test
	public void testChunkableFindAllDefaultOneBooks() {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		ChunkRequest req = new ChunkRequest();
		Chunk<Book> books = sut.findAll(req);
		assertThat(books.getContent().isEmpty(), is(false));
		assertThat(books.getContent().size(), is(1));
		Book chunked = Iterables.getOnlyElement(books.getContent());
		assertThat(chunked, is(created));
	}

	@Test(expected = NullPointerException.class)
	public void testBatchGettableFindAllNull() {
		List<String> ids = null;
		sut.findAll(ids);
	}

	@Test
	public void testBatchGettableFindAllEmpty() {
		List<String> ids = new ArrayList<>();
		Iterable<Book> books = sut.findAll(ids);
		assertThat(books, is(notNullValue()));
		List<Book> bookList = Lists.newArrayList(books);
		assertThat(bookList.isEmpty(), is(true));
	}

	@Test
	public void testBatchGettableFindAllTwentyFive() {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < 25; i++) {
			ids.add(sut.create(new Book(UuidGenerator.generateModelId().toString())).getBookId());
		}
		Iterable<Book> books = sut.findAll(ids);
		assertThat(books, is(notNullValue()));
		List<Book> bookList = Lists.newArrayList(books);
		assertThat(bookList, is(notNullValue()));
		assertThat(bookList.isEmpty(), is(false));
		assertThat(bookList.size(), is(25));
	}

	@Test
	public void testBatchGettableFindAllTwentySix() {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < 26; i++) {
			ids.add(sut.create(new Book(UuidGenerator.generateModelId().toString())).getBookId());
		}
		sut.findAll(ids);
	}

	@Test
	public void testCreatableCreateNull() {
		assertThat(sut.create(null), is(nullValue()));
	}

	@Test
	public void testCreatableCreateNormal() {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		assertThat(created, is(notNullValue()));
		assertThat(created.getBookId(), is(book.getBookId()));
		assertThat(created.getName(), is(book.getName()));
	}

	@Test(expected = DuplicateKeyException.class)
	public void testCreatableCreateDuplicate() {
		Book book = new Book(BOOK_NAME);
		sut.create(book);
		sut.create(book);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeletableDeleteEntityNull() {
		Book book = null;
		sut.delete(book);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeletableDeleteEntityNotCreated() {
		Book book = new Book("asdf");
		sut.delete(book);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeletableDeleteEntityDifferent() {
		Book book = new Book("asdf");
		sut.create(book);
		sut.delete(new Book("asdf"));
	}

	@Test
	public void testDeletableDeleteEntity() {
		Book book = new Book("asdf");
		book = sut.create(book);
		assertThat(sut.exists(book.getBookId()), is(true));
		sut.delete(book);
		assertThat(sut.exists(book.getBookId()), is(false));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeletableDeleteIdNull() {
		String bookId = null;
		sut.delete(bookId);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeletableDeleteIdNotCreated() {
		sut.delete("not here");
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeletableDeleteIdDifferent() {
		Book book = new Book("asdf");
		sut.create(book);
		sut.delete(book.getBookId() + "different");
	}

	@Test
	public void testDeletableDeleteId() {
		Book book = new Book("asdf");
		book = sut.create(book);
		assertThat(sut.exists(book.getBookId()), is(true));
		sut.delete(book.getBookId());
		assertThat(sut.exists(book.getBookId()), is(false));
	}

	@Test(expected = NullPointerException.class)
	public void testPatchableKeysNull() {
		sut.update(null /*BookId*/, new JsonPatch(new ArrayList<>()), false /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = NullPointerException.class)
	public void testPatchablePatchNull() {
		Book book = new Book(BOOK_NAME);
		sut.update(book.getBookId(), null /*patch*/, false /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPatchableBookDoesntExistNoPatches() {
		Book book = new Book(BOOK_NAME);
		sut.update(book.getBookId(), new JsonPatch(new ArrayList<>()), false /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPatchableBookDoesntExistNoPatchesButLockVersion() {
		Book book = new Book(BOOK_NAME);
		sut.update(book.getBookId(), new JsonPatch(new ArrayList<>()), false /*increment*/, 0 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistNoPatchesButIncrement() {
		Book book = new Book(BOOK_NAME);
		sut.update(book.getBookId(), new JsonPatch(new ArrayList<>()), true /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistNoPatchesButIncrementAndLockVersion() {
		Book book = new Book(BOOK_NAME);
		sut.update(book.getBookId(), new JsonPatch(new ArrayList<>()), true /*increment*/, 0 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistWithPatchesIncrementAndLockVersion() throws JsonPointerException {
		Book book = new Book(BOOK_NAME);
		sut.update(book.getBookId(),
				getPatch(SECOND_BOOK_NAME),
				true /*increment*/, 0 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistWithPatchesIncrementButNotLockVersion() throws JsonPointerException {
		Book book = new Book(BOOK_NAME);
		sut.update(book.getBookId(),
				getPatch(SECOND_BOOK_NAME),
				true /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistWithPatchesButNotIncrementAndNotLockVersion() throws JsonPointerException {
		Book book = new Book(BOOK_NAME);
		sut.update(book.getBookId(),
				getPatch(SECOND_BOOK_NAME),
				false /*increment*/, -1 /*version to lock*/);
	}

	@Test
	public void testPatchableBookExistsWithPatchesButNotIncrementAndNotLockVersion() throws JsonPointerException {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		Book updated = sut.update(book.getBookId(),
				getPatch(SECOND_BOOK_NAME), false /*increment*/, -1 /*version to lock*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getName(), is(SECOND_BOOK_NAME));
		assertThat(updated.getVersion(), is(created.getVersion()));
	}

	@Test
	public void testPatchableBookExistsWithPatchesAndIncrementButNotLockVersion() throws JsonPointerException {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		Book updated = sut.update(book.getBookId(),
				getPatch(SECOND_BOOK_NAME), true /*increment*/, -1 /*version to lock*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getName(), is(SECOND_BOOK_NAME));
		assertThat(updated.getVersion(), is(created.getVersion() + 1L));
	}

	@Test
	public void testPatchableBookExistsWithPatchesNotIncrementButWithLockVersionSucceed() throws JsonPointerException {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		Book updated = sut.update(book.getBookId(), getPatch(SECOND_BOOK_NAME), false /*increment*/,
				created.getVersion() /*version to lock*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getName(), is(SECOND_BOOK_NAME));
		assertThat(updated.getVersion(), is(created.getVersion()));
	}

	private JsonPatch getPatch(String name) throws JsonPointerException {
		return new JsonPatch(Collections.singletonList(new AddOperation(new JsonPointer("/name"),
				TextNode.valueOf(name))));
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testPatchableBookExistsWithPatchesNotIncrementButWithLockVersionFail() throws JsonPointerException {
		Book book = new Book(BOOK_NAME);
		sut.create(book);
		sut.update(book.getBookId(), getPatch(SECOND_BOOK_NAME), false /*increment*/, 2 /*version to lock*/);
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testPatchableBookExistsWithPatchesAndIncrementAndLockVersionFail() throws JsonPointerException {
		Book book = new Book(BOOK_NAME);
		sut.create(book);
		sut.update(book.getBookId(), getPatch(SECOND_BOOK_NAME), true /*increment*/, 2 /*version to lock*/);
	}

	@Test
	public void testPatchableBookExistsWithPatchesAndIncrementAndLockVersionSucceed() throws JsonPointerException {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		Book updated = sut.update(book.getBookId(), getPatch(SECOND_BOOK_NAME), true /*increment*/,
				created.getVersion() /*version to lock*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getName(), is(SECOND_BOOK_NAME));
		assertThat(updated.getVersion(), is(created.getVersion() + 1L));
	}

	@Test(expected = NullPointerException.class)
	public void testConditionalEntityConditionNull() {
		sut.update(null /*domain*/, null /*condition*/);
	}

	@Test(expected = NullPointerException.class)
	public void testConditionalEntityNullConditionNotNull() {
		sut.update(null /*domain*/, VersionCondition.of(Optional.of(1L)) /*condition*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testConditionalEntityNotNullConditionNullFail() {
		Book book = new Book(BOOK_NAME);
		sut.update(book /*domain*/, null /*condition*/);
	}

	@Test
	public void testConditionalEntityNotNullConditionNullSucceed() {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		Book updated = sut.update(created /*domain*/, null /*condition*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getVersion(), is(created.getVersion()));
	}

	@Test
	public void testConditionalEntityNotNullConditionNotNullSucceed() {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		Book updated = sut.update(created /*domain*/, VersionCondition.of(Optional.of(0L)) /*condition*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getVersion(), is(created.getVersion()));
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testConditionalEntityNotNullConditionNotNullFail() {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		sut.update(created /*domain*/, VersionCondition.of(Optional.of(2L)) /*condition*/);
	}

	@Test(expected = NullPointerException.class)
	public void testGetDeletableKeysNullIllegalVersion() {
		sut.getAndDelete(null /*BookId*/, -2 /*version to lock*/);
	}

	@Test(expected = NullPointerException.class)
	public void testGetDeletableKeysNullNoLockingVersion() {
		sut.getAndDelete(null /*BookId*/, -1 /*version to lock*/);
	}

	@Test(expected = NullPointerException.class)
	public void testGetDeletableKeysNullLockingVersion() {
		sut.getAndDelete(null /*BookId*/, 0 /*version to lock*/);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetDeletableKeysNotNullIllegalVersionNotExists() {
		Book book = new Book(BOOK_NAME);
		sut.getAndDelete(book.getBookId() /*BookId*/, -2 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testGetDeletableKeysNotNullNoLockingVersionNotExists() {
		Book book = new Book(BOOK_NAME);
		sut.getAndDelete(book.getBookId() /*BookId*/, -1 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testGetDeletableKeysNotNullLockingVersionNotExists() {
		Book book = new Book(BOOK_NAME);
		sut.getAndDelete(book.getBookId() /*BookId*/, 0 /*version to lock*/);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetDeletableKeysNotNullIllegalVersionExists() {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		sut.getAndDelete(created.getBookId() /*BookId*/, -2 /*version to lock*/);
	}

	@Test
	public void testGetDeletableKeysNotNullNoLockingVersionExists() {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		Book deleted = sut.getAndDelete(created.getBookId() /*BookId*/, -1 /*version to lock*/);
		assertThat(deleted, is(notNullValue()));
		assertThat(deleted.getBookId(), is(created.getBookId()));
		assertThat(deleted.getVersion(), is(created.getVersion()));
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testGetDeletableKeysNotNullLockingVersionExistsWrongVersion() {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		sut.getAndDelete(created.getBookId() /*BookId*/, 1 /*version to lock*/);
	}

	@Test
	public void testGetDeletableKeysNotNullLockingVersionExistsRightVersion() {
		Book book = new Book(BOOK_NAME);
		Book created = sut.create(book);
		Book deleted = sut.getAndDelete(created.getBookId() /*BookId*/, 0 /*version to lock*/);
		assertThat(deleted, is(notNullValue()));
		assertThat(deleted.getBookId(), is(created.getBookId()));
		assertThat(deleted.getVersion(), is(created.getVersion()));
	}

	@Test(expected = NonTransientDataAccessResourceException.class)
	public void testTruncateTableDoesntExist() {
		dynamoDBLocalRule.getAmazonDynamoDB().deleteTable(BookDynamoDbRepository.TABLE_NAME);
		try {
			sut.deleteAll();
		} catch (Exception e) {
			assertThat(e.getCause(), is(instanceOf(ResourceNotFoundException.class)));
			sut.afterPropertiesSet();
			throw e;
		}
		fail();
	}
}
