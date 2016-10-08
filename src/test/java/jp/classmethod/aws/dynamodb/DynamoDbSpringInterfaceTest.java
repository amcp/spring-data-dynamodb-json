package jp.classmethod.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.shared.mapper.DynamoDBObjectMapper;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.node.IntNode;
import com.github.fge.jackson.jsonpointer.JsonPointer;
import com.github.fge.jackson.jsonpointer.JsonPointerException;
import com.github.fge.jsonpatch.AddOperation;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import jp.xet.sparwings.spring.data.chunk.Chunk;
import jp.xet.sparwings.spring.data.chunk.ChunkRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Created by alexp on 10/9/16.
 */
public class DynamoDbSpringInterfaceTest {
	private static final String VISITOR = "visitor";
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

	@Test(expected = IllegalArgumentException.class)
	public void testReadableExistsNull() {
		sut.exists(null);
	}

	@Test
	public void testReadableExistsBookNotCreated() {
		Book Book = new Book("v");
		assertThat(sut.exists(Book.getBookId()), is(false));
	}

	@Test
	public void testReadableExistsBookCreated() {
		Book Book = new Book("v");
		sut.create(Book);
		assertThat(sut.exists(Book.getBookId()), is(true));
	}

	@Test(expected = NullPointerException.class)
	public void testReadableFindOneNull() {
		sut.findOne(null);
	}

	@Test
	public void testReadableFindOneBookNotCreated() {
		Book Book = new Book("v");
		Book actual = sut.findOne(Book.getBookId());
		assertThat(actual, is(nullValue()));
	}

	@Test
	public void testReadableFindOneBookCreated() {
		Book Book = new Book("v");
		sut.create(Book);
		Book found = sut.findOne(Book.getBookId());
		assertThat(found, is(notNullValue()));
		assertThat(found.getBookId(), is(Book.getBookId()));
	}

	@Test(expected = NullPointerException.class)
	public void testReadableGetIdNull() {
		sut.getId(null);
	}

	@Test
	public void testReadableGetIdBookNotCreated() {
		Book Book = new Book("v");
		assertThat(sut.getId(Book), is(notNullValue()));
		assertThat(sut.getId(Book), is(Book.getBookId()));
	}

	@Test
	public void testReadableGetIdBookCreated() {
		Book Book = new Book("v");
		sut.create(Book);
		Book found = sut.findOne(Book.getBookId());
		assertThat(found.getBookId(), is(Book.getBookId()));
	}

	@Test(expected = NullPointerException.class)
	public void testChunkableFindAllNull() {
		ChunkRequest req = null;
		sut.findAll(req);
	}

	@Test
	public void testChunkableFindAllDefaultNoBooks() {
		ChunkRequest req = new ChunkRequest();
		Chunk<Book> Books = sut.findAll(req);
		assertThat(Books.getContent().isEmpty(), is(true));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testChunkableFindAllDescendingNoBooks() {
		ChunkRequest req = new ChunkRequest(Sort.Direction.DESC);
		sut.findAll(req);
	}

	@Test
	public void testChunkableFindAllDefaultOneBooks() {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		ChunkRequest req = new ChunkRequest();
		Chunk<Book> Books = sut.findAll(req);
		assertThat(Books.getContent().isEmpty(), is(false));
		assertThat(Books.getContent().size(), is(1));
		Book chunked = Iterables.getOnlyElement(Books.getContent());
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
		Iterable<Book> Books = sut.findAll(ids);
		assertThat(Books, is(notNullValue()));
		List<Book> BookList = Lists.newArrayList(Books);
		assertThat(BookList.isEmpty(), is(true));
	}

	@Test
	public void testBatchGettableFindAllTwentyFive() {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < 25; i++) {
			ids.add(sut.create(new Book(UuidGenerator.generateModelId().toString())).getBookId());
		}
		Iterable<Book> Books = sut.findAll(ids);
		assertThat(Books, is(notNullValue()));
		List<Book> BookList = Lists.newArrayList(Books);
		assertThat(BookList, is(notNullValue()));
		assertThat(BookList.isEmpty(), is(false));
		assertThat(BookList.size(), is(25));
	}

	@Test
	public void testBatchGettableFindAllTwentySix() {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < 26; i++) {
			ids.add(sut.create(new Book(UuidGenerator.generateModelId().toString())).getBookId());
		}
		sut.findAll(ids);
	}

	@Test(expected = NullPointerException.class)
	public void testCreatableCreateNull() {
		sut.create(null);
	}

	@Test
	public void testCreatableCreateNormal() {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		assertThat(created, is(notNullValue()));
		assertThat(created.getBookId(), is(Book.getBookId()));
		assertThat(created.getName(), is(Book.getName()));
	}

	@Test(expected = DuplicateKeyException.class)
	public void testCreatableCreateDuplicate() {
		Book Book = new Book(VISITOR);
		sut.create(Book);
		sut.create(Book);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeletableDeleteEntityNull() {
		Book Book = null;
		sut.delete(Book);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeletableDeleteEntityNotCreated() {
		Book Book = new Book("asdf");
		sut.delete(Book);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeletableDeleteEntityDifferent() {
		Book Book = new Book("asdf");
		sut.create(Book);
		sut.delete(new Book("asdf"));
	}

	@Test
	public void testDeletableDeleteEntity() {
		Book Book = new Book("asdf");
		Book = sut.create(Book);
		assertThat(sut.exists(Book.getBookId()), is(true));
		sut.delete(Book);
		assertThat(sut.exists(Book.getBookId()), is(false));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeletableDeleteIdNull() {
		String BookId = null;
		sut.delete(BookId);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeletableDeleteIdNotCreated() {
		sut.delete("not here");
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testDeletableDeleteIdDifferent() {
		Book Book = new Book("asdf");
		sut.create(Book);
		sut.delete(Book.getBookId() + "different");
	}

	@Test
	public void testDeletableDeleteId() {
		Book Book = new Book("asdf");
		Book = sut.create(Book);
		assertThat(sut.exists(Book.getBookId()), is(true));
		sut.delete(Book.getBookId());
		assertThat(sut.exists(Book.getBookId()), is(false));
	}

	@Test(expected = NullPointerException.class)
	public void testPatchableKeysNull() {
		sut.update(null /*BookId*/, new JsonPatch(new ArrayList<>()), false /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = NullPointerException.class)
	public void testPatchablePatchNull() {
		Book Book = new Book(VISITOR);
		sut.update(Book.getBookId(), null /*patch*/, false /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPatchableBookDoesntExistNoPatches() {
		Book Book = new Book(VISITOR);
		sut.update(Book.getBookId(), new JsonPatch(new ArrayList<>()), false /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPatchableBookDoesntExistNoPatchesButLockVersion() {
		Book Book = new Book(VISITOR);
		sut.update(Book.getBookId(), new JsonPatch(new ArrayList<>()), false /*increment*/, 0 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistNoPatchesButIncrement() {
		Book Book = new Book(VISITOR);
		sut.update(Book.getBookId(), new JsonPatch(new ArrayList<>()), true /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistNoPatchesButIncrementAndLockVersion() {
		Book Book = new Book(VISITOR);
		sut.update(Book.getBookId(), new JsonPatch(new ArrayList<>()), true /*increment*/, 0 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistWithPatchesIncrementAndLockVersion() throws JsonPointerException {
		Book Book = new Book(VISITOR);
		sut.update(Book.getBookId(),
				getPatch(2),
				true /*increment*/, 0 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistWithPatchesIncrementButNotLockVersion() throws JsonPointerException {
		Book Book = new Book(VISITOR).setName("asdf");
		sut.update(Book.getBookId(),
				getPatch(2),
				true /*increment*/, -1 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testPatchableBookDoesntExistWithPatchesButNotIncrementAndNotLockVersion() throws JsonPointerException {
		Book Book = new Book(VISITOR);
		sut.update(Book.getBookId(),
				getPatch(2),
				false /*increment*/, -1 /*version to lock*/);
	}

	@Test
	public void testPatchableBookExistsWithPatchesButNotIncrementAndNotLockVersion() throws JsonPointerException {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		Book updated = sut.update(Book.getBookId(),
				getPatch(2), false /*increment*/, -1 /*version to lock*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getName(), is(VISITOR));
		assertThat(updated.getVersion(), is(created.getVersion()));
	}

	@Test
	public void testPatchableBookExistsWithPatchesAndIncrementButNotLockVersion() throws JsonPointerException {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		Book updated = sut.update(Book.getBookId(),
				getPatch(2), true /*increment*/, -1 /*version to lock*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getName(), is(VISITOR));
		assertThat(updated.getVersion(), is(created.getVersion() + 1L));
	}

	@Test
	public void testPatchableBookExistsWithPatchesNotIncrementButWithLockVersionSucceed() throws JsonPointerException {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		Book updated = sut.update(Book.getBookId(), getPatch(2), false /*increment*/,
				created.getVersion() /*version to lock*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getName(), is(VISITOR));
		assertThat(updated.getVersion(), is(created.getVersion()));
	}

	private JsonPatch getPatch(int quantity) throws JsonPointerException {
		return new JsonPatch(Collections.singletonList(new AddOperation(new JsonPointer("/name"),
				IntNode.valueOf(quantity))));
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testPatchableBookExistsWithPatchesNotIncrementButWithLockVersionFail() throws JsonPointerException {
		Book Book = new Book(VISITOR);
		sut.create(Book);
		sut.update(Book.getBookId(), getPatch(2), false /*increment*/, 2 /*version to lock*/);
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testPatchableBookExistsWithPatchesAndIncrementAndLockVersionFail() throws JsonPointerException {
		Book Book = new Book(VISITOR);
		sut.create(Book);
		sut.update(Book.getBookId(), getPatch(2), true /*increment*/, 2 /*version to lock*/);
	}

	@Test
	public void testPatchableBookExistsWithPatchesAndIncrementAndLockVersionSucceed() throws JsonPointerException {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		Book updated = sut.update(Book.getBookId(), getPatch(2), true /*increment*/,
				created.getVersion() /*version to lock*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getName(), is(VISITOR));
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
		Book Book = new Book(VISITOR);
		sut.update(Book /*domain*/, null /*condition*/);
	}

	@Test
	public void testConditionalEntityNotNullConditionNullSucceed() {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		Book updated = sut.update(created /*domain*/, null /*condition*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getVersion(), is(created.getVersion()));
	}

	@Test
	public void testConditionalEntityNotNullConditionNotNullSucceed() {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		Book updated = sut.update(created /*domain*/, VersionCondition.of(Optional.of(1L)) /*condition*/);
		assertThat(updated, is(notNullValue()));
		assertThat(updated.getBookId(), is(created.getBookId()));
		assertThat(updated.getVersion(), is(created.getVersion()));
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testConditionalEntityNotNullConditionNotNullFail() {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
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
		Book Book = new Book(VISITOR);
		sut.getAndDelete(Book.getBookId() /*BookId*/, -2 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testGetDeletableKeysNotNullNoLockingVersionNotExists() {
		Book Book = new Book(VISITOR);
		sut.getAndDelete(Book.getBookId() /*BookId*/, -1 /*version to lock*/);
	}

	@Test(expected = IncorrectResultSizeDataAccessException.class)
	public void testGetDeletableKeysNotNullLockingVersionNotExists() {
		Book Book = new Book(VISITOR);
		sut.getAndDelete(Book.getBookId() /*BookId*/, 0 /*version to lock*/);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetDeletableKeysNotNullIllegalVersionExists() {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		sut.getAndDelete(created.getBookId() /*BookId*/, -2 /*version to lock*/);
	}

	@Test
	public void testGetDeletableKeysNotNullNoLockingVersionExists() {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		Book deleted = sut.getAndDelete(created.getBookId() /*BookId*/, -1 /*version to lock*/);
		assertThat(deleted, is(notNullValue()));
		assertThat(deleted.getBookId(), is(created.getBookId()));
		assertThat(deleted.getVersion(), is(created.getVersion()));
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void testGetDeletableKeysNotNullLockingVersionExistsWrongVersion() {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		sut.getAndDelete(created.getBookId() /*BookId*/, 0 /*version to lock*/);
	}

	@Test
	public void testGetDeletableKeysNotNullLockingVersionExistsRightVersion() {
		Book Book = new Book(VISITOR);
		Book created = sut.create(Book);
		Book deleted = sut.getAndDelete(created.getBookId() /*BookId*/, 1 /*version to lock*/);
		assertThat(deleted, is(notNullValue()));
		assertThat(deleted.getBookId(), is(created.getBookId()));
		assertThat(deleted.getVersion(), is(created.getVersion()));
	}
}
