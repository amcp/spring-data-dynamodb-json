package jp.classmethod.aws.dynamodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.NoRepositoryBean;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Attribute;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateTableSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexUpdate;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.PutItemExpressionSpec;
import com.amazonaws.services.dynamodbv2.xspec.UpdateItemExpressionSpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import jp.xet.sparwings.spring.data.chunk.Chunk;
import jp.xet.sparwings.spring.data.chunk.ChunkImpl;
import jp.xet.sparwings.spring.data.chunk.Chunkable;
import jp.xet.sparwings.spring.data.repository.BatchReadableRepository;
import jp.xet.sparwings.spring.data.repository.ChunkableRepository;
import jp.xet.sparwings.spring.data.repository.ConditionalUpdatableRepository;
import jp.xet.sparwings.spring.data.repository.CreatableRepository;
import jp.xet.sparwings.spring.data.repository.DeletableRepository;
import jp.xet.sparwings.spring.data.repository.TruncatableRepository;

/**
 * @author Alexander Patrikalakis
 * @since #version#
 *
 * This class contains the basic logic and implementations of some of the methods of the
 * MultiIndexBaseRepository interface, when the backing store is DynamoDB
 *
 * final play zone
 *
 * @param <E> the entity type to be persisted
 *
 * TODO some refactoring to pass the PMD GodClass validation
 */
@Slf4j
@NoRepositoryBean
public abstract class DynamoDbRepository<E, K extends Serializable> implements InitializingBean,
		ChunkableRepository<E, K>,
		BatchReadableRepository<E, K>,
		CreatableRepository<E, K>,
		DeletableRepository<E, K>,
		PatchableRepository<E, K>,
		ConditionalUpdatableRepository<E, K, VersionCondition>,
		GetDeletableRepository<E, K>,
		TruncatableRepository<E, K> {

	public static final String UPDATE_FAILED_ENTITY_NOT_FOUND = "update failed because entity not found";

	public static final String UPDATE_FAILED_NOT_FOUND_OR_BAD_VERSION =
			UPDATE_FAILED_ENTITY_NOT_FOUND + " or the version was wrong or JSON patch conditions were not met";

	private static final String EXPRESSION_REFERS_TO_NON_EXTANT_ATTRIBUTE =
			"The provided expression refers to an attribute that does not exist in the item";

	private static final String VALIDATION_EXCEPTION = "ValidationException";

	public static final String VERSION = "version";


	private ProvisionedThroughput convert(ProvisionedThroughputDescription d) {
		return new ProvisionedThroughput(d.getReadCapacityUnits(), d.getWriteCapacityUnits());
	}

	/**
	 * Creates a GSI configuration object
	 *
	 * @param name             name of the index object to create
	 * @param hashKey          hash key of the index
	 * @param rangeKey         range key of the index
	 * @param nonKeyAttributes determines the projection type and top level projected attributes.
	 *                         if null, ALL attributes are projected. if an empty list, KEYS_ONLY are projected.
	 *                         if the list has elements, only the elements INCLUDED in the list are projected
	 * @return a description of a global secondary index that can be used to create a table.
	 */
	protected static GlobalSecondaryIndex createGlobalSecondaryIndex(String name, String hashKey, String rangeKey,
																	 List<String> nonKeyAttributes) {
		Preconditions.checkArgument(false == Strings.isNullOrEmpty(hashKey));
		final KeySchemaElement hks = new KeySchemaElement(hashKey, KeyType.HASH);

		final Projection projection;
		if (nonKeyAttributes == null) {
			projection = new Projection().withProjectionType(ProjectionType.ALL);
		} else if (nonKeyAttributes.isEmpty()) {
			projection = new Projection().withProjectionType(ProjectionType.KEYS_ONLY);
		} else {
			projection = new Projection().withProjectionType(ProjectionType.INCLUDE)
					.withNonKeyAttributes(nonKeyAttributes);
		}

		final GlobalSecondaryIndex result = new GlobalSecondaryIndex().withIndexName(name).withProjection(projection);
		if (Strings.isNullOrEmpty(rangeKey)) {
			result.withKeySchema(hks);
		} else {
			result.withKeySchema(hks, new KeySchemaElement(rangeKey, KeyType.RANGE));
		}
		return result;
	}


	/** document sdk table wrapper */
	protected final Table table;

	/**scalar key type definitions**/
	final Map<String, ScalarAttributeType> definitions;

	/**base table primary keys**/
	final List<KeySchemaElement> schemata;

	/**the key condition to use when persisting a new entity**/
	final String conditionalCreateCondition;

	/**base table hash key name**/
	final String hashKeyName;

	/**base table range key name. Null if hash only schema**/
	final String rangeKeyName;

	/**hash key names of the gsis**/
	protected final Map<String, String> gsiHashKeys;

	/**range key names of the gsis**/
	protected final Map<String, String> gsiRangeKeys;

	protected final AmazonDynamoDB dynamoDB;

	private final Map<String, ProvisionedThroughput> ptMap;

	private final Map<String, GlobalSecondaryIndex> gsis;

	private final Map<String, String> lookupKeyConditions;

	private final ObjectMapper objectMapper;

	private final String conditionalDeleteCondition;

	private final Class<E> clazz;

	private final String tableNameSuffix;


	/**
	 * Create instance.
	 *
	 * @param prefix the table prefix
	 * @param tableNameSuffix the suffix for the table name
	 * @param amazonDynamoDB dynamodb client
	 * @param provisionedThroughputMap map of provisioned througput
	 * @param objectMapper mapper to use for back/forth to json
	 * @param clazz class reference of E
	 * @param attributeDefinitions types of keys on base table and GSI
	 * @param baseTableKeyNames names of base table keys
	 * @param gsiList list of GSI definitions
	 * @since #version#
	 */
	protected DynamoDbRepository(String prefix, String tableNameSuffix, AmazonDynamoDB amazonDynamoDB,
								 Map<String, ProvisionedThroughput> provisionedThroughputMap, ObjectMapper objectMapper,
								 Class<E> clazz, Map<String, ScalarAttributeType> attributeDefinitions,
								 List<String> baseTableKeyNames, Map<String, GlobalSecondaryIndex> gsiList) {
		Preconditions.checkNotNull(amazonDynamoDB);
		Preconditions.checkArgument(false == Strings.isNullOrEmpty(tableNameSuffix));
		Preconditions.checkNotNull(provisionedThroughputMap);
		Preconditions.checkNotNull(attributeDefinitions);
		this.dynamoDB = amazonDynamoDB;
		this.tableNameSuffix = tableNameSuffix;
		final String tableName =
				Strings.isNullOrEmpty(prefix) ? tableNameSuffix
						: String.format(Locale.ENGLISH, "%s_%s", prefix, tableNameSuffix);
		this.table = new Table(this.dynamoDB, tableName);
		this.ptMap = provisionedThroughputMap;
		this.gsis = gsiList != null ? new HashMap<>() : null;
		this.definitions = new HashMap<>(attributeDefinitions);
		this.lookupKeyConditions = new HashMap<>();
		this.objectMapper = objectMapper;
		this.clazz = clazz;
		this.gsiHashKeys = new HashMap<>();
		this.gsiRangeKeys = new HashMap<>();
		Optional.ofNullable(gsiList).orElse(new HashMap<>()).values().forEach(gsi -> {
			final String indexName = gsi.getIndexName();
			//make a copy
			final GlobalSecondaryIndex copy = new GlobalSecondaryIndex()
					.withIndexName(gsi.getIndexName())
					.withKeySchema(gsi.getKeySchema())
					.withProjection(gsi.getProjection())
					.withProvisionedThroughput(ptMap.get(indexName));
			final String hk = copy.getKeySchema().get(0).getAttributeName();
			final String rk =
					copy.getKeySchema().size() == 2 ? copy.getKeySchema().get(1).getAttributeName() : null;
			this.gsis.put(indexName, copy);
			this.gsiHashKeys.put(indexName, hk);
			if (rk != null) {
				lookupKeyConditions.put(indexName,
						String.format(Locale.ENGLISH, "%s = :%s and %s = :%s", hk, hk, rk, rk));
				this.gsiRangeKeys.put(indexName, rk);
			} else {
				lookupKeyConditions.put(indexName, String.format(Locale.ENGLISH, "%s = :%s", hk, hk));
			}
		});

		Preconditions.checkNotNull(baseTableKeyNames);
		Preconditions.checkArgument(false == baseTableKeyNames.isEmpty(), "need at least one key");
		Preconditions.checkArgument(baseTableKeyNames.size() <= 2,
				"cant have more than two keys (one partition and one sort key)");

		//add the attribute definitions for the base table key schema
		baseTableKeyNames.stream().forEach(name -> Preconditions.checkArgument(this.definitions.containsKey(name)));
		this.schemata = Lists.newArrayList(new KeySchemaElement(baseTableKeyNames.get(0), KeyType.HASH));
		if (baseTableKeyNames.size() == 2) {
			schemata.add(new KeySchemaElement(baseTableKeyNames.get(1), KeyType.RANGE));
		}
		hashKeyName = schemata.get(0).getAttributeName();
		rangeKeyName = schemata.size() == 2 ? schemata.get(1).getAttributeName() : null;
		conditionalCreateCondition = rangeKeyName != null
				? String.format(Locale.ENGLISH, "attribute_not_exists(%s) and attribute_not_exists(%s)", hashKeyName,
				rangeKeyName)
				: String.format(Locale.ENGLISH, "attribute_not_exists(%s)", hashKeyName);
		conditionalDeleteCondition = rangeKeyName != null
				? String.format(Locale.ENGLISH, "attribute_exists(%s) and attribute_exists(%s)", hashKeyName,
				rangeKeyName)
				: String.format(Locale.ENGLISH, "attribute_exists(%s)", hashKeyName);
	}

	@Override
	public void afterPropertiesSet() {
		open();
	}

	private TableDescription updateTable(TableDescription desc) {
		Preconditions.checkNotNull(desc, "table description must not be null");
		UpdateTableSpec spec = null;
		if (false == ptMap.get(tableNameSuffix).equals(convert(desc.getProvisionedThroughput()))) {
			//if the throughput of the table is not the same as the throughput in the ptMap configuration,
			//update the thruput of the table
			spec = new UpdateTableSpec().withProvisionedThroughput(ptMap.get(tableNameSuffix));
		}
		final List<GlobalSecondaryIndexUpdate> gsiUpdates = new ArrayList<>();
		if (desc.getGlobalSecondaryIndexes() != null && false == desc.getGlobalSecondaryIndexes().isEmpty()) {
			//if the table description has updates to secondary indexes
			desc.getGlobalSecondaryIndexes().forEach(gsi -> {
				//for each gsi in the table description
				final String indexName = gsi.getIndexName();
				ProvisionedThroughput pt = ptMap.get(indexName);
				if (pt != null && false == pt.equals(convert(gsi.getProvisionedThroughput()))) {
					//if the throughput of the gsi in the description is not the same as the throughput in the pt map
					//add an update to the gsi's thruput
					gsiUpdates.add(new GlobalSecondaryIndexUpdate()
							.withUpdate(new UpdateGlobalSecondaryIndexAction()
									.withIndexName(indexName)
									.withProvisionedThroughput(pt)));
				}
			});
		}
		if (false == gsiUpdates.isEmpty()) {
			if (spec == null) {
				spec = new UpdateTableSpec();
			}
			spec.withGlobalSecondaryIndexUpdates(gsiUpdates);
		}
		return spec == null ? null : table.updateTable(spec);
	}

	private CreateTableRequest createTableRequest() {
		return new CreateTableRequest()
				.withTableName(table.getTableName())
				.withProvisionedThroughput(ptMap.get(tableNameSuffix))
				.withKeySchema(schemata)
				.withAttributeDefinitions(
						definitions.keySet().stream()
								.map(name -> new AttributeDefinition(name, definitions.get(name)))
								.collect(Collectors.toList()))
				.withGlobalSecondaryIndexes(gsis == null ? null : gsis.values());
	}

	public String tableName() {
		return table.getTableName();
	}

	@Override
	public E findOne(K keys) throws DataAccessException {
		Preconditions.checkNotNull(keys, "keys must not be null");
		//just read the item and return it
		final Item item;
		final PrimaryKey pk = createKeys(keys);
		GetItemSpec spec = new GetItemSpec().withPrimaryKey(pk);
		try {
			//TODO add projection expression for keys
			item = table.getItem(spec);
			return item == null ? null : convertItemToDomain(item);
		} catch (AmazonClientException e) {
			throw convertDynamoDBException(e, "read",
					null /* conditionMessage is null because GetItem doesnt take a condition */);
		}
	}

	@Override
	public boolean exists(K keys) {
		Preconditions.checkArgument(keys != null, "keys must not be null");
		return null != findOne(keys);
	}

	/**
	 * Translates low level database exceptions to application level exceptions
	 *
	 * @param e the low level amazon client exception
	 * @param action crud action name
	 * @param conditionalSupplier creates condition failed exception (context dependent)
	 * @return a translation of the AWS/DynamoDB exception
	 */
	DataAccessException convertDynamoDBException(AmazonClientException e,
												 String action,
												 Supplier<? extends DataAccessException> conditionalSupplier) {

		final String format = "unable to %s entity due to %s.";
		if (e instanceof ConditionalCheckFailedException) {
			return conditionalSupplier.get();
		} else if (e instanceof ProvisionedThroughputExceededException) {
			return new DynamoDbThrottleException(String.format(Locale.ENGLISH, format, action, "throttling"), e);
		} else if (e instanceof AmazonServiceException) {
			AmazonServiceException ase = (AmazonServiceException) e;
			if (VALIDATION_EXCEPTION.equals(((AmazonServiceException) e).getErrorCode())) {
				return new InvalidDataAccessResourceUsageException(String.format(Locale.ENGLISH, format, action,
						"client error"), e);
			} else {
				return new DynamoDbServiceException(
						String.format(Locale.ENGLISH, format, action, "DynamoDB service error"), ase);
			}
		} else {
			return new InvalidDataAccessResourceUsageException(String.format(Locale.ENGLISH, format, action,
					"client error"), e);
		}
	}

	private Chunk<Item> getItemListForGsi(String indexName, QuerySpec spec) {
		Preconditions.checkNotNull(spec, "spec must not be null");
		final ItemCollection<QueryOutcome> outcome = table.getIndex(indexName).query(spec);

		List<Item> results = new ArrayList<>();
		try {
			outcome.pages().forEach(p -> {
				p.iterator().forEachRemaining(o -> results.add(o));
			});
		} catch (AmazonServiceException e) {
			throw convertDynamoDBException(e, "getting by spec: " + spec.toString(),
					null /*no write condition exception*/);
		}
		Map<String, AttributeValue> lastEvaluatedKey =
				outcome.getLastLowLevelResult().getQueryResult().getLastEvaluatedKey();
		String lastEvaluatedItemJson = lastEvaluatedKey == null ? null
				: Item.fromMap(InternalUtils.toSimpleMapValue(lastEvaluatedKey)).toJSON();

		return new ChunkImpl<>(results, lastEvaluatedItemJson, null /*chunkable*/);
	}

	/**
	 * gets a full item from a GSI
	 * @param indexName name of GSI
	 * @param spec query spec for gsi query
	 * @return a full item keyed at keys in the GSI. If the GSI does not project all attributes, will read the item from
	 * the base table
	 *
	 * TODO, ideally, return a wrapper of the last evaluated key from the query result and the list, but since we are
	 * not filtering on the server side, do this later.
	 */
	private Chunk<E> getFromGSI(String indexName, QuerySpec spec, boolean isUnique) {
		Preconditions.checkNotNull(spec, "query spec was null");
		Chunk<Item> chunk = getItemListForGsi(indexName, spec);
		//check if the index was not a unique index
		if (isUnique) {
			Preconditions.checkState(chunk.getContent().size() < 2,
					"the index had more than one item at spec=" + spec.toString());
		}

		if (ProjectionType.fromValue(gsis.get(indexName).getProjection().getProjectionType()) == ProjectionType.ALL) {
			//the GSI had the full item so return it.
			return new ChunkImpl<>(chunk.getContent().parallelStream()
					.map(i -> convertItemToDomain(i)).collect(Collectors.toList()), chunk.getPaginationToken(),
					null /*chunkable*/);
		}
		//else read the item from the base table
		try {
			List<AttributeValue> pks = chunk.getContent().stream()
					.map(i -> getPrimaryKeyFromItem(i))
					.map(pk -> Iterables.getOnlyElement(pk.getComponents().stream()
							.filter(c -> c.getName().equals(hashKeyName))
							.map(Attribute::getValue)
							.map(InternalUtils::toAttributeValue)
							.collect(Collectors.toList())))
					.collect(Collectors.toList());
			return new ChunkImpl<>(findAll(pks, true), chunk.getPaginationToken(), null /*chunkable*/);
		} catch (AmazonClientException e) {
			throw convertDynamoDBException(e, "read", null /*condition failed exception provider*/);
		}
	}

	@Override
	public E getAndDelete(K key, long version) {
		Preconditions.checkNotNull(key, "keys must not be null");
		Preconditions.checkArgument(version >= -1L, "version must be greater than or equal to -1");
		final PrimaryKey pk = createKeys(key);
		final boolean conditioning = version >= 0;
		final String actualCondition;
		final Map<String, Object> valueMap;
		final Map<String, String> nameMap;

		if (conditioning) {
			actualCondition = String.format(Locale.ENGLISH, "%s and #version = :v", conditionalDeleteCondition);
			valueMap = Collections.singletonMap(":v", version);
			nameMap = Collections.singletonMap("#version", VERSION);
		} else {
			actualCondition = conditionalDeleteCondition;
			valueMap = null;
			nameMap = null;
		}
		final DeleteItemSpec spec = new DeleteItemSpec().withPrimaryKey(pk).withNameMap(nameMap).withValueMap(valueMap)
				.withConditionExpression(actualCondition).withReturnValues(ReturnValue.ALL_OLD);
		final Item item;
		try {
			item = table.deleteItem(spec).getItem();
		} catch (AmazonClientException e) {
			throw convertDynamoDBException(e, "delete",
					() -> convertConditionalCheckFailedExceptionForDelete(e, version, key));
		}
		return convertItemToDomain(item);
	}

	private DataAccessException convertConditionalCheckFailedExceptionForDelete(AmazonClientException e,
																				long version, K key) {
		if (version == -1 || false == exists(key)) {
			return getNotFoundException("didnt delete since entity didnt exist", e);
		}
		return new OptimisticLockingFailureException("did not delete entity because of version mismatch", e);
	}

	@Override
	public Iterable<E> findAll(Iterable<K> ids) {
		Preconditions.checkNotNull(ids, "ids may not be null");
		List<AttributeValue> idList = Lists.newArrayList(ids).parallelStream()
				.map(DynamoDbInternalUtils::toAttributeValue)
				.collect(Collectors.toList());
		return findAll(idList, true /*useParallelBatches*/);
	}

	private List<E> findAll(Iterable<AttributeValue> ids, boolean useParallelBatches) {
		Preconditions.checkNotNull(ids, "ids may not be null");
		List<AttributeValue> idList = Lists.newArrayList(ids);
		if (idList.isEmpty()) {
			return new ArrayList<>();
		}
		List<Map<String, AttributeValue>> resultantItems = new ArrayList<>();

		StreamSupport.stream(Iterables.partition(idList, 25).spliterator(), useParallelBatches).forEach(inner -> {
			BatchGetItemRequest req = new BatchGetItemRequest();
			KeysAndAttributes keysAndAttributes = new KeysAndAttributes();
			keysAndAttributes.setConsistentRead(true);
			keysAndAttributes.setKeys(inner.stream()
					.map(id -> ImmutableMap.of(hashKeyName, id))
					.collect(Collectors.toList()));
			String tableName = tableName();
			req.withRequestItems(ImmutableMap.of(tableName, keysAndAttributes));

			BatchGetItemResult result;

			do {
				try {
					result = dynamoDB.batchGetItem(req);
					resultantItems.addAll(result.getResponses().get(tableName));
					req.setRequestItems(result.getUnprocessedKeys());
				} catch (AmazonClientException e) {
					throw this.convertDynamoDBException(e, "batch get", null /*no conditions for reads*/);
				}
			} while (false == result.getUnprocessedKeys().isEmpty());
		});

		return resultantItems.stream()
				.map(legacyItem -> Item.fromMap(InternalUtils.toSimpleMapValue(legacyItem)))
				.map(item -> convertItemToDomain(item))
				.collect(Collectors.toList());
	}

	private PrimaryKey getPrimaryKeyFromItem(Item item) {
		KeyAttribute[] keyAttributes = schemata.stream().map(keySchemaElement -> {
			final String attrName = keySchemaElement.getAttributeName();
			Preconditions.checkArgument(item.hasAttribute(attrName),
					"must provide keys with " + attrName + " field set");
			return new KeyAttribute(attrName, item.get(attrName));
		}).toArray(KeyAttribute[]::new);
		return new PrimaryKey(keyAttributes);
	}

	@Override
	public E update(K key, JsonPatch patch, boolean increment, long version) {
		final PrimaryKey pk = createKeys(key);
		Preconditions.checkNotNull(patch, "patch must not be null");
		Preconditions.checkArgument(version >= -1);

		ExpressionSpecBuilder builder = patch.get();

		//add a condition on item existence
		builder.withCondition(ExpressionSpecBuilder.attribute_exists(hashKeyName));
		//add update expression for incrementing the version
		if (increment) {
			builder.addUpdate(ExpressionSpecBuilder.N(VERSION)
					.set(ExpressionSpecBuilder.N(VERSION).plus(1L)));
		}
		//add version condition
		if (version >= 0) {
			builder.withCondition(ExpressionSpecBuilder.N(VERSION).eq(version));
		}

		UpdateItemExpressionSpec spec = builder.buildForUpdate();
		Preconditions.checkArgument(false == Strings.isNullOrEmpty(spec.getUpdateExpression()),
				"patch may not be empty"); // TODO add mechanism to JSON patch to allow iterating over list of ops
		try {
			UpdateItemOutcome updateItemOutcome = table.updateItem(new UpdateItemSpec()
					.withExpressionSpec(spec)
					.withPrimaryKey(pk)
					.withReturnValues(ReturnValue.ALL_NEW));
			return convertItemToDomain(updateItemOutcome.getItem());
		} catch (AmazonClientException e) {
			throw processUpdateItemException(key, e);
		}
	}

	protected DataAccessException processUpdateItemException(K key, AmazonClientException e) {
		final String format = "unable to update entity due to %s.";
		if (e instanceof ConditionalCheckFailedException) {
			if (null == findOne(key)) {
				return getNotFoundException(UPDATE_FAILED_ENTITY_NOT_FOUND, e);
			}
			return new OptimisticLockingFailureException(UPDATE_FAILED_NOT_FOUND_OR_BAD_VERSION, e);
		} else if (e instanceof ProvisionedThroughputExceededException) {
			throw new DynamoDbThrottleException(String.format(Locale.ENGLISH, format, "throttling"), e);
		} else if (e instanceof AmazonServiceException) {
			AmazonServiceException ase = (AmazonServiceException) e;
			if (VALIDATION_EXCEPTION.equals(ase.getErrorCode())) {
				if (EXPRESSION_REFERS_TO_NON_EXTANT_ATTRIBUTE.equals(ase.getErrorMessage()) && null == findOne(key)) {
					// if no locking and we get a specific message, then it also means the item does not exist
					return getNotFoundException(UPDATE_FAILED_ENTITY_NOT_FOUND, e);
				}
				return new InvalidDataAccessResourceUsageException(
						String.format(Locale.ENGLISH, format, "client error"), e);
			} else {
				return new DynamoDbServiceException(
						String.format(Locale.ENGLISH, format, "DynamoDB service error"), ase);
			}
		} else {
			return new InvalidDataAccessResourceUsageException(String.format(Locale.ENGLISH, format, "client error"),
					e);
		}
	}

	private <T> String convertDomainToJSON(T domain) {
		try {
			return objectMapper.writeValueAsString(domain);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("unable to convert domain object to JSON", e);
		}
	}

	/**
	 * converts a Jackson annotated domain object to a DynamoDB document (item)
	 * @param domain the object to convert
	 * @return a DynamoDB Document representation of the domain object
	 */
	<T> Item convertDomainToItem(T domain) {
		return Item.fromJSON(convertDomainToJSON(domain));
	}

	protected E convertItemToDomain(Item item) {
		return convertItemToDomain(item, clazz);
	}

	protected E findOneByGsi(String gsiName, QuerySpec spec) {
		Chunk<E> chunk = getFromGSI(gsiName, spec, true /*isUnique*/);
		return Optional.ofNullable(chunk.getContent().isEmpty() ? null : Iterables.getOnlyElement(chunk.getContent()))
				.orElseThrow(() -> new IncorrectResultSizeDataAccessException(
						"could not find one matching record for spec:" + spec.toString(), 1 /*expected*/,
						0 /*actual*/));
	}

	protected Chunk<E> findAllByGsi(String gsiName, QuerySpec spec) {
		return getFromGSI(gsiName, spec, false /*isUnique*/);
	}

	<S extends E> S convertItemToDomain(Item item, Class<? extends S> crass) {
		try {
			if (item == null) {
				return null;
			}
			String json = item.toJSON();
			return objectMapper.readValue(json, crass);
		} catch (IOException e) {
			throw new IllegalStateException("unable to convert orders JSON to domain object", e);
		}
	}

	@Override
	public Chunk<E> findAll(Chunkable chunkable) {
		Preconditions.checkNotNull(chunkable);
		Preconditions.checkArgument(Sort.Direction.DESC != chunkable.getDirection(),
				"DynamoDB only supports scanning forwards");
		ScanSpec spec = new ScanSpec();
		if (false == Strings.isNullOrEmpty(chunkable.getPaginationToken())) {
			spec.withExclusiveStartKey(new PrimaryKey(hashKeyName, chunkable.getPaginationToken()));
		}
		spec.withMaxPageSize(chunkable.getMaxPageSize()).withMaxResultSize(chunkable.getMaxPageSize());

		final ItemCollection<ScanOutcome> results;

		try {
			results = table.scan(spec);
		} catch (AmazonClientException e) {
			throw convertDynamoDBException(e, "scan", null /* conditionMessage */);
		}

		final List<Item> itemList = Lists.newArrayList(results.iterator());
		final List<E> entities = itemList.stream()
				.map(this::convertItemToDomain) //O(n)
				.collect(Collectors.toList()); //O(n)
		final Map<String, AttributeValue> lastEvaluatedKey = results.getLastLowLevelResult() == null
				? null : results.getLastLowLevelResult().getScanResult().getLastEvaluatedKey();
		final String paginationToken = lastEvaluatedKey == null ? null : lastEvaluatedKey.get(hashKeyName).getS();
		return new ChunkImpl<>(entities, paginationToken, chunkable);
	}

	private PrimaryKey createKeys(K key) {
		return createKeys(hashKeyName, definitions.get(hashKeyName), key);
	}

	private static <K extends Serializable> PrimaryKey createKeys(String hashKeyName,
																  ScalarAttributeType hashKeyType,
																  K key) {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(hashKeyType == scalarAttributeType(key));

		//hash key only
		return new PrimaryKey().addComponent(hashKeyName, key);
	}

	private static ScalarAttributeType scalarAttributeType(Object o) {
		if (o instanceof String) {
			return ScalarAttributeType.S;
		}
		if (o instanceof ByteBuffer) {
			return ScalarAttributeType.B;
		}
		if (o instanceof Number) {
			return ScalarAttributeType.N;
		}
		throw new IllegalArgumentException("Unsupported scalar type");
	}

	PutItemSpec putItemSpec(Item domainItem) {
		return new PutItemSpec()
				.withItem(domainItem)
				.withConditionExpression(conditionalCreateCondition);
	}

	@Override
	public void delete(E entity) {
		Preconditions.checkArgument(entity != null, "cannot delete null entity");
		delete(getId(entity));
	}

	@Override
	public void delete(K keys) {
		//not throwing NPE as per DeletableRepository spec (says IAE for null)
		Preconditions.checkArgument(keys != null, "keys may not be null");
		getAndDelete(keys, -1 /* version->not conditioning */);
	}

	public void open() {
		TableDescription desc = null;
		boolean tableNotFound = false;
		try {
			desc = table.describe();
		} catch (ResourceNotFoundException rnfe) {
			tableNotFound = true;
		}
		if (tableNotFound) {
			log.info("creating {}", table.getTableName());
			CreateTableRequest ctr = createTableRequest();
			try {
				dynamoDB.createTable(ctr);
			} catch (AmazonClientException e) {
				log.error(ctr.toString());
				throw convertDynamoDBException(e, "CreateTable", null /* conditionMessage */);
			}
			try {
				table.waitForActive();
				log.info("created {}", table.getTableName());
			} catch (InterruptedException ie) {
				throw new IllegalStateException("Unable to create table " + table.getTableName(), ie);
			}
		}
		log.debug("{} exists", table.getTableName());
		if (desc != null) { //if the table is not a newly created table
			if (null != updateTable(desc)) { //if there are updates
				log.info("updating {}", table.getTableName());
				try {
					table.waitForActive();
					log.info("updated {}", table.getTableName());
				} catch (InterruptedException e) {
					throw new IllegalStateException("Unable to patch table " + table.getTableName(), e);
				}
			}
		}
	}

	@Override
	public <S extends E> S update(S domain, VersionCondition condition) {
		Preconditions.checkNotNull(domain, "domain must not be null");
		final Item domainItem = convertDomainToItem(domain);
		Preconditions.checkArgument(domainItem.hasAttribute(hashKeyName),
				"hash key must be set in domain object when updating: " + hashKeyName);

		ExpressionSpecBuilder builder = new ExpressionSpecBuilder();
		builder.withCondition(ExpressionSpecBuilder.S(hashKeyName).exists());
		if (condition != null) {
			builder.withCondition(ExpressionSpecBuilder.N(VERSION).eq(condition.getVersion()));
		}
		PutItemExpressionSpec xSpec = builder.buildForPut();
		PutItemSpec spec = new PutItemSpec().withItem(domainItem).withExpressionSpec(xSpec);
		try {
			table.putItem(spec);
		} catch (AmazonClientException e) {
			throw processUpdateItemException(getId(domain), e);
		}
		// PutItem does not accept ReturnValue.ALL_NEW
		return domain;
	}

	private IncorrectResultSizeDataAccessException getNotFoundException(String msg, Throwable e) {
		return new IncorrectResultSizeDataAccessException(msg, 1 /*expected*/, e);
	}

	@Override
	public <S extends E> S update(S domain) {
		return update(domain, null);
	}

	@Override
	public <S extends E> S create(S domain) {
		Preconditions.checkNotNull(domain, "domain must not be null");
		final Item domainItem = convertDomainToItem(domain);

		// stackoverflow.com/questions/4460580/java-generics-why-someobject-getclass-doesnt-return-class-extends-t
		@SuppressWarnings("unchecked")
		final Class<? extends S> domainClass = (Class<? extends S>) domain.getClass();
		Item itemCreated = DynamoDbInternalUtils.cloneItem(domainItem, true /*filterEmptyStrings*/);
		PutItemSpec spec = putItemSpec(itemCreated);
		try {
			table.putItem(spec);
		} catch (AmazonClientException e) {
			throw convertDynamoDBException(e, "create",
					() -> new DuplicateKeyException("uuid " + getId(domain) + " already exists", e));
		}
		return convertItemToDomain(itemCreated, domainClass);
	}

	@Override
	public void deleteAll() {
		try {
			table.delete();
			open();
		} catch (AmazonClientException e) {
			throw convertDynamoDBException(e, "delete table", null /*no condition supplier*/);
		}
	}
}
