package jp.classmethod.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by alexp on 10/9/16.
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
								  @Qualifier("objectMapperForDynamoDb") ObjectMapper objectMapper) {
		super(null /*prefix*/, TABLE_NAME, amazonDynamoDB, ImmutableMap.of(TABLE_NAME, throughput),
				objectMapper, Book.class, ATTRIBUTE_TYPE_MAP, Collections.singletonList(Book.BOOK_ID),
				null /*gsi list*/);
	}

	@Override
	public String getId(Book book) {
		return book.getBookId();
	}
}
