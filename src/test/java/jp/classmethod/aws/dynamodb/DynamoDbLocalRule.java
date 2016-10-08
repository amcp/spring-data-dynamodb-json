package jp.classmethod.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import lombok.Getter;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * JUnit {@link TestRule} to start DynamoDBLocal before test.
 *
 * @author daisuke
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
