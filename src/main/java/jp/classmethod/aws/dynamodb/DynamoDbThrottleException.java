package jp.classmethod.aws.dynamodb;

/**
 * @author Alexander Patrikalakis
 * @since #version#
 *
 * This exception gets thrown if dynamodb detects a throttle (retriable - transient)
 */
@SuppressWarnings("serial")
public class DynamoDbThrottleException extends DynamoDbServiceException {
	public DynamoDbThrottleException(String msg, Throwable e) {
		super(msg, e);
	}
}
