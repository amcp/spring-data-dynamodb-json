package jp.classmethod.aws.dynamodb;

import org.springframework.dao.TransientDataAccessResourceException;

/**
 * @author Alexander Patrikalakis
 * @since #version#
 *
 * This exception gets thrown if dynamodb detects a service error (retriable - transient)
 */
@SuppressWarnings("serial")
public class DynamoDbServiceException extends TransientDataAccessResourceException {
	public DynamoDbServiceException(String msg, Throwable e) {
		super(msg, e);
	}
}
