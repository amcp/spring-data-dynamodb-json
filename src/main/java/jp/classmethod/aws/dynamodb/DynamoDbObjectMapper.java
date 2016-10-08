package jp.classmethod.aws.dynamodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.xebia.jacksonlombok.JacksonLombokAnnotationIntrospector;

/**
 * {@link ObjectMapper} configured for metropolis project.
 *
 * <ul>
 *   <li>Support Java 8Language specs (Optional, etc.)</li>
 *   <li>Support lombok</li>
 * </ul>
 *
 * @author daisuke
 * @since #version#
 */
@SuppressWarnings("serial")
public class DynamoDbObjectMapper extends ObjectMapper {
	/**
	 * Create instance.
	 *
	 * @since #version#
	 */
	public DynamoDbObjectMapper() {
		registerModule(new Jdk8Module());
		setAnnotationIntrospector(new JacksonLombokAnnotationIntrospector());
	}
}
