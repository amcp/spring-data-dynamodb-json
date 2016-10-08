package jp.classmethod.aws.dynamodb;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * Created by alexp on 10/9/16.
 */
@Getter
@Accessors(chain = true)
@ToString
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Book {
	public static final String BOOK_ID = "book_id";

	@Setter(AccessLevel.PACKAGE)
	@JsonProperty(BOOK_ID)
	private String bookId;

	@Setter
	@JsonProperty("name")
	private String name;

	@Setter(AccessLevel.PACKAGE)
	@JsonProperty("version")
	private Long version;

	public Book(String name) {
		this.bookId = UuidGenerator.generateModelId().toString();
		this.name = name;
	}
}
