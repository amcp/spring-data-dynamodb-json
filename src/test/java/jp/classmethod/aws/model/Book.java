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
package jp.classmethod.aws.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jp.classmethod.aws.dynamodb.UuidGenerator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * A Book domain model class to use for testing the library
 *
 * @author Alexander Patrikalakis
 * @since #version#
 */
@Getter
@Accessors(chain = true)
@ToString
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@AllArgsConstructor
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
		this.version = 0L;
	}
}
