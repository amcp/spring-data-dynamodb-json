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

import com.github.fge.jsonpatch.JsonPatch;
import jp.xet.sparwings.spring.data.repository.BaseRepository;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.repository.NoRepositoryBean;

import java.io.Serializable;

/**
 * This repo defines the interface of applying a JsonPatch to an entity in a repository with a version condition
 * @param <E> the entity type
 * @param <ID> the key type
 */
@NoRepositoryBean
public interface PatchableRepository<E, ID extends Serializable>extends BaseRepository<E, ID> {
	/**
	 * Updates an entity given a patch object with a condition on the present version
	 *
	 * @param keys a holder for the unique keys of the entity object to be updated
	 * @param patch  JsonPatch object representing the changes to the entity specified by the keys in entity
	 * @param increment if true, increment the version of the entity by one
	 * @param version optional version parameter. The patch operation should fail if the version in the database
	 *                does not match the version provided
	 * @return the post image of the entity object, after the changes in patch are applied.
	 * @throws IncorrectResultSizeDataAccessException if the entity object represented by the unique keys does not exist
	 * @throws OptimisticLockingFailureException if it exists and the version does not match the version provided
	 * @throws QueryTimeoutException if the storage backend throttled the request
	 * @throws DataAccessException if any other access error encountered
	 */
	E update(ID keys, JsonPatch patch, boolean increment, long version) throws IncorrectResultSizeDataAccessException,
			OptimisticLockingFailureException, QueryTimeoutException;
}
