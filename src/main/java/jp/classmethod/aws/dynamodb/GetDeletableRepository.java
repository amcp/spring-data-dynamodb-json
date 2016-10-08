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

import jp.xet.sparwings.spring.data.repository.BaseRepository;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.repository.NoRepositoryBean;

import java.io.Serializable;

/**
 * This repo defines the interface of deleting an entity with a version condition
 * @param <E> the entity type
 * @param <ID> the key type
 */
@NoRepositoryBean
public interface GetDeletableRepository<E, ID extends Serializable>extends BaseRepository<E, ID> {

	/**
	 * Deletes an entity with a condition on the present version
	 *
	 * @param keys    the unique keys of the entity to remove
	 * @param version version parameter. The delete operation should fail if the version in the database
	 *                does not match the version provided. If version is -1, no conditioning occurs.
	 * @return the pre-image of the entity record prior to deletion
	 * @throws IncorrectResultSizeDataAccessException  if the keys provided do not correspond with a record in the
	 * database
	 * @throws OptimisticLockingFailureException if the provided version does not match
	 * @throws QueryTimeoutException if the database throttles (refuses to service) the delete request
	 * @throws NullPointerException if keys is null
	 * @throws IllegalArgumentException if smaller than -1
	 */
	E getAndDelete(ID keys, long version) throws OptimisticLockingFailureException, QueryTimeoutException,
			IncorrectResultSizeDataAccessException;
}
