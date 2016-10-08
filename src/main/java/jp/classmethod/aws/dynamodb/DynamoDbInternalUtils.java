/*
 * Copyright 2016 Classmethod, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright Amazon.com, Inc. or its affiliates.
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
package jp.classmethod.aws.dynamodb; //NOPMD - contains god class

import static com.amazonaws.util.BinaryUtils.copyAllBytesFrom;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.BinaryUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

/**
 * Utilities that were copied from the DynamoDB SDK or belong in the DynamoDB SDK
 *
 * @since #version#
 */
public class DynamoDbInternalUtils { //NOPMD - cyclomatic complexity high

	/**
	 * Clones an item, optionally ignoring string attributes that are empty strings
	 * @param item
	 * @param filterEmptyStrings
	 * @return
	 */
	static Item cloneItem(Item item, boolean filterEmptyStrings) {
		Map<String, AttributeValue> raw = toAttributeValues(item);
		return Item.fromMap(toSimpleMapValue(clone(raw, filterEmptyStrings)));
	}

	private static Map<String, AttributeValue> clone(Map<String, AttributeValue> item, boolean filterEmptyStrings) {
		if (item == null) {
			return null;
		}
		Map<String, AttributeValue> clonedItem = Maps.newHashMap();
		IdentityHashMap<AttributeValue, AttributeValue> sourceDestinationMap = new IdentityHashMap<>();

		for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
			if (false == sourceDestinationMap.containsKey(entry.getValue())) {
				sourceDestinationMap.put(entry.getValue(),
						clone(entry.getValue(), sourceDestinationMap, filterEmptyStrings));
			}
			if (false == (filterEmptyStrings && sourceDestinationMap.get(entry.getValue()).getS() != null
					&& sourceDestinationMap.get(entry.getValue()).getS().isEmpty())) {
				clonedItem.put(entry.getKey(), sourceDestinationMap.get(entry.getValue()));
			}
		}
		return clonedItem;
	}

	private static AttributeValue clone(AttributeValue val, //NOPMD
										IdentityHashMap<AttributeValue, AttributeValue> sourceDestinationMap,
										boolean filterEmptyStrings) {
		if (val == null) {
			return null;
		}

		if (sourceDestinationMap.containsKey(val)) {
			return sourceDestinationMap.get(val);
		}

		AttributeValue clonedVal = new AttributeValue();
		sourceDestinationMap.put(val, clonedVal);
		if (val.getN() != null) {
			clonedVal.setN(val.getN());
		} else if (val.getS() != null) {
			clonedVal.setS(val.getS());
		} else if (val.getB() != null) {
			clonedVal.setB(val.getB());
		} else if (val.getNS() != null) {
			clonedVal.setNS(val.getNS());
		} else if (val.getSS() != null) {
			clonedVal.setSS(val.getSS());
		} else if (val.getBS() != null) {
			clonedVal.setBS(val.getBS());
		} else if (val.getBOOL() != null) {
			clonedVal.setBOOL(val.getBOOL());
		} else if (val.getNULL() != null) {
			clonedVal.setNULL(val.getNULL());
		} else if (val.getL() != null) {
			List<AttributeValue> list = new ArrayList<>(val.getL().size());
			for (AttributeValue listItemValue : val.getL()) {
				if (!sourceDestinationMap.containsKey(listItemValue)) {
					sourceDestinationMap.put(listItemValue, clone(listItemValue, sourceDestinationMap,
							filterEmptyStrings));
				}
				AttributeValue destination = sourceDestinationMap.get(listItemValue);
				if (false == (filterEmptyStrings && destination.getS() != null && destination.getS().isEmpty())) {
					list.add(destination);
				}
			}
			clonedVal.setL(list);
		} else if (val.getM() != null) {
			Map<String, AttributeValue> map = new HashMap<>(val.getM().size());
			for (Map.Entry<String, AttributeValue> pair : val.getM().entrySet()) {
				if (!sourceDestinationMap.containsKey(pair.getValue())) {
					sourceDestinationMap.put(pair.getValue(), clone(pair.getValue(), sourceDestinationMap,
							filterEmptyStrings));
				}
				AttributeValue destination = sourceDestinationMap.get(pair.getValue());
				if (false == (filterEmptyStrings && destination.getS() != null && destination.getS().isEmpty())) {
					map.put(pair.getKey(), destination);
				}
			}
			clonedVal.setM(map);
		}
		return clonedVal;
	}

	/**
	 * Copied from DynamoDB Document SDK InternalUtils.java
	 *
	 * converts a number to a bigdecimal
	 */
	private static BigDecimal toBigDecimal(Number n) {
		if (n instanceof BigDecimal) {
			return (BigDecimal) n;
		}
		return new BigDecimal(n.toString());
	}

	/**
	 * Copied from DynamoDB Document SDK InternalUtils.java
	 *
	 * Converts an <code>Item</code> into the low-level representation;
	 * or null if the input is null.
	 */
	public static Map<String, AttributeValue> toAttributeValues(Item item) {
		if (item == null) {
			return null;
		}
		// row with multiple attributes
		Map<String, AttributeValue> result = new LinkedHashMap<String, AttributeValue>();
		for (Map.Entry<String, Object> entry : item.attributes()) {
			result.put(entry.getKey(), toAttributeValue(entry.getValue()));
		}
		return result;
	}

	/**
	 * Copied from DynamoDB Document SDK InternalUtils.java
	 *
	 * Converts a simple value into the low-level <code><AttributeValue/code>
	 * representation.
	 *
	 * @param value
	 *            the given value which can be one of the followings:
	 * <ul>
	 * <li>String</li>
	 * <li>Set&lt;String></li>
	 * <li>Number (including any subtypes and primitive types)</li>
	 * <li>Set&lt;Number></li>
	 * <li>byte[]</li>
	 * <li>Set&lt;byte[]></li>
	 * <li>ByteBuffer</li>
	 * <li>Set&lt;ByteBuffer></li>
	 * <li>Boolean or boolean</li>
	 * <li>null</li>
	 * <li>Map&lt;String,T>, where T can be any type on this list but must not
	 * induce any circular reference</li>
	 * <li>List&lt;T>, where T can be any type on this list but must not induce
	 * any circular reference</li>
	 * </ul>
	 * @return a non-null low level representation of the input object value
	 *
	 * @throws UnsupportedOperationException
	 *             if the input object type is not supported
	 */
	public static AttributeValue toAttributeValue(Object value) { //NOPMD
		AttributeValue result = new AttributeValue();
		if (value == null) {
			return result.withNULL(Boolean.TRUE);
		} else if (value instanceof Boolean) {
			return result.withBOOL((Boolean) value);
		} else if (value instanceof String) {
			return result.withS((String) value);
		} else if (value instanceof BigDecimal || value instanceof Number) {
			return result.withN(value instanceof Number ? value.toString() : ((BigDecimal) value).toPlainString());
		} else if (value instanceof byte[]) {
			return result.withB(ByteBuffer.wrap((byte[]) value));
		} else if (value instanceof ByteBuffer) {
			return result.withB((ByteBuffer) value);
		} else if (value instanceof Set) {
			// default to an empty string set if there is no element
			@SuppressWarnings("unchecked")
			Set<Object> set = (Set<Object>) value;
			if (set.isEmpty()) {
				result.setSS(new LinkedHashSet<>());
				return result;
			}
			Object element = set.iterator().next();
			if (element instanceof String) {
				@SuppressWarnings("unchecked")
				Set<String> ss = (Set<String>) value;
				result.setSS(new ArrayList<>(ss));
			} else if (element instanceof Number) {
				@SuppressWarnings("unchecked")
				Set<Number> in = (Set<Number>) value;
				List<String> out = new ArrayList<>(set.size());
				for (Number n : in) {
					BigDecimal bd = toBigDecimal(n);
					out.add(bd.toPlainString());
				}
				result.setNS(out);
			} else if (element instanceof byte[]) {
				@SuppressWarnings("unchecked")
				Set<byte[]> in = (Set<byte[]>) value;
				List<ByteBuffer> out = new ArrayList<>(set.size());
				for (byte[] buf : in) {
					out.add(ByteBuffer.wrap(buf));
				}
				result.setBS(out);
			} else if (element instanceof ByteBuffer) {
				@SuppressWarnings("unchecked")
				Set<ByteBuffer> bs = (Set<ByteBuffer>) value;
				result.setBS(bs);
			} else {
				throw new UnsupportedOperationException("element type: "
						+ element.getClass());
			}
		} else if (value instanceof List) {
			@SuppressWarnings("unchecked")
			List<Object> in = (List<Object>) value;
			List<AttributeValue> out = new ArrayList<>();
			for (Object v : in) {
				out.add(toAttributeValue(v));
			}
			result.setL(out);
		} else if (value instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> in = (Map<String, Object>) value;
			if (false == in.isEmpty()) {
				for (Map.Entry<String, Object> e : in.entrySet()) {
					result.addMEntry(e.getKey(), toAttributeValue(e.getValue()));
				}
			} else {    // empty map
				result.setM(new LinkedHashMap<>());
			}
		} else {
			throw new UnsupportedOperationException("value type: "
					+ value.getClass());
		}
		return result;
	}

	/**
	 * Copied from DynamoDB Document SDK InternalUtils.java
	 *
	 * @param values primitive item to convert
	 * @param <T> the type the input maps to
	 * @return a simple map.
	 */
	private static <T> Map<String, T> toSimpleMapValue(
			Map<String, AttributeValue> values) {
		if (values == null) {
			return null;
		}

		Map<String, T> result = new LinkedHashMap<String, T>(values.size());
		for (Map.Entry<String, AttributeValue> entry : values.entrySet()) {
			T t = toSimpleValue(entry.getValue());
			result.put(entry.getKey(), t);
		}
		return result;
	}

	/**
	 * Copied from DynamoDB Document SDK InternalUtils.java
	 *
	 * Converts a low-level <code>AttributeValue</code> into a simple value,
	 * which can be one of the followings:
	 *
	 * <ul>
	 * <li>String</li>
	 * <li>Set&lt;String></li>
	 * <li>Number (including any subtypes and primitive types)</li>
	 * <li>Set&lt;Number></li>
	 * <li>byte[]</li>
	 * <li>Set&lt;byte[]></li>
	 * <li>ByteBuffer</li>
	 * <li>Set&lt;ByteBuffer></li>
	 * <li>Boolean or boolean</li>
	 * <li>null</li>
	 * <li>Map&lt;String,T>, where T can be any type on this list but must not
	 * induce any circular reference</li>
	 * <li>List&lt;T>, where T can be any type on this list but must not induce
	 * any circular reference</li>
	 * </ul>
	 *
	 * @throws IllegalArgumentException
	 *             if an empty <code>AttributeValue</code> value is specified
	 */
	@SuppressWarnings("unchecked")
	private static <T> T toSimpleValue(AttributeValue value) { //NOPMD
		if (value == null) {
			return null;
		}

		if (Boolean.FALSE.equals(value.getNULL())) {
			throw new UnsupportedOperationException("False-NULL is not supported in DynamoDB");
		}
		final T t;
		if (Boolean.TRUE.equals(value.getNULL())) {
			t = null;
		} else if (value.getBOOL() != null) {
			t = (T) value.getBOOL();
		} else if (value.getS() != null) {
			t = (T) value.getS();
		} else if (value.getN() != null) {
			t = (T) new BigDecimal(value.getN());
		} else if (value.getB() != null) {
			t = (T) copyAllBytesFrom(value.getB());
		} else if (value.getSS() != null) {
			t = (T) new LinkedHashSet<>(value.getSS());
		} else if (value.getNS() != null) {
			Set<BigDecimal> set = new LinkedHashSet<>(value.getNS().size());
			set.addAll(value.getNS().stream().map(BigDecimal::new).collect(Collectors.toList()));
			t = (T) set;
		} else if (value.getBS() != null) {
			Set<byte[]> set = new LinkedHashSet<>(value.getBS().size());
			set.addAll(value.getBS().stream().map(BinaryUtils::copyAllBytesFrom).collect(Collectors.toList()));
			t = (T) set;
		} else if (value.getL() != null) {
			t = (T) toSimpleList(value.getL());
		} else if (value.getM() != null) {
			t = (T) toSimpleMapValue(value.getM());
		} else {
			throw new IllegalArgumentException("Attribute value must not be empty: " + value);
		}

		return t;
	}

	/**
	 * Copied from DynamoDB Document SDK InternalUtils.java
	 *
	 * Converts a list of low-level <code>AttributeValue</code> into a list of
	 * simple values. Each value in the returned list can be one of the
	 * followings:
	 *
	 * <ul>
	 * <li>String</li>
	 * <li>Set&lt;String></li>
	 * <li>Number (including any subtypes and primitive types)</li>
	 * <li>Set&lt;Number></li>
	 * <li>byte[]</li>
	 * <li>Set&lt;byte[]></li>
	 * <li>ByteBuffer</li>
	 * <li>Set&lt;ByteBuffer></li>
	 * <li>Boolean or boolean</li>
	 * <li>null</li>
	 * <li>Map&lt;String,T>, where T can be any type on this list but must not
	 * induce any circular reference</li>
	 * <li>List&lt;T>, where T can be any type on this list but must not induce
	 * any circular reference</li>
	 * </ul>
	 */
	private static List<Object> toSimpleList(List<AttributeValue> attrValues) {
		if (attrValues == null) {
			return null;
		}
		List<Object> result = new ArrayList<Object>(attrValues.size());
		for (AttributeValue attrValue : attrValues) {
			Object value = toSimpleValue(attrValue);
			result.add(value);
		}
		return result;
	}
}
