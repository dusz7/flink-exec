/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.functions.hive.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.hive.FlinkHiveUDFException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Util for any ObjectInspector related inspection and conversion of Hive data to/from Flink data.
 *
 * <p>Hive ObjectInspector is a group of flexible APIs to inspect value in different data representation,
 * and developers can extend those API as needed, so technically, object inspector supports arbitrary data type in java.
 */
@Internal
public class HiveInspectors {

	/**
	 * Get an array of ObjectInspector from the give array of args and their types.
	 */
	public static ObjectInspector[] toInspectors(Object[] args, DataType[] argTypes) {
		assert args.length == argTypes.length;

		ObjectInspector[] argumentInspectors = new ObjectInspector[argTypes.length];

		for (int i = 0; i < argTypes.length; i++) {
			Object constant = args[i];

			if (constant == null) {
				argumentInspectors[i] =
					TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
						HiveTypeUtil.toHiveTypeInfo(argTypes[i]));
			} else {
				PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) HiveTypeUtil.toHiveTypeInfo(argTypes[i]);
				constant = getConversion(getObjectInspector(primitiveTypeInfo), argTypes[i].getLogicalType())
						.toHiveObject(constant);
				argumentInspectors[i] =
					HiveInspectors.getObjectInspectorForPrimitiveConstant(
						primitiveTypeInfo,
						constant
					);
			}
		}

		return argumentInspectors;
	}

	private static ObjectInspector getObjectInspectorForPrimitiveConstant(PrimitiveTypeInfo primitiveTypeInfo, Object value) {
		String className;
		value = hivePrimitiveToWritable(value);
		switch (primitiveTypeInfo.getPrimitiveCategory()) {
			case BOOLEAN:
				className = WritableConstantBooleanObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case BYTE:
				className = WritableConstantByteObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case SHORT:
				className = WritableConstantShortObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case INT:
				className = WritableConstantIntObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case LONG:
				className = WritableConstantLongObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case FLOAT:
				className = WritableConstantFloatObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DOUBLE:
				className = WritableConstantDoubleObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case STRING:
				className = WritableConstantStringObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case CHAR:
				try {
					Constructor<WritableConstantHiveCharObjectInspector> constructor =
							WritableConstantHiveCharObjectInspector.class.getDeclaredConstructor(CharTypeInfo.class, value.getClass());
					constructor.setAccessible(true);
					return constructor.newInstance(primitiveTypeInfo, value);
				} catch (Exception e) {
					throw new FlinkHiveUDFException("Failed to create writable constant object inspector", e);
				}
			case VARCHAR:
				try {
					Constructor<WritableConstantHiveVarcharObjectInspector> constructor =
							WritableConstantHiveVarcharObjectInspector.class.getDeclaredConstructor(VarcharTypeInfo.class, value.getClass());
					constructor.setAccessible(true);
					return constructor.newInstance(primitiveTypeInfo, value);
				} catch (Exception e) {
					throw new FlinkHiveUDFException("Failed to create writable constant object inspector", e);
				}
			case DATE:
				className = WritableConstantDateObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case TIMESTAMP:
				className = WritableConstantTimestampObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DECIMAL:
				try {
					Constructor<WritableConstantHiveDecimalObjectInspector> constructor =
							WritableConstantHiveDecimalObjectInspector.class.getDeclaredConstructor(DecimalTypeInfo.class, value.getClass());
					constructor.setAccessible(true);
					return constructor.newInstance(primitiveTypeInfo, value);
				} catch (Exception e) {
					throw new FlinkHiveUDFException("Failed to create writable constant object inspector", e);
				}
			case BINARY:
				className = WritableConstantBinaryObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case UNKNOWN:
			case VOID:
				// If type is null, we use the Constant String to replace
				className = WritableConstantStringObjectInspector.class.getName();
				return HiveReflectionUtils.createConstantObjectInspector(className, value.toString());
			default:
				throw new FlinkHiveUDFException(
						String.format("Cannot find ConstantObjectInspector for %s", primitiveTypeInfo));
		}
	}

	static Writable hivePrimitiveToWritable(Object value) {
		if (value == null) {
			return null;
		}
		Writable writable;
		// in case value is already a Writable
		if (value instanceof Writable) {
			writable = (Writable) value;
		} else if (value instanceof Boolean) {
			writable = new BooleanWritable((Boolean) value);
		} else if (value instanceof Byte) {
			writable = new ByteWritable((Byte) value);
		} else if (value instanceof Short) {
			writable = new ShortWritable((Short) value);
		} else if (value instanceof Integer) {
			writable = new IntWritable((Integer) value);
		} else if (value instanceof Long) {
			writable = new LongWritable((Long) value);
		} else if (value instanceof Float) {
			writable = new FloatWritable((Float) value);
		} else if (value instanceof Double) {
			writable = new DoubleWritable((Double) value);
		} else if (value instanceof String) {
			writable = new Text((String) value);
		} else if (value instanceof HiveChar) {
			writable = new HiveCharWritable((HiveChar) value);
		} else if (value instanceof HiveVarchar) {
			writable = new HiveVarcharWritable((HiveVarchar) value);
		} else if (value instanceof HiveDecimal) {
			writable = new HiveDecimalWritable((HiveDecimal) value);
		} else if (value instanceof Date) {
			writable = new DateWritable((Date) value);
		} else if (value instanceof Timestamp) {
			writable = new TimestampWritable((Timestamp) value);
		} else if (value instanceof BigDecimal) {
			HiveDecimal hiveDecimal = HiveDecimal.create((BigDecimal) value);
			writable = new HiveDecimalWritable(hiveDecimal);
		} else if (value instanceof byte[]) {
			writable = new BytesWritable((byte[]) value);
		} else {
			throw new FlinkHiveException("Unsupported primitive java value of class " + value.getClass().getName());
		}
		return writable;
	}

	/**
	 * Get conversion for converting Flink object to Hive object from an ObjectInspector and the corresponding Flink DataType.
	 */
	public static HiveObjectConversion getConversion(ObjectInspector inspector, LogicalType dataType) {
		if (inspector instanceof PrimitiveObjectInspector) {
			HiveObjectConversion conversion;
			if (inspector instanceof BooleanObjectInspector ||
					inspector instanceof StringObjectInspector ||
					inspector instanceof ByteObjectInspector ||
					inspector instanceof ShortObjectInspector ||
					inspector instanceof IntObjectInspector ||
					inspector instanceof LongObjectInspector ||
					inspector instanceof FloatObjectInspector ||
					inspector instanceof DoubleObjectInspector ||
					inspector instanceof DateObjectInspector ||
					inspector instanceof TimestampObjectInspector ||
					inspector instanceof BinaryObjectInspector) {
				conversion = IdentityConversion.INSTANCE;
			} else if (inspector instanceof HiveCharObjectInspector) {
				conversion = o -> new HiveChar((String) o, ((CharType) dataType).getLength());
			} else if (inspector instanceof HiveVarcharObjectInspector) {
				conversion = o -> new HiveVarchar((String) o, ((VarCharType) dataType).getLength());
			} else if (inspector instanceof HiveDecimalObjectInspector) {
				conversion = o -> o == null ? null : HiveDecimal.create((BigDecimal) o);
			} else {
				throw new FlinkHiveUDFException("Unsupported primitive object inspector " + inspector.getClass().getName());
			}
			if (((PrimitiveObjectInspector) inspector).preferWritable()) {
				conversion = new WritableHiveObjectConversion(conversion);
			}
			return conversion;
		}

		if (inspector instanceof ListObjectInspector) {
			HiveObjectConversion eleConvert = getConversion(
				((ListObjectInspector) inspector).getListElementObjectInspector(),
				((ArrayType) dataType).getElementType());
			return o -> {
				Object[] array = (Object[]) o;
				List<Object> result = new ArrayList<>();

				for (Object ele : array) {
					result.add(eleConvert.toHiveObject(ele));
				}
				return result;
			};
		}

		if (inspector instanceof MapObjectInspector) {
			MapObjectInspector mapInspector = (MapObjectInspector) inspector;
			MapType kvType = (MapType) dataType;

			HiveObjectConversion keyConversion =
				getConversion(mapInspector.getMapKeyObjectInspector(), kvType.getKeyType());
			HiveObjectConversion valueConversion =
				getConversion(mapInspector.getMapValueObjectInspector(), kvType.getValueType());

			return o -> {
				Map<Object, Object> map = (Map) o;
				Map<Object, Object> result = new HashMap<>(map.size());

				for (Map.Entry<Object, Object> entry : map.entrySet()){
					result.put(
						keyConversion.toHiveObject(entry.getKey()),
						valueConversion.toHiveObject(entry.getValue()));
				}
				return result;
			};
		}

		if (inspector instanceof StructObjectInspector) {
			StructObjectInspector structInspector = (StructObjectInspector) inspector;

			List<? extends StructField> structFields = structInspector.getAllStructFieldRefs();

			List<RowType.RowField> rowFields = ((RowType) dataType).getFields();

			HiveObjectConversion[] conversions = new HiveObjectConversion[structFields.size()];
			for (int i = 0; i < structFields.size(); i++) {
				conversions[i] = getConversion(structFields.get(i).getFieldObjectInspector(), rowFields.get(i).getType());
			}

			return o -> {
				Row row = (Row) o;
				List<Object> result = new ArrayList<>(row.getArity());
				for (int i = 0; i < row.getArity(); i++) {
					result.add(conversions[i].toHiveObject(row.getField(i)));
				}
				return result;
			};
		}

		throw new FlinkHiveUDFException(
			String.format("Flink doesn't support convert object conversion for %s yet", inspector));
	}

	/**
	 * Converts a Hive object to Flink object with an ObjectInspector.
	 */
	public static Object toFlinkObject(ObjectInspector inspector, Object data) {
		if (data == null || inspector instanceof VoidObjectInspector) {
			return null;
		}

		if (inspector instanceof PrimitiveObjectInspector) {
			if (inspector instanceof BooleanObjectInspector ||
					inspector instanceof StringObjectInspector ||
					inspector instanceof ByteObjectInspector ||
					inspector instanceof ShortObjectInspector ||
					inspector instanceof IntObjectInspector ||
					inspector instanceof LongObjectInspector ||
					inspector instanceof FloatObjectInspector ||
					inspector instanceof DoubleObjectInspector ||
					inspector instanceof DateObjectInspector ||
					inspector instanceof TimestampObjectInspector ||
					inspector instanceof BinaryObjectInspector) {

				PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inspector;
				return poi.getPrimitiveJavaObject(data);
			} else if (inspector instanceof HiveCharObjectInspector) {
				HiveCharObjectInspector oi = (HiveCharObjectInspector) inspector;

				return oi.getPrimitiveJavaObject(data).getValue();
			} else if (inspector instanceof HiveVarcharObjectInspector) {
				HiveVarcharObjectInspector oi = (HiveVarcharObjectInspector) inspector;

				return oi.getPrimitiveJavaObject(data).getValue();
			} else if (inspector instanceof HiveDecimalObjectInspector) {
				HiveDecimalObjectInspector oi = (HiveDecimalObjectInspector) inspector;

				return oi.getPrimitiveJavaObject(data).bigDecimalValue();
			}
		}

		if (inspector instanceof ListObjectInspector) {
			ListObjectInspector listInspector = (ListObjectInspector) inspector;
			List<?> list = listInspector.getList(data);

			Object[] result = new Object[list.size()];
			for (int i = 0; i < list.size(); i++) {
				result[i] = toFlinkObject(listInspector.getListElementObjectInspector(), list.get(i));
			}
			return result;
		}

		if (inspector instanceof MapObjectInspector) {
			MapObjectInspector mapInspector = (MapObjectInspector) inspector;
			Map<?, ?> map = mapInspector.getMap(data);

			Map<Object, Object> result = new HashMap<>(map.size());
			for (Map.Entry<?, ?> entry : map.entrySet()){
				result.put(
					toFlinkObject(mapInspector.getMapKeyObjectInspector(), entry.getKey()),
					toFlinkObject(mapInspector.getMapValueObjectInspector(), entry.getValue()));
			}
			return result;
		}

		if (inspector instanceof StructObjectInspector) {
			StructObjectInspector structInspector = (StructObjectInspector) inspector;

			List<? extends StructField> fields = structInspector.getAllStructFieldRefs();

			Row row = new Row(fields.size());
			// StandardStructObjectInspector.getStructFieldData in Hive-1.2.1 only accepts array or list as data
			if (!data.getClass().isArray() && !(data instanceof List) && (inspector instanceof StandardStructObjectInspector)) {
				data = new Object[]{data};
			}
			for (int i = 0; i < row.getArity(); i++) {
				row.setField(
					i,
					toFlinkObject(
							fields.get(i).getFieldObjectInspector(),
							structInspector.getStructFieldData(data, fields.get(i)))
				);
			}
			return row;
		}

		throw new FlinkHiveUDFException(
			String.format("Unwrap does not support ObjectInspector '%s' yet", inspector));
	}

	public static ObjectInspector getObjectInspector(Class clazz) {
		TypeInfo typeInfo;

		if (clazz.equals(String.class) || clazz.equals(Text.class)) {

			typeInfo = TypeInfoFactory.stringTypeInfo;
		} else if (clazz.equals(Boolean.class) || clazz.equals(BooleanWritable.class)) {

			typeInfo = TypeInfoFactory.booleanTypeInfo;
		} else if (clazz.equals(Byte.class) || clazz.equals(ByteWritable.class)) {

			typeInfo = TypeInfoFactory.byteTypeInfo;
		} else if (clazz.equals(Short.class) || clazz.equals(ShortWritable.class)) {

			typeInfo = TypeInfoFactory.shortTypeInfo;
		} else if (clazz.equals(Integer.class) || clazz.equals(IntWritable.class)) {

			typeInfo = TypeInfoFactory.intTypeInfo;
		} else if (clazz.equals(Long.class) || clazz.equals(LongWritable.class)) {

			typeInfo = TypeInfoFactory.longTypeInfo;
		} else if (clazz.equals(Float.class) || clazz.equals(FloatWritable.class)) {

			typeInfo = TypeInfoFactory.floatTypeInfo;
		} else if (clazz.equals(Double.class) || clazz.equals(DoubleWritable.class)) {

			typeInfo = TypeInfoFactory.doubleTypeInfo;
		} else if (clazz.equals(Date.class) || clazz.equals(DateWritable.class)) {

			typeInfo = TypeInfoFactory.dateTypeInfo;
		} else if (clazz.equals(Timestamp.class) || clazz.equals(TimestampWritable.class)) {

			typeInfo = TypeInfoFactory.timestampTypeInfo;
		} else if (clazz.equals(byte[].class) || clazz.equals(BytesWritable.class)) {

			typeInfo = TypeInfoFactory.binaryTypeInfo;
		} else if (clazz.equals(HiveChar.class) || clazz.equals(HiveCharWritable.class)) {

			typeInfo = TypeInfoFactory.charTypeInfo;
		} else if (clazz.equals(HiveVarchar.class) || clazz.equals(HiveVarcharWritable.class)) {

			typeInfo = TypeInfoFactory.varcharTypeInfo;
		} else if (clazz.equals(HiveDecimal.class) || clazz.equals(HiveDecimalWritable.class)) {

			typeInfo = TypeInfoFactory.decimalTypeInfo;
		} else {
			throw new FlinkHiveUDFException(
				String.format("Class %s is not supported yet", clazz.getName()));
		}

		return getObjectInspector(typeInfo);
	}

	/**
	 * Get Hive {@link ObjectInspector} for a Flink {@link TypeInformation}.
	 */
	public static ObjectInspector getObjectInspector(DataType flinkType) {
		return getObjectInspector(HiveTypeUtil.toHiveTypeInfo(flinkType));
	}

	private static ObjectInspector getObjectInspector(TypeInfo type) {
		switch (type.getCategory()) {

			case PRIMITIVE:
				PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
				return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveType);
			case LIST:
				ListTypeInfo listType = (ListTypeInfo) type;
				return ObjectInspectorFactory.getStandardListObjectInspector(
						getObjectInspector(listType.getListElementTypeInfo()));
			case MAP:
				MapTypeInfo mapType = (MapTypeInfo) type;
				return ObjectInspectorFactory.getStandardMapObjectInspector(
						getObjectInspector(mapType.getMapKeyTypeInfo()), getObjectInspector(mapType.getMapValueTypeInfo()));
			case STRUCT:
				StructTypeInfo structType = (StructTypeInfo) type;
				List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();

				List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
				for (TypeInfo fieldType : fieldTypes) {
					fieldInspectors.add(getObjectInspector(fieldType));
				}

				return ObjectInspectorFactory.getStandardStructObjectInspector(
						structType.getAllStructFieldNames(), fieldInspectors);
			default:
				throw new CatalogException("Unsupported Hive type category " + type.getCategory());
		}
	}

	public static DataType toFlinkType(ObjectInspector inspector) {
		return HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(inspector.getTypeName()));
	}
}
