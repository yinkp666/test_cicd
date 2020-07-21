package com.airwallex.data.streaming.common.rowformat;

import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;

public class MyAvroRowDeserializationSchema extends AvroRowDeserializationSchema{

    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();
    private Class<? extends SpecificRecord> recordClazz;
    private String schemaString;
    private transient Schema schema;
    private transient RowTypeInfo typeInfo;
    private transient IndexedRecord record;
    private transient DatumReader<IndexedRecord> datumReader;
//    private transient MutableByteArrayInputStream inputStream;
//    private transient Decoder decoder;

    public MyAvroRowDeserializationSchema(String avroSchemaString) {
        super(avroSchemaString);
        Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
        this.recordClazz = null;
        TypeInformation<?> typeInfo = AvroSchemaConverter.convertToTypeInfo(avroSchemaString);
        Preconditions.checkArgument(typeInfo instanceof RowTypeInfo, "Row type information expected.");
        this.typeInfo = (RowTypeInfo)typeInfo;
        this.schemaString = avroSchemaString;
        this.schema = (new Parser()).parse(avroSchemaString);
        this.record = new Record(this.schema);
        this.datumReader = new GenericDatumReader(this.schema);

//        this.inputStream = new MutableByteArrayInputStream();
//        this.decoder = DecoderFactory.get().binaryDecoder(this.inputStream, (BinaryDecoder)null);
    }

    @Override
    public Row deserialize(byte[] message) {
        try {
            final int idSize = 4;
            ByteBuffer buffer = ByteBuffer.wrap(message);
            buffer.get();
            int schemaId = buffer.getInt();

            int length = buffer.limit() - 1 - idSize;

            int start = buffer.position() + buffer.arrayOffset();

            this.record = (IndexedRecord) this.datumReader.read(null, DecoderFactory.get().binaryDecoder(buffer.array(),
                    start, length, null));

             return this.convertAvroRecordToRow(this.schema, this.typeInfo, this.record);
        } catch (Exception var3) {
           //  throw new IOException("Failed to deserialize Avro record.", var3);
           // System.out.println("err deser");
            return null;
        }
    }

    private Row convertAvroRecordToRow(Schema schema, RowTypeInfo typeInfo, IndexedRecord record) {
        List<Schema.Field> fields = schema.getFields();
        TypeInformation<?>[] fieldInfo = typeInfo.getFieldTypes();
        int length = fields.size();
        Row row = new Row(length);
        for(int i = 0; i < length; ++i) {
            Schema.Field field = (Schema.Field)fields.get(i);
            row.setField(i, this.convertAvroType(field.schema(), fieldInfo[i], record.get(i)));
        }

        return row;
    }

    private Object convertAvroType(Schema schema, TypeInformation<?> info, Object object) {
        if (object == null) {
            return null;
        } else {
            switch(schema.getType()) {
                case RECORD:
                    if (object instanceof IndexedRecord) {
                        return this.convertAvroRecordToRow(schema, (RowTypeInfo)info, (IndexedRecord)object);
                    }

                    throw new IllegalStateException("IndexedRecord expected but was: " + object.getClass());
                case ENUM:
                case STRING:
                    return object.toString();
                case ARRAY:
                    TypeInformation elementInfo;
                    if (info instanceof BasicArrayTypeInfo) {
                        elementInfo = ((BasicArrayTypeInfo)info).getComponentInfo();
                        return this.convertToObjectArray(schema.getElementType(), elementInfo, object);
                    }

                    elementInfo = ((ObjectArrayTypeInfo)info).getComponentInfo();
                    return this.convertToObjectArray(schema.getElementType(), elementInfo, object);
                case MAP:
                    MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo)info;
                    Map<String, Object> convertedMap = new HashMap();
                    Map<?, ?> map = (Map)object;
                    Iterator var14 = map.entrySet().iterator();

                    while(var14.hasNext()) {
                        Entry<?, ?> entry = (Entry)var14.next();
                        convertedMap.put(entry.getKey().toString(), this.convertAvroType(schema.getValueType(), mapTypeInfo.getValueTypeInfo(), entry.getValue()));
                    }

                    return convertedMap;
                case UNION:
                    List<Schema> types = schema.getTypes();
                    int size = types.size();
                    if (size == 2 && ((Schema)types.get(0)).getType() == Schema.Type.NULL) {
                        return this.convertAvroType((Schema)types.get(1), info, object);
                    } else if (size == 2 && ((Schema)types.get(1)).getType() == Schema.Type.NULL) {
                        return this.convertAvroType((Schema)types.get(0), info, object);
                    } else {
                        if (size == 1) {
                            return this.convertAvroType((Schema)types.get(0), info, object);
                        }

                        return object;
                    }
                case FIXED:
                    byte[] fixedBytes = ((GenericFixed)object).bytes();
                    if (info == Types.BIG_DEC) {
                        return this.convertToDecimal(schema, fixedBytes);
                    }

                    return fixedBytes;
                case BYTES:
                    ByteBuffer byteBuffer = (ByteBuffer)object;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    if (info == Types.BIG_DEC) {
                        return this.convertToDecimal(schema, bytes);
                    }

                    return bytes;
                case INT:
                    if (info == Types.SQL_DATE) {
                        return this.convertToDate(object);
                    } else {
                        if (info == Types.SQL_TIME) {
                            return this.convertToTime(object);
                        }

                        return object;
                    }
                case LONG:
                    if (info == Types.SQL_TIMESTAMP) {
                        return this.convertToTimestamp(object);
                    }

                    return object;
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                    return object;
                default:
                    throw new RuntimeException("Unsupported Avro type:" + schema);
            }
        }
    }

    private BigDecimal convertToDecimal(Schema schema, byte[] bytes) {
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal)schema.getLogicalType();
        return new BigDecimal(new BigInteger(bytes), decimalType.getScale());
    }

    private Date convertToDate(Object object) {
        long millis;
        if (object instanceof Integer) {
            Integer value = (Integer)object;
            long t = (long)value * 86400000L;
            millis = t - (long)LOCAL_TZ.getOffset(t);
        } else {
            LocalDate value = (LocalDate)object;
            millis = value.toDate().getTime();
        }

        return new Date(millis);
    }

    private Time convertToTime(Object object) {
        long millis;
        if (object instanceof Integer) {
            millis = (long)(Integer)object;
        } else {
            LocalTime value = (LocalTime)object;
            millis = (long)value.get(DateTimeFieldType.millisOfDay());
        }

        return new Time(millis - (long)LOCAL_TZ.getOffset(millis));
    }

    private Timestamp convertToTimestamp(Object object) {
        long millis;
        if (object instanceof Long) {
            millis = (Long)object;
        } else {
            DateTime value = (DateTime)object;
            millis = value.toDate().getTime();
        }

        return new Timestamp(millis - (long)LOCAL_TZ.getOffset(millis));
    }

    private Object[] convertToObjectArray(Schema elementSchema, TypeInformation<?> elementInfo, Object object) {
        List<?> list = (List)object;
        Object[] convertedArray = (Object[])((Object[]) Array.newInstance(elementInfo.getTypeClass(), list.size()));

        for(int i = 0; i < list.size(); ++i) {
            convertedArray[i] = this.convertAvroType(elementSchema, elementInfo, list.get(i));
        }

        return convertedArray;
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(this.recordClazz);
        outputStream.writeUTF(this.schemaString);
    }

    private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
        this.recordClazz = (Class)inputStream.readObject();
        this.schemaString = inputStream.readUTF();
        TypeInformation<?> typeInfo = AvroSchemaConverter.convertToTypeInfo(this.schemaString);
        this.typeInfo = (RowTypeInfo)typeInfo;
        this.schema = (new Parser()).parse(this.schemaString);
        if (this.recordClazz != null) {
            this.record = (SpecificRecord)SpecificData.newInstance(this.recordClazz, this.schema);
        } else {
            this.record = new Record(this.schema);
        }

        this.datumReader = new SpecificDatumReader(this.schema);
//        this.inputStream = new MutableByteArrayInputStream();
//        this.decoder = DecoderFactory.get().binaryDecoder(this.inputStream, (BinaryDecoder)null);
    }

}
