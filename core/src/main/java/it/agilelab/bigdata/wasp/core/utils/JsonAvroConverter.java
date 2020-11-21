package it.agilelab.bigdata.wasp.core.utils;

import it.agilelab.darwin.manager.AvroSchemaManager;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Option;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class JsonAvroConverter {
    private JsonGenericRecordReader recordReader;

    public JsonAvroConverter() {
        this.recordReader = new JsonGenericRecordReader();
    }

    public JsonAvroConverter(ObjectMapper objectMapper) {
        this.recordReader = new JsonGenericRecordReader(objectMapper);
    }

    public byte[] convertToAvro(byte[] data, String schema, Option<AvroSchemaManager> avroSchemaManagerOption) {
        return convertToAvro(data, new Schema.Parser().parse(schema), avroSchemaManagerOption);
    }

    public byte[] convertToAvro(byte[] data, Schema schema, Option<AvroSchemaManager> avroSchemaManagerOption) {
        try {

            Object writeThis = null;

            switch (schema.getType()) {
                case RECORD:
                    writeThis = data;
                    break;
                case ENUM:
                    schema.getEnumOrdinal(new String(data, StandardCharsets.UTF_8));
                    break;
                case UNION:
                    throw new IllegalArgumentException("Union are not supported for bare values");
                case FIXED:
                    writeThis = data;
                    break;
                case STRING:
                    writeThis = new String(data, StandardCharsets.UTF_8);
                    break;
                case BYTES:
                    writeThis = data;
                    break;
                case INT:
                    writeThis = Integer.parseInt(new String(data, StandardCharsets.UTF_8));
                    break;
                case LONG:
                    writeThis = Long.parseLong(new String(data, StandardCharsets.UTF_8));
                    break;
                case FLOAT:
                    writeThis = Float.parseFloat(new String(data, StandardCharsets.UTF_8));
                    break;
                case DOUBLE:
                    writeThis = Double.parseDouble(new String(data, StandardCharsets.UTF_8));
                    break;
                case BOOLEAN:
                    writeThis = Boolean.parseBoolean(new String(data, StandardCharsets.UTF_8));
                    break;
                case NULL:
                    writeThis = null;
                    break;
            }


            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            if (schema.getType() == Schema.Type.RECORD) {
                writer.write(convertToGenericDataRecord(data, schema), encoder);
            } else {
                writer.write(writeThis, encoder);
            }
            encoder.flush();
            if (avroSchemaManagerOption.isDefined())
                return avroSchemaManagerOption.get().generateAvroSingleObjectEncoded(outputStream.toByteArray(), schema);
            else
                return outputStream.toByteArray();

        } catch (IOException e) {
            throw new AvroConversionException("Failed to convert to AVRO.", e);
        }
    }

    public GenericData.Record convertToGenericDataRecord(byte[] data, Schema schema) {
        return recordReader.read(data, schema);
    }

    public byte[] convertToJson(byte[] avro, String schema) {
        return convertToJson(avro, new Schema.Parser().parse(schema));
    }

    public byte[] convertToJson(byte[] avro, Schema schema) {
        try {
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(avro, null);
            if(schema.getType().equals(Schema.Type.RECORD)) {
                GenericRecord record = new GenericDatumReader<GenericRecord>(schema).read(null, binaryDecoder);
                return convertToJson(record);
            }else {
                Object record1 = new GenericDatumReader<Object>(schema).read(null, binaryDecoder);
                return convertToJsonAny(record1, schema);
            }
        } catch (IOException e) {
            throw new AvroConversionException("Failed to create avro structure.", e);
        }
    }


    public byte[] convertToJsonAny(Object record, Schema schema) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            NoWrappingJsonEncoder jsonEncoder = new NoWrappingJsonEncoder(schema, outputStream);
            new GenericDatumWriter<Object>(schema).write(record, jsonEncoder);
            jsonEncoder.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new AvroConversionException("Failed to convert to JSON.", e);
        }
    }

    public byte[] convertToJson(GenericRecord record) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            NoWrappingJsonEncoder jsonEncoder = new NoWrappingJsonEncoder(record.getSchema(), outputStream);
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, jsonEncoder);
            jsonEncoder.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new AvroConversionException("Failed to convert to JSON.", e);
        }
    }

}
