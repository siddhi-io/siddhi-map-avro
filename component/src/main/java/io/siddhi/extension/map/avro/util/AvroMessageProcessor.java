/*
 * Copyright (c)  2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.map.avro.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * This class to contain methods to deserialize byte array to Avro Record and serialize
 * json string to byte array.
 */
public class AvroMessageProcessor {
    private static final Logger log = Logger.getLogger(AvroMessageProcessor.class);

    public static byte[] serializeAvroMessage(String jsonString, Schema schema) {
        Decoder decoder = null;
        GenericRecord datum = null;
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        try {
            decoder = DecoderFactory.get().jsonDecoder(schema, jsonString);
            datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
            output.close();
            return output.toByteArray();
        } catch (IOException e) {
            log.error("IOException occured when serializing event " + jsonString +
                    " to avro message of schema " + schema.toString());
            return null;
        }
    }

    public static Object deserializeByteArray(byte[] data, Schema schema) {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        try {
            Object record = reader.read(null, decoder);
            return record;
        } catch (IOException e) {
            log.error("Error occured when deserializing avro byte stream conforming " +
                    "to schema " + schema.toString() + ". Hence dropping the event.");
            return null;
        }
    }
}
