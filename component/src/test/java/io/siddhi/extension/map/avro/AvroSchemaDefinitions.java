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
package io.siddhi.extension.map.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSchemaDefinitions {

    private static final Logger log = LogManager.getLogger(AvroSchemaDefinitions.class);
    private static GenericRecord user;
    private static GenericRecord complexUser;
    private static GenericArray arrayOfUsers;
    private static GenericRecord flatAvroRecord;
    private static GenericRecord person;
    private static Schema schema;
    private static Schema complexSchema;
    private static Schema arrayschema;
    private static Schema flatAvroSchema;
    private static Schema registrySchema;

    private static String schemaDef = "{\"namespace\": \"avro.user\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"user\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}\n" +
            " ]\n" +
            "}";

    private static String flatSchemaDef = "{\"namespace\": \"avro.user\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"user\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";

    private static String complexSchemaDef = "{\n" +
            "    \"type\" : \"record\",\n" +
            "    \"name\" : \"userInfo\",\n" +
            "    \"namespace\" : \"avro.userInfo\",\n" +
            "    \"fields\" : [{\"name\" : \"username\",\"type\" : \"string\"},\n" +
            "                  {\"name\" : \"age\",\"type\" : \"int\"},\n" +
            "                  {\"name\" : \"address\", \"type\" :{\"type\":\"record\",\n" +
            "                                           \"name\":\"addressField\",\n" +
            "                                           \"fields\":[\n" +
            "                                           {\"name\":\"street\",\"type\":\"string\"},\n" +
            "                                           {\"name\":\"country\",\"type\":{\"type\":\"record\",\n" +
            "                                           \"name\":\"countryField\",\n" +
            "                                           \"fields\":[\n" +
            "                                           {\"name\":\"city\",\"type\":\"string\"},\n" +
            "                                           {\"name\":\"country\",\"type\": \"string\"}]}} \n" +
            "            ]\n" +
            "        } }\n" +
            "   ]\n" +
            "}\n";


    private static String complexSchemaDef2 = "{\n" +
            "    \"type\" : \"record\",\n" +
            "    \"name\" : \"userInfo\",\n" +
            "    \"namespace\" : \"avro.userInfo\",\n" +
            "    \"fields\" : [{\"name\" : \"username\",\"type\" : \"string\"},\n" +
            "                  {\"name\" : \"age\",\"type\" : \"int\"},\n" +
            "                  {\"name\" : \"address\", \"type\" :{\"type\":\"record\",\n" +
            "                                                     \"name\":\"addressField\",\n" +
            "                                                     \"fields\":[\n" +
            "                                                     {\"name\":\"street\",\"type\":\"string\"},\n" +
            "                                                     {\"name\":\"city\",\"type\": \"string\" } \n" +
            "            ]\n" +
            "        } },\n" +
            "                 {\"name\" : \"gender\",\"type\" : \"string\"}\n" +
            "   ]\n" +
            "}\n";

    private static String arraySchema = "{ \"type\":\"array\",\"items\":{\"type\":\"record\",\"name\": " +
            "\"user\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"favorite_number\",\"type\":[\"int\", \"null\"]}]}}";


    private static String addressSchema = "{\"type\":\"record\",\"name\":\"addressField\",\"fields\":[" +
            "{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"}]}";

    private static String complexAddressSchema = "{\"type\":\"record\",\"name\":\"addressField\",\"fields\":[" +
            "{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"country\",\"type\":{\"type\":\"record\",\n" +
            "                                           \"name\":\"countryField\",\n" +
            "                                           \"fields\":[\n" +
            "                                           {\"name\":\"city\",\"type\":\"string\"},\n" +
            "                                           {\"name\":\"country\",\"type\": \"string\"}] \n" +
            "                    }}]}";

    private static String countrySchema = "{\"type\":\"record\",\"name\":\"countryField\",\n" +
            "                               \"fields\":[\n" +
            "                                   {\"name\":\"city\",\"type\":\"string\"},\n" +
            "                                   {\"name\":\"country\",\"type\": \"string\"}] \n" +
            "                    }";

    private static String flatAvroSchemaDef = "{\"namespace\": \"avro.stock\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"stock\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"symbol\", \"type\": \"string\"},\n" +
            "     {\"name\": \"price\",  \"type\": \"float\"},\n" +
            "     {\"name\": \"volume\", \"type\": \"double\"}\n" +
            " ]\n" +
            "}";

    private static String registrySchemaDef = "{\"name\": \"Person\",\n" +
            " \"type\": \"record\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"firstName\", \"type\": \"string\"},\n" +
            "     {\"name\": \"lastName\",  \"type\": \"string\"},\n" +
            "     {\"name\": \"birthDate\",  \"type\": \"long\"}\n" +
            " ]\n" +
            "}";


    public static byte[] createSimpleAvroMessage() {
        schema = new Schema.Parser().
                parse(schemaDef);
        user = new GenericData.Record(schema);
        user.put("name", "WSO2");
        user.put("favorite_number", null);
        return serializeAvroRecord(user, schema);
    }

    public static byte[] createAvroMessage() {
        flatAvroSchema = new Schema.Parser().
                parse(flatAvroSchemaDef);
        flatAvroRecord = new GenericData.Record(flatAvroSchema);
        flatAvroRecord.put("symbol", "WSO2");
        flatAvroRecord.put("price", 102.5f);
        flatAvroRecord.put("volume", 55.66);
        return serializeAvroRecord(flatAvroRecord, flatAvroSchema);
    }

    public static byte[] createComplexAvroMessage() {
        Schema countySchema = new Schema.Parser().parse(countrySchema);
        GenericData.Record countryRecord = new GenericData.Record(countySchema);
        countryRecord.put("city", "Colombo");
        countryRecord.put("country", "SriLanka");

        Schema addressScheme = new Schema.Parser().parse(complexAddressSchema);
        GenericData.Record addressRecord = new GenericData.Record(addressScheme);
        addressRecord.put("street", "Palm Grove");
        addressRecord.put("country", countryRecord);

        complexSchema = new Schema.Parser().parse(complexSchemaDef);
        complexUser = new GenericData.Record(complexSchema);
        complexUser.put("username", "WSO2");
        complexUser.put("age", 26);
        complexUser.put("address", addressRecord);

        return serializeAvroRecord(complexUser, complexSchema);
    }

    public static byte[] createComplexAvroMessage2() {
        Schema addressScheme = new Schema.Parser().parse(addressSchema);
        GenericData.Record addressRecord = new GenericData.Record(addressScheme);
        addressRecord.put("street", "Palm Grove");
        addressRecord.put("city", "Colombo");

        complexSchema = new Schema.Parser().parse(complexSchemaDef2);
        complexUser = new GenericData.Record(complexSchema);
        complexUser.put("username", "WSO2");
        complexUser.put("age", 26);
        complexUser.put("address", addressRecord);
        complexUser.put("gender", "female");

        return serializeAvroRecord(complexUser, complexSchema);
    }

    public static byte[] createArrayOfAvroMessage() {
        schema = new Schema.Parser().
                parse(schemaDef);
        user = new GenericData.Record(schema);
        user.put("name", "WSO2");
        user.put("favorite_number", null);
        GenericRecord userRecord = new GenericData.Record(schema);
        userRecord.put("name", "IBM");
        userRecord.put("favorite_number", 100);
        arrayschema = new Schema.Parser().parse(arraySchema);
        arrayOfUsers = new GenericData.Array<GenericRecord>(2, arrayschema);
        arrayOfUsers.add(user);
        arrayOfUsers.add(userRecord);

        return serializeAvroArray(arrayOfUsers, arrayschema);
    }

    public static byte[] createAvroMessagForRegistrySchema() {
        registrySchema = new Schema.Parser().
                parse(registrySchemaDef);
        person = new GenericData.Record(registrySchema);
        person.put("firstName", "WSO2");
        person.put("lastName", "WSO2");
        person.put("birthDate", 19931025L);
        return serializeAvroRecord(person, registrySchema);
    }

    private static byte[] serializeAvroRecord(GenericRecord record, Schema schema) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        try {
            writer.write(record, encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return out.toByteArray();
    }

    private static byte[] serializeAvroArray(GenericArray array, Schema schema) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericArray> writer = new GenericDatumWriter<>(schema);

        try {
            writer.write(array, encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return out.toByteArray();
    }

    public static Schema getFlatSchema() {
        return new Schema.Parser().parse(flatSchemaDef);
    }

    public static Schema getComplexSchema() {
        return new Schema.Parser().parse(complexSchemaDef);
    }

    public static Schema getFlatAvroSchema() {
        return new Schema.Parser().parse(flatAvroSchemaDef);
    }
}
