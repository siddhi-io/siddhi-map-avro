/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.map.avro.util.schema;

import com.google.gson.internal.LinkedTreeMap;
import feign.Feign;
import feign.FeignException;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import feign.okhttp.OkHttpClient;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

/**
 * Class to connect to Schema Registry and retrive schema using the schema id.
 */
public class SchemaRegistryReader {
    public Schema getSchemaFromID(String registryURL, String schemaID) throws SchemaParseException, FeignException {
        SchemaRegistryClient registryClient = getSchemaRegistryClient(registryURL);
        LinkedTreeMap returnedSchema = registryClient.findByID(schemaID);
        String jsonSchema = returnedSchema.get("schema").toString();
        return new Schema.Parser().parse(jsonSchema);
    }

    public Schema getSchemaFromName(String registryURL, String schemaName) throws SchemaParseException, FeignException {
        SchemaRegistryClient registryClient = getSchemaRegistryClient(registryURL);
        LinkedTreeMap returnedSchema = registryClient.findBySchemaName(schemaName);
        String jsonSchema = returnedSchema.get("schema").toString();
        return new Schema.Parser().parse(jsonSchema);
    }
    private SchemaRegistryClient getSchemaRegistryClient(String registryURL){
        return Feign.builder()
                .client(new OkHttpClient())
                .encoder(new GsonEncoder())
                .decoder(new GsonDecoder())
                .target(SchemaRegistryClient.class, registryURL);
    }
}
