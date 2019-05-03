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
package io.siddhi.extension.map.avro.util.schema;

import com.google.gson.internal.LinkedTreeMap;
import feign.Headers;
import feign.Param;
import feign.RequestLine;

/**
 * This interface defines the http calls to schema registry.
 */
public interface SchemaRegistryClient {
    @RequestLine("GET")
    Object connect();

    @RequestLine("GET /schemas/ids/{id}")
    @Headers("Content-Type: application/json")
    LinkedTreeMap findByID(@Param("id") String id);
}
