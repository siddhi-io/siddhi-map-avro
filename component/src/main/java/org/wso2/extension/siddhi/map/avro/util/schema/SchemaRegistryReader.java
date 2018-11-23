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
package org.wso2.extension.siddhi.map.avro.util.schema;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Class to connect to Schema Registry and retrive schema using the schema id.
 */
public class SchemaRegistryReader {

    Logger log = Logger.getLogger(SchemaRegistryReader.class);
    private static final String httpRequest = "/schemas/ids/";
    Gson gson = new Gson();
    private DefaultHttpClient httpClient;
    private ResponseHandler<String> handler;
    private HttpGet getRequest;
    private HttpResponse httpResponse;
    private String responseBody;
    private Schema schema;

    public SchemaRegistryReader() {
         httpClient = new DefaultHttpClient();
         handler = new BasicResponseHandler();
    }

    public Schema getSchemaFromID(String id, String url) throws IOException {
        getRequest = new HttpGet(url + httpRequest + id);
        try {
            httpResponse = httpClient.execute(getRequest);
            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                responseBody = handler.handleResponse(httpResponse);
                Object json = gson.fromJson(responseBody, Object.class);
                schema =  new Schema.Parser().parse((((LinkedTreeMap) json).get("schema")).toString());
            } else if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                schema = null;
                log.error("Schema with id: " + id + " not found in schema registry: " + url);
            } else {
                schema = null;
                log.error(responseBody);
            }
        } catch (ClientProtocolException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (SchemaParseException e) {
            throw e;
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
        return schema;
    }
}
