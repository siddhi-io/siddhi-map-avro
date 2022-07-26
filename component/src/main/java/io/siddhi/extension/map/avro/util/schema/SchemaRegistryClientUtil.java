/*
 * Copyright (c)  2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;
import org.apache.kafka.common.config.SslConfigs;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles schema registry client.
 */
public class SchemaRegistryClientUtil {
    public static CachedSchemaRegistryClient buildSchemaRegistryClient(String url, String username, String password,
                                                                       String sslKeystorePath,
                                                                       String sslKeystorePassword,
                                                                       String sslTruststorePath,
                                                                       String sslTruststorePassword) {
        RestService restService = new RestService(url);
        // If the username and the password is specified, initialize the credential provider.
        if (username != null && password != null) {
            Map<String, String> userInfoConfigs = new HashMap<>();
            userInfoConfigs.put(SchemaRegistryClientConfig.USER_INFO_CONFIG, username + ":" + password);
            UserInfoCredentialProvider userInfoCredentialProvider = new UserInfoCredentialProvider();
            userInfoCredentialProvider.configure(userInfoConfigs);
            restService.setBasicAuthCredentialProvider(userInfoCredentialProvider);
        }
        // If the url starts with "https://" initialize ssl socket factory.
        if (url.startsWith("https://")) {
            Map<String, String> sslConfigs = new HashMap<>();
            sslConfigs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystorePath);
            sslConfigs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
            sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststorePath);
            sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
            restService.setSslSocketFactory(new SslFactory(sslConfigs).sslContext().getSocketFactory());
        }
        return new CachedSchemaRegistryClient(restService, 100);
    }
}
