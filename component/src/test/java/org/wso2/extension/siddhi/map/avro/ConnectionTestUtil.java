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

import feign.Feign;
import feign.FeignException;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import feign.okhttp.OkHttpClient;
import io.siddhi.extension.map.avro.util.schema.SchemaRegistryClient;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Class defining the Constants for Kafka Test cases.
 */
public class ConnectionTestUtil {
    private static final Logger log = Logger.getLogger(ConnectionTestUtil.class);
    public static final String ZK_SERVER_CON_STRING = "localhost:2181";

    private static GenericRecord stock;
    private static Schema schema;

    private static String schemaDef = "{\"namespace\": \"avro.stock\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"stock\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"symbol\", \"type\": \"string\"},\n" +
            "     {\"name\": \"price\",  \"type\": \"float\"},\n" +
            "     {\"name\": \"volume\",  \"type\": \"double\"}," +
            "     {\"name\": \"isDelivered\",  \"type\": \"boolean\"}\n" +
            " ]\n" +
            "}";

    public static void initAvroSchema() {
        schema = new Schema.Parser().parse(schemaDef);
        stock = new GenericData.Record(schema);
        stock.put("symbol", "WSO2");
        stock.put("price", 20.5f);
        stock.put("volume", 40.42);
        stock.put("isDelivered", true);
    }

    public static void createTopic(String topics[], int numOfPartitions) {
        createTopic(ZK_SERVER_CON_STRING, topics, numOfPartitions);
    }

    public static void createTopic(String connectionString, String topics[], int numOfPartitions) {
        ZkClient zkClient = new ZkClient(connectionString, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkConnection zkConnection = new ZkConnection(connectionString);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        for (String topic : topics) {
            try {
                AdminUtils.createTopic(zkUtils, topic, numOfPartitions, 1, new Properties(),
                        RackAwareMode.Enforced$.MODULE$);
            } catch (TopicExistsException e) {
                log.warn("topic exists for: " + topic);
            }
        }
        zkClient.close();
    }

    public static void deleteTopic(String topics[]) {
        deleteTopic("localhost:2181", topics);
    }

    public static void deleteTopic(String connectionString, String topics[]) {
        ZkClient zkClient = new ZkClient(connectionString, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkConnection zkConnection = new ZkConnection(connectionString);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        for (String topic : topics) {
            AdminUtils.deleteTopic(zkUtils, topic);
        }
        zkClient.close();
    }

    public static void kafkaPublisher(String topics[], int numOfPartitions, int numberOfEventsPerTopic, boolean
            publishWithPartition, String bootstrapServers) {
        kafkaPublisher(topics, numOfPartitions, numberOfEventsPerTopic, 1000, publishWithPartition,
                bootstrapServers);
    }

    public static void kafkaPublisher(String topics[], int numOfPartitions, int numberOfEventsPerTopic, long sleep,
                                      boolean publishWithPartition, String bootstrapServers) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

        try {
            writer.write(stock, encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        byte[] data = out.toByteArray();

        Properties props = new Properties();
        if (null == bootstrapServers) {
            props.put("bootstrap.servers", "localhost:9092");
        } else {
            props.put("bootstrap.servers", bootstrapServers);
        }
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33559000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
        for (String topic : topics) {
            for (int i = 0; i < numberOfEventsPerTopic; i++) {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                }
                if (numOfPartitions > 1 || publishWithPartition) {
                    producer.send(new ProducerRecord<>(topic, (i % numOfPartitions), null, data));
                } else {
                    producer.send(new ProducerRecord<>(topic, null, null, data));
                }
            }
        }
        producer.flush();
        producer.close();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            log.error("Thread sleep failed", e);
        }
    }

    public static void connectToSchemaRegistry(String url) throws FeignException {
        SchemaRegistryClient registryClient = Feign.builder()
                .client(new OkHttpClient())
                .encoder(new GsonEncoder())
                .decoder(new GsonDecoder())
                .target(SchemaRegistryClient.class, url);
        registryClient.connect();
    }
}
