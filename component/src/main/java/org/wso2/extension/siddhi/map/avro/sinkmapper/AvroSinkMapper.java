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
package org.wso2.extension.siddhi.map.avro.sinkmapper;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import feign.FeignException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.map.avro.util.AvroMessageProcessor;
import org.wso2.extension.siddhi.map.avro.util.schema.RecordSchema;
import org.wso2.extension.siddhi.map.avro.util.schema.SchemaRegistryReader;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.sink.SinkListener;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Mapper class to convert a Siddhi event to a Avro message.
 */

@Extension(
        name = "avro",
        namespace = "sinkMapper",
        description = "" +
                "This extension is a Siddhi Event to Avro Message output mapper." +
                "Transports that publish  messages to Avro sink can utilize this extension to convert Siddhi " +
                "events to Avro messages.\n You can either specify the Avro schema or provide the schema registry " +
                "URL and the schema reference ID as parameters in the stream definition.\n" +
                "If no Avro schema is specified, a flat Avro schema of the 'record' type is generated with " +
                "the stream attributes as schema fields.",
        parameters = {
                @Parameter(name = "schema.def",
                        description = "This specifies the required Avro schema to be used to convert Siddhi " +
                                "events to Avro messages.\n" +
                                "The schema needs to be specified as a quoted JSON string.",
                        type = {DataType.STRING}),
                @Parameter(name = "schema.registry",
                        description = "This specifies the URL of the schema registry.",
                        type = {DataType.STRING}),
                @Parameter(name = "schema.id",
                        description = "This specifies the ID of the avro schema. This ID is the global ID that is " +
                                "returned from the schema registry when posting the schema to the registry. " +
                                "The specified ID is used to retrieve the schema from the schema registry.",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='avro'," +
                                "schema.def = \"\"\"{\"type\":\"record\",\"name\":\"stock\"," +
                                "\"namespace\":\"stock.example\",\"fields\":[{\"name\":\"symbol\"," +
                                "\"type\":\"string\"},{\"name\":\"price\":\"type\":\"float\"}," +
                                "{\"name\":\"volume\",\"type\":\"long\"}]}\"\"\"))\n" +
                                "define stream StockStream (symbol string, price float, volume long);",
                        description = "The above configuration performs a default Avro mapping that generates " +
                                "an Avro message as an output ByteBuffer."),
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='avro'," +
                                "schema.registry = 'http://localhost:8081', schema.id ='22'," +
                                "@payload(\"\"\"{\"Symbol\":{{symbol}},\"Price\":{{price}}," +
                                "\"Volume\":{{volume}}}\"\"\"\n" +
                                ")))\n" +
                                "define stream StockStream (symbol string, price float, volume long);",
                        description = "The above configuration performs a custom Avro mapping that generates " +
                                "an Avro message as an output ByteBuffer. The Avro schema is retrieved " +
                                "from the given schema registry (localhost:8081) using the schema ID provided.")
        }
)

public class AvroSinkMapper extends SinkMapper {
    private static final Logger log = Logger.getLogger(AvroSinkMapper.class);
    private static final String DEFAULT_AVRO_MAPPING_PREFIX = "schema";
    private static final String SCHEMA_IDENTIFIER = "def";
    private static final String UNDEFINED = "undefined";
    private static final String SCHEMA_REGISTRY = "registry";
    private static final String SCHEMA_ID = "id";

    private String[] attributeNameArray;
    private Schema schema;
    private List<Attribute> attributeList;

    /**
     * Initialize the mapper and the mapping configurations.
     *
     * @param streamDefinition          The stream definition
     * @param optionHolder              Option holder containing static and dynamic options
     * @param payloadTemplateBuilderMap Unmapped list of payloads for reference
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, Map<String, TemplateBuilder>
            payloadTemplateBuilderMap, ConfigReader mapperConfigReader, SiddhiAppContext siddhiAppContext) {

        this.attributeNameArray = streamDefinition.getAttributeNameArray();
        this.attributeList = streamDefinition.getAttributeList();
        //if @payload() is added there must be at least 1 element in it, otherwise a SiddhiParserException raised
        if (payloadTemplateBuilderMap != null && payloadTemplateBuilderMap.size() != 1) {
            throw new SiddhiAppCreationException("Avro sink-mapper does not support multiple @payload mappings, " +
                    "error at the mapper of '" + streamDefinition.getId() + "'");
        }
        if (payloadTemplateBuilderMap != null &&
                payloadTemplateBuilderMap.get(payloadTemplateBuilderMap.keySet().iterator().next()).isObjectMessage()) {
            throw new SiddhiAppCreationException("Avro sink-mapper does not support object @payload mappings, " +
                    "error at the mapper of '" + streamDefinition.getId() + "'");
        }
        schema = getAvroSchema(optionHolder.validateAndGetStaticValue(DEFAULT_AVRO_MAPPING_PREFIX.concat(".").
                        concat(SCHEMA_IDENTIFIER), null),
                optionHolder.validateAndGetStaticValue(DEFAULT_AVRO_MAPPING_PREFIX.concat(".").
                        concat(SCHEMA_REGISTRY), null),
                optionHolder.validateAndGetStaticValue(DEFAULT_AVRO_MAPPING_PREFIX.concat(".").
                        concat(SCHEMA_ID), null), streamDefinition.getId());
    }

    private Schema getAvroSchema(String schemaDefinition, String schemaRegistryURL, String schemaID,
                                 String streamName) {
        Schema returnSchema = null;
        try {
            if (schemaDefinition != null) {
                returnSchema = new Schema.Parser().parse(schemaDefinition);
            } else if (schemaRegistryURL != null) {
                SchemaRegistryReader schemaRegistryReader = new SchemaRegistryReader();
                returnSchema = schemaRegistryReader.getSchemaFromID(schemaRegistryURL, schemaID);
            } else if (attributeList.size() > 0) {
                log.warn("Schema Definition and Schema Registry is not specified in Stream. Hence generating " +
                        "schema from stream attributes.");
                RecordSchema recordSchema = new RecordSchema();
                returnSchema = recordSchema.generateAvroSchema(this.attributeList, streamName);
            } else {
                throw new SiddhiAppCreationException("Avro Schema is not specified in the stream definition. "
                        + streamName);
            }
        } catch (SchemaParseException e) {
            throw new SiddhiAppCreationException("Unable to parse Schema for stream:" + streamName + ". " +
                    e.getMessage());
        } catch (FeignException e) {
            throw new SiddhiAppCreationException("Error when retriving schema from schema registry. " + e.getMessage());
        }
        if (returnSchema == null) {
            throw new SiddhiAppCreationException("Error when generating Avro Schema for stream: "
                    + streamName);
        }
        return returnSchema;
    }

    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, Map<String, TemplateBuilder>
            payloadTemplateBuilderMap, SinkListener sinkListener) {
        List<byte[]> data = new ArrayList<>();
        for (Event event : events) {
            byte[] returnedData = mapSingleEvent(event, payloadTemplateBuilderMap);
            if (returnedData != null) {
                data.add(returnedData);
            }
        }
        for (byte[] message : data) {
            sinkListener.publish(ByteBuffer.wrap(message));
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, Map<String, TemplateBuilder>
            payloadTemplateBuilderMap, SinkListener sinkListener) {
        byte[] data = null;
        data = mapSingleEvent(event, payloadTemplateBuilderMap);
        if (data != null) {
            sinkListener.publish(ByteBuffer.wrap(data));
        }
    }

    private byte[] mapSingleEvent(Event event, Map<String, TemplateBuilder>
            payloadTemplateBuilderMap) {
        byte[] data = null;
        if (payloadTemplateBuilderMap == null) {
            data = constructAvroForDefaultMapping(event);
        } else {
            data = constructAvroForCustomMapping(event, payloadTemplateBuilderMap.get(
                    payloadTemplateBuilderMap.keySet().iterator().next()));
        }
        return data;
    }

    /**
     * The method returns a null instead of a byte[0] to enhance the performance.
     * Creation of empty byte array and length > 0 check for each event conversion is costly
     */
    private byte[] constructAvroForDefaultMapping(Object eventObj) {
        byte[] convertedEvent = null;

        if (eventObj instanceof Event) {
            Event event = (Event) eventObj;
            JsonObject jsonEvent = constructSingleEventForDefaultMapping(event);
            try {
                convertedEvent = AvroMessageProcessor.serializeAvroMessage(jsonEvent.toString(), schema);
            } catch (Throwable t) {
                log.error("Error when converting siddhi event: " + Arrays.toString(event.getData()) +
                        " to Avro message of schema: " + schema + "." + t.getMessage() +
                        ". Hence dropping the event.");
            }
            return convertedEvent;
        } else {
            log.error("Invalid object type. " + eventObj.toString() + " of type " + eventObj.getClass().getName() +
                    " cannot be converted to an Avro Message");
            return null;
        }
    }

    /**
     * The method returns a null instead of a byte[0] to enhance the performance.
     * Creation of empty byte array and length > 0 check for each event conversion is costly
     */
    private byte[] constructAvroForCustomMapping(Event event, TemplateBuilder payloadTemplateBuilder) {
        String jsonString = (String) payloadTemplateBuilder.build(doPartialProcessing(event));
        try {
            return AvroMessageProcessor.serializeAvroMessage(jsonString, schema);
        } catch (Throwable t) {
            log.error("Error when converting siddhi event: " + Arrays.toString(event.getData()) +
                    " to Avro message of schema: " + schema + "." + t.getMessage() +
                    ". Hence dropping the event.");
            return null;
        }
    }

    private JsonObject constructSingleEventForDefaultMapping(Event event) {
        Object[] data = event.getData();
        JsonObject innerParentObject = new JsonObject();
        String attributeName;
        Object attributeValue;
        Gson gson = new Gson();
        for (int i = 0; i < data.length; i++) {
            attributeName = attributeNameArray[i];
            attributeValue = data[i];
            if (attributeValue != null) {
                if (attributeValue instanceof String) {
                    innerParentObject.addProperty(attributeName, attributeValue.toString());
                } else if (attributeValue instanceof Number) {
                    innerParentObject.addProperty(attributeName, (Number) attributeValue);
                } else if (attributeValue instanceof Boolean) {
                    innerParentObject.addProperty(attributeName, (Boolean) attributeValue);
                } else if (attributeValue instanceof Map) {
                    if (!((Map) attributeValue).isEmpty()) {
                        innerParentObject.add(attributeName, gson.toJsonTree(attributeValue));
                    }
                }
            } else {
                innerParentObject.addProperty(attributeName, UNDEFINED);
            }
        }
        return innerParentObject;
    }

    private Event doPartialProcessing(Event event) {
        Object[] data = event.getData();
        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                data[i] = UNDEFINED;
            }
        }
        return event;
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{ByteBuffer.class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }
}
