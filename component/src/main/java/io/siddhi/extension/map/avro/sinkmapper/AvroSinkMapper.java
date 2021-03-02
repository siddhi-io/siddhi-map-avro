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
package io.siddhi.extension.map.avro.sinkmapper;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import feign.FeignException;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.sink.SinkListener;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.TemplateBuilder;
import io.siddhi.extension.map.avro.util.AvroMessageProcessor;
import io.siddhi.extension.map.avro.util.schema.RecordSchema;
import io.siddhi.extension.map.avro.util.schema.SchemaRegistryReader;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
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
                        type = {DataType.STRING}),
                @Parameter(name = "use.avro.serializer",
                        description = "Set this parameter to true when you use the class " +
                                "io.confluent.kafka.serializers.KafkaAvroSerializer as the value " +
                                "serializer when creating the Kafka producer. " +
                                "When set to false, org.apache.kafka.common.serialization.ByteArraySerializer " +
                                "will be used.",
                        optional = true,
                        defaultValue = "false",
                        type = {DataType.BOOL})
        },
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='avro'," +
                                "schema.def = \"\"\"{\"type\":\"record\",\"name\":\"stock\"," +
                                "\"namespace\":\"stock.example\",\"fields\":[{\"name\":\"symbol\"," +
                                "\"type\":\"string\"},{\"name\":\"price\",\"type\":\"float\"}," +
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
    public static final String USE_AVRO_SERIALIZER = "use.avro.serializer";

    private String[] attributeNameArray;
    private Schema schema;
    private List<Attribute> attributeList;
    private boolean useAvroSerializer;

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
        useAvroSerializer = Boolean.parseBoolean(
                optionHolder.validateAndGetStaticValue(USE_AVRO_SERIALIZER, "false"));
    }

    private Schema getAvroSchema(String schemaDefinition, String schemaRegistryURL, String schemaID,
                                 String streamName) {
        Schema returnSchema;
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
            throw new SiddhiAppCreationException(
                    "Error when retrieving schema from schema registry. " + e.getMessage());
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
        for (Event event : events) {
            sinkListener.publish(mapSingleEvent(event, payloadTemplateBuilderMap));
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, Map<String, TemplateBuilder>
            payloadTemplateBuilderMap, SinkListener sinkListener) {
        Object mappedEvent = mapSingleEvent(event, payloadTemplateBuilderMap);
        if (mappedEvent != null) {
            sinkListener.publish(mappedEvent);
        }
    }

    private Object mapSingleEvent(Event event, Map<String, TemplateBuilder> payloadTemplateBuilderMap) {
        Object object;
        if (payloadTemplateBuilderMap == null) {
            object = constructAvroForDefaultMapping(event);
        } else {
            object = constructAvroForCustomMapping(event, payloadTemplateBuilderMap.get(
                    payloadTemplateBuilderMap.keySet().iterator().next()));
        }
        return object;
    }

    /**
     * The method returns a null instead of a byte[0] to enhance the performance.
     * Creation of empty byte array and length > 0 check for each event conversion is costly
     */
    private Object constructAvroForDefaultMapping(Object eventObj) {
        Object convertedEvent = null;

        if (eventObj instanceof Event) {
            Event event = (Event) eventObj;
            JsonObject jsonEvent = constructSingleEventForDefaultMapping(event);
            try {
                convertedEvent = AvroMessageProcessor.serializeAvroMessage(
                        jsonEvent.toString(), schema, useAvroSerializer);
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
    private Object constructAvroForCustomMapping(Event event, TemplateBuilder payloadTemplateBuilder) {
        String jsonString = (String) payloadTemplateBuilder.build(doPartialProcessing(event));
        try {
            return AvroMessageProcessor.serializeAvroMessage(jsonString, schema, useAvroSerializer);
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
