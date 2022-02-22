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
package io.siddhi.extension.map.avro.sourcemapper;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import feign.FeignException;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.MappingFailedException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.source.AttributeMapping;
import io.siddhi.core.stream.input.source.InputEventHandler;
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.core.util.AttributeConverter;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.map.avro.util.AvroMessageProcessor;
import io.siddhi.extension.map.avro.util.schema.RecordSchema;
import io.siddhi.extension.map.avro.util.schema.SchemaRegistryReader;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import net.minidev.json.JSONArray;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This mapper converts Avro message stream input to {@link io.siddhi.core.event.ComplexEventChunk}.
 */

@Extension(
        name = "avro",
        namespace = "sourceMapper",
        description = "" +
                "This extension is an Avro to Event input mapper. Transports that accept Avro messages can utilize " +
                "this extension to convert the incoming Avro messages to Siddhi events.\n The Avro" +
                " schema to be used for creating Avro messages can be specified as a parameter in the stream " +
                "definition.\n If no Avro schema is specified, a flat avro schema of the 'record' type is generated" +
                " with the stream attributes as schema fields.\n" +
                "The generated/specified Avro schema is used to convert Avro messages to Siddhi events.",
        parameters = {
                @Parameter(name = "schema.def",
                        description =
                                "This specifies the schema of the Avro message. The full schema used to create the " +
                                        "Avro message needs to be specified as a quoted JSON string.",
                        type = {DataType.STRING}),
                @Parameter(name = "schema.registry",
                        description = "This specifies the URL of the schema registry.",
                        type = {DataType.STRING}),
                @Parameter(name = "schema.id",
                        description =
                                "This specifies the ID of the Avro schema. This ID is the global ID that is " +
                                        "returned from the schema registry when posting the schema to the registry. " +
                                        "The schema is retrieved from the schema registry via the specified ID.",

                        type = {DataType.STRING}),
                @Parameter(name = "fail.on.missing.attribute",
                        description = "If this parameter is set to 'true', a JSON execution failing or returning a " +
                                "null value results in that message being dropped by the system.\n" +
                                "If this parameter is set to 'false', a JSON execution failing or returning a " +
                                "null value results in the system being prompted to send the event with a null " +
                                "value to Siddhi so that the user can handle it as required (i.e., by assigning" +
                                " a default value.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"),
                @Parameter(name = "use.avro.deserializer",
                        description = "Set this parameter to true when you use the class " +
                                "io.confluent.kafka.serializers.KafkaAvroDeserializer as the value " +
                                "deserializer when creating the Kafka consumer configs. " +
                                "When set to false, org.apache.kafka.common.serialization.ByteArrayDeserializer " +
                                "will be used.",
                        optional = true,
                        defaultValue = "false",
                        type = {DataType.BOOL})
        },
        examples = {
                @Example(
                        syntax = "@source(type='inMemory', topic='user', @map(type='avro', schema .def = \"\"\"" +
                                "{\"type\":\"record\",\"name\":\"userInfo\",\"namespace\":\"user.example\",\"fields\"" +
                                ":[{\"name\":\"name\",\"type\":\"string\"}, {\"name\":\"age\",\"type\":\"int\"}]}" +
                                "\"\"\"))\n"
                                + "define stream UserStream (name string, age int );\n",
                        description = "The above Siddhi query performs a default Avro input mapping. The input Avro " +
                                "message that contains user information is converted to a Siddhi event.\n" +
                                "The expected input is a byte array or ByteBuffer."),
                @Example(
                        syntax = "@source(type='inMemory', topic='user', @map(type='avro', schema .def = \"\"\"" +
                                "{\"type\":\"record\",\"name\":\"userInfo\",\"namespace\":\"avro.userInfo\"," +
                                "\"fields\":[{\"name\":\"username\",\"type\":\"string\"}, {\"name\":\"age\"," +
                                "\"type\":\"int\"}]}\"\"\",@attributes(name=\"username\",age=\"age\")))\n" +
                                "define stream userStream (name string, age int );\n",
                        description = "The above Siddhi query performs a custom Avro input mapping. " +
                                "The input Avro message that contains user information is converted  to a Siddhi" +
                                " event.\n " +
                                "The expected input is a byte array or ByteBuffer."),
                @Example(
                        syntax = "@source(type='inMemory', topic='user', @map(type='avro'," +
                                "schema.registry='http://192.168.2.5:9090', schema.id='1'," +
                                "@attributes(name=\"username\",age=\"age\")))\n" +
                                "define stream UserStream (name string, age int );\n",
                        description = "The above Siddhi query performs a custom Avro input mapping. The input Avro " +
                                "message that contains user information is converted to a Siddhi event via the " +
                                "schema retrieved from the given schema registry(localhost:8081).\n" +
                                "The expected input is a byte array or ByteBuffer.")
        }
)

public class AvroSourceMapper extends SourceMapper {

    private static final Logger log = LogManager.getLogger(AvroSourceMapper.class);
    private static final String DEFAULT_AVRO_MAPPING_PREFIX = "schema";
    private static final String SCHEMA_IDENTIFIER = "def";
    private static final String DEFAULT_JSON_PATH = "$";
    private static final String SCHEMA_REGISTRY = "registry";
    private static final String SCHEMA_ID = "id";
    private static final String FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER = "fail.on.missing.attribute";
    public static final String USE_AVRO_DESERIALIZER = "use.avro.deserializer";

    private StreamDefinition streamDefinition;
    private List<Attribute> streamAttributes;
    private int streamAttributesSize;
    private JsonFactory jsonFactory;
    private boolean isCustomMappingEnabled;
    private boolean failOnMissingAttribute = true;
    private MappingPositionData[] mappingPositions;
    private AttributeConverter attributeConverter = new AttributeConverter();
    private ObjectMapper objectMapper = new ObjectMapper();
    private Gson gson = new Gson();
    private Schema schema;
    private boolean useAvroDeserializer;

    /**
     * Initialize the mapper and the mapping configurations.
     *
     * @param streamDefinition
     * @param optionHolder
     * @param attributeMappingList
     * @param configReader
     * @param siddhiAppContext
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, List<AttributeMapping>
            attributeMappingList, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        jsonFactory = new JsonFactory();
        this.streamDefinition = streamDefinition;
        this.streamAttributes = this.streamDefinition.getAttributeList();
        this.streamAttributesSize = this.streamDefinition.getAttributeList().size();
        this.failOnMissingAttribute = Boolean.parseBoolean(optionHolder.
                validateAndGetStaticValue(FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER, "true"));
        if (attributeMappingList != null && attributeMappingList.size() > 0) {
            this.mappingPositions = new MappingPositionData[attributeMappingList.size()];
            isCustomMappingEnabled = true;
            for (int i = 0; i < attributeMappingList.size(); i++) {
                AttributeMapping attributeMapping = attributeMappingList.get(i);
                String attributeName = attributeMapping.getName();
                int position = this.streamDefinition.getAttributePosition(attributeName);
                this.mappingPositions[i] = new MappingPositionData(position, attributeMapping.getMapping());
            }
        }
        schema = getAvroSchema(optionHolder.validateAndGetStaticValue(DEFAULT_AVRO_MAPPING_PREFIX.concat(".").
                        concat(SCHEMA_IDENTIFIER), null),
                optionHolder.validateAndGetStaticValue(DEFAULT_AVRO_MAPPING_PREFIX.concat(".").
                        concat(SCHEMA_REGISTRY), null),
                optionHolder.validateAndGetStaticValue(DEFAULT_AVRO_MAPPING_PREFIX.concat(".").
                        concat(SCHEMA_ID), null), streamDefinition.getId());
        useAvroDeserializer = Boolean.parseBoolean(
                optionHolder.validateAndGetStaticValue(USE_AVRO_DESERIALIZER, "false"));
    }

    private Schema getAvroSchema(String schemaDefinition, String schemaRegistryURL, String schemaID,
                                 String streamName) {
        Schema schema;
        try {
            if (schemaDefinition != null) {
                schema = new Schema.Parser().parse(schemaDefinition);
            } else if (schemaRegistryURL != null) {
                SchemaRegistryReader schemaRegistryReader = new SchemaRegistryReader();
                schema = schemaRegistryReader.getSchemaFromID(schemaRegistryURL, schemaID);
            } else if (streamAttributes.size() > 0) {
                log.warn("Schema Definition or Schema Registry is not specified in Stream. Hence generating " +
                        "schema from stream attributes.");
                RecordSchema recordSchema = new RecordSchema();
                schema = recordSchema.generateAvroSchema(streamAttributes, streamDefinition.getId());
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
        if (schema == null) {
            throw new SiddhiAppCreationException("Error when generating Avro Schema for stream: "
                    + streamName);
        }
        return schema;
    }

    /**
     * Receives an event or events as a byte[] from source, converts it to
     * a {@link io.siddhi.core.event.ComplexEventChunk}.
     *
     * @param eventObject       the input, given as a byte array or ByteBuffer
     * @param inputEventHandler input handler
     */
    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler)
            throws InterruptedException, MappingFailedException {
        Event[] convertedEvent = null;
        List<ErroneousEvent> failedEvents = new ArrayList<>(0);
        try {
            convertedEvent = convertToEvents(eventObject, failedEvents);
        } catch (Throwable t) {
            log.error("Exception occurred when converting Avro message: " + eventObject.toString() +
                    " to Siddhi Event", t);
            failedEvents.add(new ErroneousEvent(eventObject,
                    "Exception occurred when converting Avro message: " + eventObject.toString() +
                            " to Siddhi Event"));
        }
        if (convertedEvent != null) {
            inputEventHandler.sendEvents(convertedEvent);
        }
        if (!failedEvents.isEmpty()) {
            throw new MappingFailedException(failedEvents);
        }
    }

    public static String toJsonString(Schema schema, GenericRecord genericRecord) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos, false);
        writer.write(genericRecord, encoder);
        encoder.flush();
        return baos.toString(Charset.defaultCharset().name());
    }

    private Event[] convertToEvents(Object eventObject, List<ErroneousEvent> failedEvents)
            throws MappingFailedException {
        byte[] binaryEvent = new byte[0];
        if (eventObject instanceof byte[]) {
            binaryEvent = (byte[]) eventObject;
        } else if (eventObject instanceof ByteBuffer) {
            binaryEvent = ((ByteBuffer) eventObject).array();
        } else if (!(eventObject instanceof GenericRecord)) {
            log.error("Event object is invalid. Expected byte Array, GenericRecord or ByteBuffer , but found "
                    + eventObject.getClass().getCanonicalName());
            failedEvents.add(new ErroneousEvent(eventObject,
                    "Event object is invalid. Expected byte Array or ByteBuffer, but found "
                            + eventObject.getClass().getCanonicalName()));
            return null;
        }
        String avroMessage = null;
        if (useAvroDeserializer) {
            try {
                avroMessage = toJsonString(schema, (GenericRecord) eventObject);
            } catch (IOException e) {
                log.error("Failed to convert received GenericRecord object to Json string for schema "
                        + schema.toString() + ". Reason: " + e.getMessage());
                failedEvents.add(new ErroneousEvent(eventObject, "Failed to convert received " +
                        "GenericRecord object to Json string for schema "
                        + schema.toString() + ". Reason: " + e.getMessage()));
            }
        } else {
            Object record = AvroMessageProcessor.deserializeByteArray(binaryEvent, schema, failedEvents);
            if (record == null) {
                return null;
            }
            avroMessage = record.toString();
        }

        if (avroMessage != null && !isJsonValid(avroMessage)) {
            log.error("Invalid Avro message :" + avroMessage + " for schema " + schema.toString());
            failedEvents.add(new ErroneousEvent(eventObject,
                    "Invalid Avro message :" + avroMessage + " for schema " + schema.toString()));
        } else {
            ReadContext readContext = JsonPath.parse(avroMessage);
            Object avroObj = readContext.read(DEFAULT_JSON_PATH);
            if (avroObj instanceof JSONArray) {
                JsonObject[] eventObjects = gson.fromJson(avroMessage, JsonObject[].class);
                if (isCustomMappingEnabled) {
                    return convertToEventArrayForCustomMapping(eventObjects);
                } else {
                    return convertToEventArrayForDefaultMapping(eventObjects, failedEvents, eventObject);
                }
            } else {
                if (isCustomMappingEnabled) {
                    return convertToSingleEventForCustomMapping(avroMessage);
                } else {
                    return convertToSingleEventForDefaultMapping(avroMessage);
                }
            }
        }
        return null;
    }

    /**
     * The method returns a null instead of a byte[0] to enhance the performance.
     * Creation of empty byte array and length > 0 check for each event conversion is costly
     */
    private Event[] convertToEventArrayForDefaultMapping(JsonObject[] eventObjects, List<ErroneousEvent> failedEvents,
                                                         Object eventObject) throws MappingFailedException {
        List<Event> eventList = new ArrayList<>();
        for (JsonObject jsonEvent : eventObjects) {
            if (jsonEvent.size() < streamAttributes.size()) {
                String errStr = "Avro message " + jsonEvent.toString() + " is not in an accepted format for default " +
                        "Avro mapping. Number of attributes in avro message:" + jsonEvent.size() + " is less  " +
                        "than the number of attributes in stream " + streamDefinition.getId() + ":" +
                        streamAttributes.size();
                log.error(errStr);
                failedEvents.add(new ErroneousEvent(eventObject, errStr));
            } else {
                Event[] event = convertToSingleEventForDefaultMapping(jsonEvent.toString());
                if (event != null) {
                    eventList.add(event[0]);
                }
            }
        }
        return listToArray(eventList);
    }

    /**
     * The method returns a null instead of a byte[0] to enhance the performance.
     * Creation of empty byte array and length > 0 check for each event conversion is costly
     */
    private Event[] convertToSingleEventForDefaultMapping(String avroMessage) throws MappingFailedException {
        List<Event> eventList = new ArrayList<>();
        Event event = new Event(streamAttributesSize);
        Object[] data = event.getData();
        JsonParser parser;
        JsonNode jsonObjectNode;
        int position = -1;

        try {
            parser = jsonFactory.createParser(avroMessage);
        } catch (IOException e) {
            String errStr = "Initializing a parser failed for the event string."
                    + avroMessage;
            log.error(errStr);
            throw new MappingFailedException(errStr, e);
        }

        while (!parser.isClosed()) {
            try {
                JsonToken jsonToken = parser.nextToken();
                if (JsonToken.START_OBJECT.equals(jsonToken)) {
                    jsonToken = parser.nextToken();
                }
                if (JsonToken.FIELD_NAME.equals(jsonToken)) {
                    String key = parser.getCurrentName();
                    position = findDefaultMappingPosition(key);
                    if (position == -1) {
                        String errStr = "Stream \"" + streamDefinition.getId() + "\" does not have an attribute " +
                                "named \"" + key + "\", but the received event " + avroMessage + "does. " +
                                "Hence dropping the message. Check whether the avro message is in a " +
                                "orrect format for default mapping."
                                + avroMessage;
                        log.error(errStr);
                        throw new MappingFailedException(errStr);
                    }
                    jsonToken = parser.nextToken();
                    Attribute.Type type = streamAttributes.get(position).getType();

                    if (JsonToken.VALUE_NULL.equals(jsonToken)) {
                        data[position] = null;
                    } else {
                        switch (type) {
                            case BOOL:
                                if (JsonToken.VALUE_TRUE.equals(jsonToken) || JsonToken.VALUE_FALSE.equals(jsonToken)) {
                                    data[position] = parser.getValueAsBoolean();
                                } else {
                                    String errStr = "Avro message " + avroMessage + "contains incompatible attribute " +
                                            "types and values. Value " + parser.getText() + " is not compatible with " +
                                            "type BOOL. Hence dropping the message.";
                                    log.error(errStr);
                                    throw new MappingFailedException(errStr);
                                }
                                break;
                            case INT:
                                if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                    data[position] = parser.getValueAsInt();
                                } else {
                                    String errStr = "Avro message " + avroMessage + "contains incompatible attribute " +
                                            "types and values. Value " + parser.getText() + " is not compatible with " +
                                            "type INT. Hence dropping the message.";
                                    log.error(errStr);
                                    throw new MappingFailedException(errStr);
                                }
                                break;
                            case DOUBLE:
                                if (JsonToken.VALUE_NUMBER_FLOAT.equals(jsonToken)) {
                                    data[position] = parser.getValueAsDouble();
                                } else {
                                    String errStr = "Avro message " + avroMessage + "contains incompatible attribute " +
                                            "types and values. Value " + parser.getText() + " is not compatible with " +
                                            "type DOUBLE. Hence dropping the message.";
                                    log.error(errStr);
                                    throw new MappingFailedException(errStr);
                                }
                                break;
                            case STRING:
                                data[position] = parser.getValueAsString();
                                break;
                            case FLOAT:
                                if (JsonToken.VALUE_NUMBER_FLOAT.equals(jsonToken) ||
                                        JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                    data[position] = attributeConverter.getPropertyValue(parser.getValueAsString(),
                                            Attribute.Type.FLOAT);
                                } else {
                                    String errStr = "Avro message " + avroMessage + "contains incompatible attribute " +
                                            "types and values. Value " + parser.getText() + " is not compatible with " +
                                            "type FLOAT. Hence dropping the message.";
                                    log.error(errStr);
                                    throw new MappingFailedException(errStr);
                                }
                                break;
                            case LONG:
                                if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                    data[position] = parser.getValueAsLong();
                                } else {
                                    String errStr = "Avro message " + avroMessage + "contains incompatible attribute " +
                                            "types and values. Value " + parser.getText() + " is not compatible with " +
                                            "type LONG. Hence dropping the message.";
                                    log.error(errStr);
                                    throw new MappingFailedException(errStr);
                                }
                                break;
                            case OBJECT:
                                switch (jsonToken) {
                                    case START_OBJECT:
                                    case START_ARRAY:
                                        jsonObjectNode = objectMapper.readTree(avroMessage).findValue(key);
                                        data[position] = gson.fromJson(jsonObjectNode.toString(), Object.class);
                                        handleJsonObject(jsonObjectNode, parser);
                                        break;
                                    case VALUE_STRING:
                                        data[position] = parser.getValueAsString();
                                        break;
                                    case VALUE_NUMBER_INT:
                                        data[position] = parser.getValueAsInt();
                                        break;
                                    case VALUE_NUMBER_FLOAT:
                                        data[position] = attributeConverter.getPropertyValue(parser.getValueAsString(),
                                                Attribute.Type.FLOAT);
                                        break;
                                    case VALUE_TRUE:
                                    case VALUE_FALSE:
                                        data[position] = parser.getValueAsBoolean();
                                        break;
                                    default:
                                        return null;
                                }
                                break;
                            default:
                                return null;
                        }
                    }
                }
            } catch (IOException e) {
                String errStr = "Avro message " + avroMessage + " cannot be converted to siddhi event.";
                log.error(errStr, e);
                throw new MappingFailedException(errStr, e);
            }
        }
        eventList.add(event);
        return eventList.toArray(new Event[0]);
    }

    /**
     * The method returns a null instead of a byte[0] to enhance the performance.
     * Creation of empty byte array and length > 0 check for each event conversion is costly
     */
    private Event[] convertToEventArrayForCustomMapping(JsonObject[] eventObjects) throws MappingFailedException {
        List<Event> eventList = new ArrayList<>();
        for (JsonObject jsonEvent : eventObjects) {
            Event[] event = convertToSingleEventForCustomMapping(jsonEvent.toString());
            if (event != null) {
                eventList.add(event[0]);
            }
        }
        return listToArray(eventList);
    }

    /**
     * The method returns a null instead of a byte[0] to enhance the performance.
     * Creation of empty byte array and length > 0 check for each event conversion is costly
     */
    private Event[] convertToSingleEventForCustomMapping(String avroMessage) throws MappingFailedException {
        Configuration conf = Configuration.defaultConfiguration();
        ReadContext readContext = JsonPath.parse(avroMessage);
        List<Event> eventList = new ArrayList<>();
        Event event = new Event(streamAttributesSize);
        Object[] data = event.getData();
        Object childObject = readContext.read(DEFAULT_JSON_PATH);
        readContext = JsonPath.using(conf).parse(childObject);

        for (MappingPositionData mappingPositionData : this.mappingPositions) {
            int position = mappingPositionData.getPosition();
            Object mappedValue;
            try {
                mappedValue = readContext.read(mappingPositionData.getMapping());
                if (mappedValue == null) {
                    data[position] = null;
                } else {
                    if (streamAttributes.get(position).getType().equals(Attribute.Type.OBJECT)) {
                        JsonParser parser = null;
                        JsonNode jsonObjectNode;
                        try {
                            parser = jsonFactory.createParser(mappedValue.toString());
                            switch (parser.nextToken()) {
                                case START_OBJECT:
                                case START_ARRAY:
                                    jsonObjectNode = objectMapper.readTree(avroMessage).
                                            findValue(mappingPositionData.getMapping());
                                    data[position] = gson.fromJson(jsonObjectNode.toString(), Object.class);
                                    break;
                                case VALUE_STRING:
                                    data[position] = parser.getValueAsString();
                                    break;
                                case VALUE_NUMBER_INT:
                                    data[position] = parser.getValueAsInt();
                                    break;
                                case VALUE_NUMBER_FLOAT:
                                    data[position] = attributeConverter.getPropertyValue(parser.getValueAsString(),
                                            Attribute.Type.FLOAT);
                                    break;
                                case VALUE_TRUE:
                                case VALUE_FALSE:
                                    data[position] = parser.getValueAsBoolean();
                                    break;
                                default:
                                    data[position] = null;
                                    log.warn(parser.nextToken() + " is not a valid data type for event data value. " +
                                            " Hence event data value is set to null");
                            }
                        } catch (IOException e) {
                            String errStr = "Initializing a parser failed for the event string." +
                                    mappedValue.toString();
                            log.error(errStr, e);
                            throw new MappingFailedException(errStr, e);
                        }
                    } else {
                        data[position] = attributeConverter.getPropertyValue(mappedValue.toString(),
                                streamAttributes.get(position).getType());
                    }
                }
            } catch (PathNotFoundException e) {
                if (failOnMissingAttribute) {
                    log.error("Json message " + childObject.toString() +
                            "contains missing attributes. Hence dropping the message.");
                    return null;
                }
                data[position] = null;
            }
        }
        eventList.add(event);
        return eventList.toArray(new Event[0]);
    }

    private boolean isJsonValid(String jsonInString) {
        Gson gson = new Gson();
        try {
            gson.fromJson(jsonInString, Object.class);
            return true;
        } catch (com.google.gson.JsonSyntaxException ex) {
            return false;
        }
    }

    private int findDefaultMappingPosition(String key) {
        for (int i = 0; i < streamAttributes.size(); i++) {
            String attributeName = streamAttributes.get(i).getName();
            if (attributeName.equals(key)) {
                return i;
            }
        }
        return -1;
    }

    private void handleJsonObject(JsonNode objectNode, JsonParser parser) throws IOException {
        Iterator objectFieldIterator = objectNode.fieldNames();
        parser.nextValue();
        while (objectFieldIterator.hasNext()) {
            objectFieldIterator.next();
            JsonToken jsonToken = parser.nextValue();
            if (jsonToken.START_OBJECT.equals(jsonToken)) {
                traverseJsonObject(parser);
            } else if (jsonToken.START_ARRAY.equals(jsonToken)) {
                traverseJsonArray(parser);
            }
        }
    }

    private boolean traverseJsonObject(JsonParser parser) throws IOException {
        JsonToken jsonToken = parser.nextValue();
        if (jsonToken.START_ARRAY.equals(jsonToken)) {
            return traverseJsonArray(parser);
        } else if (jsonToken.START_OBJECT.equals(jsonToken)) {
            return traverseJsonObject(parser);
        } else if (jsonToken.END_OBJECT.equals(jsonToken)) {
            return true;
        }
        traverseJsonObject(parser);
        return false;
    }

    private boolean traverseJsonArray(JsonParser parser) throws IOException {
        JsonToken jsonToken = parser.nextValue();
        if (jsonToken.START_ARRAY.equals(jsonToken)) {
            return traverseJsonArray(parser);
        } else if (jsonToken.END_ARRAY.equals(jsonToken)) {
            return true;
        }
        traverseJsonArray(parser);
        return false;
    }

    private Event[] listToArray(List<Event> eventList) {
        if (!eventList.isEmpty()) {
            return eventList.toArray(new Event[0]);
        } else {
            return null;
        }
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{ByteBuffer.class, byte[].class};
    }

    @Override
    protected boolean allowNullInTransportProperties() {
        return !failOnMissingAttribute;
    }

    /**
     * A POJO class which holds the attribute position in input stream and the user defined mapping.
     */
    private static class MappingPositionData {
        //Attribute position in the output stream.
        private int position;

        //The JSON mapping as defined by the user.
        private String mapping;

        public MappingPositionData(int position, String mapping) {
            this.position = position;
            this.mapping = mapping;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public String getMapping() {
            return mapping;
        }

        public void setMapping(String mapping) {
            this.mapping = mapping;
        }
    }
}
