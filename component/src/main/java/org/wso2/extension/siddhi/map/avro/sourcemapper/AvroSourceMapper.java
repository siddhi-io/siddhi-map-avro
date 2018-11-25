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
package org.wso2.extension.siddhi.map.avro.sourcemapper;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import net.minidev.json.JSONArray;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.map.avro.util.AvroMessageProcessor;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.AttributeMapping;
import org.wso2.siddhi.core.stream.input.source.InputEventHandler;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.util.AttributeConverter;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This mapper converts Avro message stream input to {@link org.wso2.siddhi.core.event.ComplexEventChunk}.
 */

@Extension(
        name = "avro",
        namespace = "sourceMapper",
        description = ""
                + "Avro to Event input mapper. Transports which accepts Avro messages can utilize this extension "
                + "to convert the incoming Avro message to Siddhi event. Users should specify the avro "
                + "schema used to create avro message as a parameter in stream definition. \n"
                + "The specified avro schema is used to convert the avro message into siddhi event.",
        parameters = {
                @Parameter(name = "schema.def",
                        description =
                                "Used to specify the schema of the Avro message. The full schema used to create the " +
                                        "avro message should be specified as quoted json string.",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@source(type='inMemory', topic='user', @map(type='avro', schema .def = \"\"\"" +
                                "{\"type\":\"record\",\"name\":\"userInfo\",\"namespace\":\"user.example\",\"fields\"" +
                                ":[{\"name\":\"name\",\"type\":\"string”}, {\"name\":\"age\",\"type\":\"int\"}]}" +
                                "\"\"\"))\n"
                                + "define stream userStream (name string, age int );\n",
                        description = "Above configuration will do a default Avro input mapping. " +
                                "The input avro message containing user info will be " +
                                "converted  to a siddhi event.\n " +
                                "Expected input is a byte array.")
        }
)

public class AvroSourceMapper extends SourceMapper {

    private static final Logger log = Logger.getLogger(AvroSourceMapper.class);
    private static final String DEFAULT_AVRO_MAPPING_PREFIX = "schema";
    private static final String SCHEMA_IDENTIFIER = "def";
    private static final String DEFAULT_JSON_PATH = "$";

    private StreamDefinition streamDefinition;
    private List<Attribute> streamAttributes;
    private int streamAttributesSize;
    private JsonFactory jsonFactory;
    private AttributeConverter attributeConverter = new AttributeConverter();
    private ObjectMapper objectMapper = new ObjectMapper();
    private Gson gson = new Gson();
    private Schema schema;

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
        schema = getAvroSchema(optionHolder.validateAndGetStaticValue(DEFAULT_AVRO_MAPPING_PREFIX.concat(".").
                        concat(SCHEMA_IDENTIFIER),
                null), streamDefinition.getId());
    }

    private Schema getAvroSchema(String schemaDefinition, String streamName) {
        if (schemaDefinition != null) {
            return new Schema.Parser().parse(schemaDefinition);
        } else {
            throw new SiddhiAppCreationException("Avro Schema is not specified in the stream definition. "
                    + streamName);
        }
    }

    /**
     * Receives an event or events as a byte[] from source, converts it to
     * a {@link org.wso2.siddhi.core.event.ComplexEventChunk}.
     *
     * @param eventObject       the input, given as a byte array
     * @param inputEventHandler input handler
     */
    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler) throws InterruptedException {
        Event[] convertedEvent = null;
        try {
            convertedEvent = convertToEvents(eventObject);
        } catch (Throwable t) {
            log.error("Exception occurred when converting Avro message: " + eventObject.toString() +
                    " to Siddhi Event", t);
        }

        if (convertedEvent != null) {
            inputEventHandler.sendEvents(convertedEvent);
        }
    }

    private Event[] convertToEvents(Object eventObject) {
        Object jsonObj = null;
        Object avroObj;
        String avroMessage;

        if (eventObject instanceof byte[]) {
            try {
                jsonObj = AvroMessageProcessor.deserializeByteArray((byte[]) eventObject, schema);
            } catch (Throwable t) {
                log.error("Error when converting avro message of schema: " + schema.toString() +
                        " to siddhi event. " + t.getMessage() +
                        ". Hence dropping the event.");
            }

            if (jsonObj != null) {
                avroMessage = jsonObj.toString();
                if (!isJsonValid(avroMessage)) {
                    log.error("Invalid Avro message :" + avroMessage + " for schema " + schema.toString());
                } else {
                    ReadContext readContext = JsonPath.parse(avroMessage);
                    avroObj = readContext.read(DEFAULT_JSON_PATH);
                    if (avroObj instanceof JSONArray) {
                        return convertToEventArrayForDefaultMapping(avroMessage);
                    } else {
                        return convertToSingleEventForDefaultMapping(avroMessage);
                    }
                }
            }
        } else {
            log.error("Event object is invalid. Expected Byte Array, but found "
                    + eventObject.getClass().getCanonicalName());
        }
        return null;
    }

    //The method returns a null instead of a byte[0] to enhance the performance.
    //Creation of empty byte array and length > 0 check for each event conversion is costly
    private Event[] convertToEventArrayForDefaultMapping(String avroMessage) {
        Gson gson = new Gson();
        List<Event> eventList = new ArrayList<>();
        JsonObject[] eventObjects = gson.fromJson(avroMessage, JsonObject[].class);
        for (JsonObject jsonEvent : eventObjects) {
            if (jsonEvent.size() < streamAttributes.size()) {
                log.error("Avro message " + jsonEvent.toString() + " is not in an accepted format for default " +
                        "avro mapping. Hence dropping the message.");
                continue;
            } else {
                Event[] event = convertToSingleEventForDefaultMapping(jsonEvent.toString());
                if (event != null) {
                    eventList.add(event[0]);
                }
            }
        }
        if (!eventList.isEmpty()) {
            return eventList.toArray(new Event[0]);
        } else {
            return null;
        }
    }

    //The method returns a null instead of a byte[0] to enhance the performance.
    //Creation of empty byte array and length > 0 check for each event conversion is costly
    private Event[] convertToSingleEventForDefaultMapping(String avroMessage)  {
        List<Event> eventList = new ArrayList<>();
        Event event = new Event(streamAttributesSize);
        Object[] data = event.getData();
        JsonParser parser;
        JsonNode jsonObjectNode;
        int position = -1;

        try {
            parser = jsonFactory.createParser(avroMessage);
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Initializing a parser failed for the event string."
                    + avroMessage);
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
                        log.error("Stream \"" + streamDefinition.getId() +
                                "\" does not have an attribute named \"" + key +
                                "\", but the received event " + avroMessage +
                                " does. Hence dropping the message." +
                                " Check whether the avro message is in a correct format for default mapping.");
                        return null;
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
                                    log.error("Avro message " + avroMessage +
                                            " contains incompatible attribute types and values. Value " +
                                            parser.getText() + " is not compatible with type BOOL. " +
                                            "Hence dropping the message.");
                                    return null;
                                }
                                break;
                            case INT:
                                if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                    data[position] = parser.getValueAsInt();
                                } else {
                                    log.error("Avro message " + avroMessage +
                                            " contains incompatible attribute types and values. Value " +
                                            parser.getText() + " is not compatible with type INT. " +
                                            "Hence dropping the message.");
                                    return null;
                                }
                                break;
                            case DOUBLE:
                                if (JsonToken.VALUE_NUMBER_FLOAT.equals(jsonToken)) {
                                    data[position] = parser.getValueAsDouble();
                                } else {
                                    log.error("Avro message " + avroMessage +
                                            " contains incompatible attribute types and values. Value " +
                                            parser.getText() + " is not compatible with type DOUBLE. " +
                                            "Hence dropping the message.");
                                    return null;
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
                                    log.error("Avro message " + avroMessage +
                                            " contains incompatible attribute types and values. Value " +
                                            parser.getText() + " is not compatible with type FLOAT. " +
                                            "Hence dropping the message.");
                                    return null;
                                }
                                break;
                            case LONG:
                                if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                    data[position] = parser.getValueAsLong();
                                } else {
                                    log.error("Json message " + avroMessage +
                                            " contains incompatible attribute types and values. Value " +
                                            parser.getText() + " is not compatible with type LONG. " +
                                            "Hence dropping the message.");
                                    return null;
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
                log.error ("Avro message " + avroMessage + " cannot be converted to siddhi event.");
                return null;
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

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, byte[].class};
    }

    @Override
    protected boolean allowNullInTransportProperties() {
        return false;
    }
}
