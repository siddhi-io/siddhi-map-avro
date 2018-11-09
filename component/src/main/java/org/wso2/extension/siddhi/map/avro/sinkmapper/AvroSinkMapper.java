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
import org.wso2.siddhi.core.stream.output.sink.SinkListener;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

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
                "This extension is an Event to Avro output mapper. \n" +
                "Transports that publish  messages to Avro sink can utilize this extension" +
                "to convert Siddhi events to " +
                "Avro messages. \n" +
                "User should specify the avro schema in stream definition.\n",
        parameters = {
                @Parameter(name = "schema.def",
                        description =
                                "This specifies the desired avro schema to be used to convert " +
                                        "siddhi event to avro message.\n" +
                                "The schema should be specified as a quoted json string.",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='avro'," +
                                 "schema.def = \"\"\"{\"type\":\"record\",\"name\":\"stock\"," +
                                "\"namespace\":\"stock.example\",\"fields\":[{\"name\":\"symbol\"," +
                                "\"type\":\"string\"},{\"name\":\"price\":\"type\":\"float\"}," +
                                 "{\"name\":\"volume\",\"type\":\"long\"}]}\"\"\"))\n" +
                                 "define stream stockStream (symbol string, price float, volume long);",
                        description = "The above configuration performs a default Avro mapping that "
                                + "generates an Avro message as output byte array.")
        }
)

public class AvroSinkMapper extends SinkMapper {
    private static final Logger log = Logger.getLogger(AvroSinkMapper.class);
    private static final String DEFAULT_AVRO_MAPPING_PREFIX = "schema";
    private static final String SCHEMA_IDENTIFIER = "def";
    private static final String UNDEFINED = "undefined";

    private String[] attributeNameArray;
    private Schema schema;


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
        schema = getAvroSchema (optionHolder.validateAndGetStaticValue(DEFAULT_AVRO_MAPPING_PREFIX.concat(".").
                        concat(SCHEMA_IDENTIFIER),
                null));
    }

    private Schema getAvroSchema(String schemaDefinition) {
        if (schemaDefinition != null) {
            return new Schema.Parser().parse(schemaDefinition);
        } else {
            log.error("Avro Schema is not specified in the stream definition");
            return null;
        }
    }

    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, Map<String, TemplateBuilder>
            payloadTemplateBuilderMap, SinkListener sinkListener) {
        List<byte[]> data = new ArrayList<>();
        if (payloadTemplateBuilderMap == null) {
            data.addAll(constructAvroArrayForDefaultMapping(events));
        }
        if (!data.isEmpty()) {
                sinkListener.publish(data);
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, Map<String, TemplateBuilder>
            payloadTemplateBuilderMap, SinkListener sinkListener) {
        byte[] data = null;
        if (payloadTemplateBuilderMap == null) {
            data = constructAvroForDefaultMapping(event);
        }
        if (data != null && data.length > 0) {
            sinkListener.publish(data);
        }
    }

    private List<byte[]> constructAvroArrayForDefaultMapping(Event[] eventObj) {
        List<byte[]> convertedEvents = new ArrayList<>();
            for (Event event: eventObj) {
                byte[] convertedEvent = constructAvroForDefaultMapping(event);
                if (convertedEvent != null && convertedEvent.length > 0) {
                    convertedEvents.add(convertedEvent);
                }
            }
            return convertedEvents;
    }

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
            log.error("Invalid object type. " + eventObj.toString() +
                    " cannot be converted to an Avro Message");
            return new byte[0];
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
                if (attributeValue.getClass() == String.class) {
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
                attributeValue = UNDEFINED;
                innerParentObject.addProperty(attributeName, attributeValue.toString());
            }
        }
        return innerParentObject;
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{byte[].class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }
}
