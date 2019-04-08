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
package org.wso2.extension.siddhi.map.avro.util.schema;

import com.google.gson.Gson;
import io.siddhi.query.api.definition.Attribute;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class representing a Avro Record schema.
 */
public class RecordSchema {
    private static final Logger log = Logger.getLogger(RecordSchema.class);
    private static final String ATTRIBUTE_NAME = "name";
    private static final String ATTRIBUTE_TYPE = "type";
    private static final String SCHEMA_TYPE = "record";

    private String name;
    private String type;
    private List<Map<String, String>> fields;

    public RecordSchema() {
        this.type = SCHEMA_TYPE;
    }

    public Schema generateAvroSchema(List<Attribute> streamAttributes, String streamName) throws SchemaParseException {
        List<Map<String, String>> schemaFields = new ArrayList<>();
        Gson gson = new Gson();

        for (Attribute attribute: streamAttributes) {
            Map<String, String> fieldAttributes = new HashMap<>();
            fieldAttributes.put(ATTRIBUTE_NAME, attribute.getName());
            fieldAttributes.put(ATTRIBUTE_TYPE, attributeTypeToString(attribute.getType(), attribute.getName()));
            schemaFields.add(fieldAttributes);
        }
        this.name = streamName;
        this.fields = schemaFields;
        return new Schema.Parser().parse(gson.toJson(this));
    }

    private String attributeTypeToString(Attribute.Type type, String name) {
        Schema.Type dataType = null;
        switch(type) {
            case STRING:
                dataType = Schema.Type.STRING;
                break;
            case INT:
                dataType = Schema.Type.INT;
                break;
            case FLOAT:
                dataType = Schema.Type.FLOAT;
                break;
            case DOUBLE:
                dataType = Schema.Type.DOUBLE;
                break;
            case BOOL:
                dataType = Schema.Type.BOOLEAN;
                break;
            case LONG:
                dataType = Schema.Type.LONG;
                break;
            default:
                log.error("Stream attribute: " + name + " has data type: " + type.toString() +
                        " which is not supported by avro schema generation ");
                return null;
        }
        return dataType.toString().toLowerCase();
    }
}
