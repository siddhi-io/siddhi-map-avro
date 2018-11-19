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
package org.wso2.extension.siddhi.map.avro.sinkmapper;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.map.avro.AvroSchemaDefinitions;
import org.wso2.extension.siddhi.map.avro.util.AvroMessageProcessor;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class AvroSinkMapperTestCase {
    private static Logger log = Logger.getLogger(AvroSinkMapperTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private boolean eventArrived;
    private boolean innerAssertionsPass;

    @BeforeMethod
    public void init() {
        count.set(0);
        eventArrived = false;
        innerAssertionsPass = false;
    }

    @Test(description = "Check Avro sink maps output siddhi events to " +
            "avro byte[] message with a flat avro schema")
    public void avroSinkMapperTest1() throws InterruptedException {
        log.info("Testing Avro Sink Mapper with flat schema structure");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream FooStream (name string, favorite_number int); " +
                "@sink(type='inMemory', topic='user', @map(type='avro', schema.def = \"\"\"" +
                "{\"namespace\": \"example.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"avro.User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\": \"string\"}\n" +
                " ]\n" +
                "}\"\"\"))" +
                "define stream BarStream (name string); ";
        String query = "" +
                "from FooStream " +
                "select name " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                Object events = AvroMessageProcessor.deserializeByteArray((byte[]) o,
                        AvroSchemaDefinitions.getFlatSchema());
                if (events != null) {
                    log.info(events);
                    eventArrived = true;
                    count.getAndIncrement();
                    switch (count.get()) {
                        case 1:
                            AssertJUnit.assertEquals("WSO2", ((GenericRecord) events).get("name").toString());
                            innerAssertionsPass = true;
                            break;
                        case 2:
                            AssertJUnit.assertEquals("IBM", ((GenericRecord) events).get("name").toString());
                            innerAssertionsPass = true;
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }

            @Override
            public String getTopic() {
                return "user";
            }
        };
        InMemoryBroker.subscribe(subscriber);

        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        fooStream.send(new Object[]{"WSO2", 4});
        fooStream.send(new Object[]{"IBM", 5});
        InMemoryBroker.unsubscribe(subscriber);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPass);
        AssertJUnit.assertEquals(2, count.get());
    }

    @Test(description = "Check Avro sink maps output siddhi events to " +
            "avro byte[] message with a complex avro schema")
    public void avroSinkMapperTest2() throws InterruptedException {
        log.info("Testing Avro Sink Mapper with complex schema structure");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream FooStream (name string, age int, address object); " +
                "@sink(type='inMemory', topic='userInfo', @map(type='avro', schema.def = \"\"\"{" +
                "    \"type\" : \"record\",\n" +
                "    \"name\" : \"userInfo\",\n" +
                "    \"namespace\" : \"avro.userInfo\",\n" +
                "    \"fields\" : [{\"name\" : \"username\",\"type\" : \"string\"},\n" +
                "                  {\"name\" : \"age\",\"type\" : \"int\"},\n" +
                "                  {\"name\" : \"address\", \"type\" : {\"type\":\"record\",\n" +
                "                                           \"name\":\"addressField\",\n" +
                "                                           \"fields\":[\n" +
                "                                           {\"name\":\"street\",\"type\":\"string\"},\n" +
                "                                           {\"name\":\"city\",\"type\":{\"type\":\"record\",\n" +
                "                                           \"name\":\"countryField\",\n" +
                "                                           \"fields\":[\n" +
                "                                           {\"name\":\"city\",\"type\":\"string\"},\n" +
                "                                           {\"name\":\"country\",\"type\": \"string\"}]}} \n" +
                "            ]\n" +
                "        } }\n" +
                "   ]\n" +
                "}\"\"\"\n))\n" +
                "define stream BarStream (username string,age int, address object); ";
        String query = "" +
                "from FooStream " +
                "select name as username, age, address " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object obj) {
                Object events = AvroMessageProcessor.deserializeByteArray((byte[]) obj,
                        AvroSchemaDefinitions.getComplexSchema());
                if (events != null) {
                    log.info(events);
                    eventArrived = true;
                    count.getAndIncrement();
                    Object address = ((GenericRecord) events).get("address");
                    Object country = ((GenericRecord) address).get("country");
                    AssertJUnit.assertEquals("WSO2", ((GenericRecord) events).get("username").toString());
                    AssertJUnit.assertEquals("Palm Grove", ((GenericRecord) address).get("street").toString());
                    AssertJUnit.assertEquals("SriLanka", ((GenericRecord) country).get("country").toString());
                    innerAssertionsPass = true;
                }
            }

            @Override
            public String getTopic() {
                return "userInfo";
            }
        };
        InMemoryBroker.subscribe(subscriber);

        Map<String, String> country = new LinkedHashMap<>();
        country.put("city", "Colombo");
        country.put("country", "SriLanka");

        Map<String, Object> address = new LinkedHashMap<>();
        address.put("street", "Palm Grove");
        address.put("city", country);

        InputHandler handler = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        handler.send(new Object[]{"WSO2", 26, address});
        InMemoryBroker.unsubscribe(subscriber);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPass);
        AssertJUnit.assertEquals(1, count.get());
    }

    @Test(description = "Check Avro sink mapper drops the event when the event is not " +
            "compatible with specified avro schema")
    public void avroSinkMapperTest3() throws InterruptedException {
        log.info("Testing Avro Sink Mapper for different schema and stream parameter names ");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream FooStream (name string, favorite_number int); " +
                "@sink(type='inMemory', topic='userData', @map(type='avro', schema.def = \"\"\"" +
                "{\"namespace\": \"avro.user\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"user\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"username\", \"type\": \"string\"}\n" +
                " ]\n" +
                "}\"\"\"))" +
                "define stream BarStream (name string); ";
        String query = "" +
                "from FooStream " +
                "select name " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                eventArrived = true;
            }

            @Override
            public String getTopic() {
                return "userData";
            }
        };
        InMemoryBroker.subscribe(subscriber);

        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        Logger logger = Logger.getLogger(AvroSinkMapper.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Layout layout = new SimpleLayout();
        Appender appender = new WriterAppender(layout, out);
        logger.addAppender(appender);

        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"WSO2", 4});

        AssertJUnit.assertEquals("ERROR - Error when converting siddhi event: [WSO2] " +
                "to Avro message of schema: {\"type\":\"record\",\"name\":\"user\",\"namespace\":\"avro.user\"," +
                "\"fields\":[{\"name\":\"username\",\"type\":\"string\"}]}." +
                "Expected field name not found: username. Hence dropping the event.", out.toString().trim());
        AssertJUnit.assertFalse(eventArrived);
        InMemoryBroker.unsubscribe(subscriber);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Check Avro sink mapper drops the event when sink avro schema " +
            "contain union data type fields")
    public void avroSinkMapperTest4() throws InterruptedException {
        log.info("Testing Avro Sink Mapper for schema with union data type fields");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream userStream (name string, age int); " +
                "@sink(type='inMemory', topic='userProfile', @map(type='avro', schema.def = \"\"\"" +
                "{\"namespace\": \"avro.userProfile\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"userProfile\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\": \"string\"},\n" +
                "     {\"name\": \"age\", \"type\": [\"int\",\"null\"]}\n" +
                " ]\n" +
                "}\"\"\"))" +
                "define stream BarStream (name string, age int); ";
        String query = "" +
                "from userStream " +
                "select name, age " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                eventArrived = true;
            }

            @Override
            public String getTopic() {
                return "userProfile";
            }
        };
        InMemoryBroker.subscribe(subscriber);
        InputHandler userStream = siddhiAppRuntime.getInputHandler("userStream");

        Logger logger = Logger.getLogger(AvroSinkMapper.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Layout layout = new SimpleLayout();
        Appender appender = new WriterAppender(layout, out);
        logger.addAppender(appender);

        siddhiAppRuntime.start();
        userStream.send(new Object[]{"WSO2", 4});

        AssertJUnit.assertEquals("ERROR - Error when converting siddhi event: [WSO2, 4] " +
                "to Avro message of schema: {\"type\":\"record\",\"name\":\"userProfile\"," +
                "\"namespace\":\"avro.userProfile\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}" +
                ",{\"name\":\"age\",\"type\":[\"int\",\"null\"]}]}." +
                "Expected start-union. Got VALUE_NUMBER_INT. Hence dropping the event.", out.toString().trim());
        AssertJUnit.assertFalse(eventArrived);
        InMemoryBroker.unsubscribe(subscriber);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Check Avro sink mapper conversion for an array of siddhi events")
    public void avroSinkMapperTest5() throws InterruptedException {
        log.info("Testing Avro Sink Mapper for event array");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream FooStream (name string, age int); " +
                "@sink(type='inMemory', topic='stock', @map(type='avro', schema.def = \"\"\"" +
                "{\"namespace\": \"example.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"avro.User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\":\"string\"}\n" +
                " ]\n" +
                "}\"\"\"))" +
                "define stream BarStream (name string); ";
        String query = "" +
                "from FooStream " +
                "select name " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object obj) {
                if (obj instanceof List) {
                    for (byte[] event : (List<byte[]>) obj) {
                        Object message = AvroMessageProcessor.deserializeByteArray(event,
                                AvroSchemaDefinitions.getFlatSchema());
                            log.info(message);
                            eventArrived = true;
                            count.getAndIncrement();
                            switch (count.get()) {
                                case 1:
                                    AssertJUnit.assertEquals("WSO2", ((GenericRecord) message).get("name").toString());
                                    innerAssertionsPass = true;
                                    break;
                                case 2:
                                    AssertJUnit.assertEquals("IBM", ((GenericRecord) message).get("name").toString());
                                    innerAssertionsPass = true;
                                    break;
                                default:
                                    AssertJUnit.fail();
                        }
                    }
                }
            }

            @Override
            public String getTopic() {
                return "stock";
            }
        };
        InMemoryBroker.subscribe(subscriber);

        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        Event[] multipleEvents = new Event[2];
        Event event0 = new Event(-1, new Object[]{"WSO2", 4});
        Event event1 = new Event(-1, new Object[]{"IBM", 5});
        multipleEvents[0] = event0;
        multipleEvents[1] = event1;
        siddhiAppRuntime.start();
        fooStream.send(multipleEvents);

        AssertJUnit.assertTrue(innerAssertionsPass);
        AssertJUnit.assertEquals(2, count.get());
        InMemoryBroker.unsubscribe(subscriber);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Check siddhi app creation fails when stream " +
            "definition does not contain schema definition", expectedExceptions = SiddhiAppCreationException.class)
    public void avroSinkMapperTest6() throws InterruptedException {
        log.info("Testing Avro Sink Mapper without schema definition");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream FooStream (name string, favorite_number int); " +
                "@sink(type='inMemory', topic='user', @map(type='avro'))" +
                "define stream BarStream (name string); ";
        String query = "" +
                "from FooStream " +
                "select name " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = null;
        try {
           siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("Error on 'TestApp' @ Line: 1. Position: 135, near " +
                    "'@sink(type='inMemory', topic='user', @map(type='avro'))'. " +
                    "Avro Schema is not specified in the stream definition. BarStream", e.getMessage());
            throw e;
        }
    }
}
