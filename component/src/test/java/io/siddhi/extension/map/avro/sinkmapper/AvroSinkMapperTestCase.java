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
package io.siddhi.extension.map.avro.sinkmapper;

import feign.FeignException;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.extension.map.avro.AvroSchemaDefinitions;
import io.siddhi.extension.map.avro.ConnectionTestUtil;
import io.siddhi.extension.map.avro.UnitTestAppender;
import io.siddhi.extension.map.avro.util.AvroMessageProcessor;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class AvroSinkMapperTestCase {
    private static final Logger log = (Logger) LogManager.getLogger(AvroSinkMapperTestCase.class);
    private static String schemaRegistryURL = "http://localhost:8081";
    private AtomicInteger count = new AtomicInteger();
    private boolean eventArrived;
    private boolean innerAssertionsPass;

    @BeforeMethod
    public void init() {
        count.set(0);
        eventArrived = false;
        innerAssertionsPass = false;
    }

    @Test(description = "Check Avro sink maps output siddhi events to avro byte[] message " +
            "with a flat avro schema")
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
                List<ErroneousEvent> failedEvents = new ArrayList<>(0);
                Object events = AvroMessageProcessor.deserializeByteArray(((ByteBuffer) o).array(),
                        AvroSchemaDefinitions.getFlatSchema(), failedEvents);
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

    @Test(description = "Check Avro sink maps output siddhi events to avro byte[] message " +
            "with a complex avro schema")
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
                List<ErroneousEvent> failedEvents = new ArrayList<>(0);
                Object events = AvroMessageProcessor.deserializeByteArray(((ByteBuffer) obj).array(),
                        AvroSchemaDefinitions.getComplexSchema(), failedEvents);
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

        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"WSO2", 4});
        AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                get("UnitTestAppender")).getMessages().contains("Error when converting siddhi event: [WSO2] " +
                "to Avro message of schema: {\"type\":\"record\",\"name\":\"user\",\"namespace\":\"avro.user\"," +
                "\"fields\":[{\"name\":\"username\",\"type\":\"string\"}]}." +
                "Expected field name not found: username. Hence dropping the event."));
        AssertJUnit.assertFalse(eventArrived);
        InMemoryBroker.unsubscribe(subscriber);
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
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

        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        siddhiAppRuntime.start();
        userStream.send(new Object[]{"WSO2", 4});

        AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                get("UnitTestAppender")).getMessages().contains("Error when converting " +
                "siddhi event: [WSO2, 4] " +
                "to Avro message of schema: {\"type\":\"record\",\"name\":\"userProfile\"," +
                "\"namespace\":\"avro.userProfile\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}" +
                ",{\"name\":\"age\",\"type\":[\"int\",\"null\"]}]}." +
                "Expected start-union. Got VALUE_NUMBER_INT. Hence dropping the event."));
        AssertJUnit.assertFalse(eventArrived);
        InMemoryBroker.unsubscribe(subscriber);
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(description = "Check Avro sink mapper default conversion for an array of siddhi events")
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
                List<ErroneousEvent> failedEvents = new ArrayList<>(0);
                if (obj instanceof ByteBuffer) {
                    Object message = AvroMessageProcessor.deserializeByteArray(((ByteBuffer) obj).array(),
                            AvroSchemaDefinitions.getFlatSchema(), failedEvents);
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

    @Test(description = "Check siddhi app creation fails when stream definition has attributes " +
            "of type OBJECT.", expectedExceptions =
            {SiddhiAppCreationException.class, SchemaParseException.class})
    public void avroSinkMapperTest6() {
        log.info("Testing Avro Sink Mapper without schema definition");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream FooStream (name object, favorite_number int); " +
                "@sink(type='inMemory', topic='user', @map(type='avro'))" +
                "define stream BarStream (name object); ";
        String query = "" +
                "from FooStream " +
                "select name " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();

        UnitTestAppender appender = new UnitTestAppender("UnitTestAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        siddhiManager.createSiddhiAppRuntime(streams + query);

        AssertJUnit.assertTrue(((UnitTestAppender) logger.getAppenders().
                get("UnitTestAppender")).getMessages().contains("Stream attribute: name has data " +
                "type: OBJECT " +
                "which is not supported by avro schema generation"));
        logger.removeAppender(appender);
    }

    @Test(description = "Check Avro sink generates avro schema from stream attributes and " +
            "convert siddhi events to avro messages")
    public void avroSinkMapperTest7() throws InterruptedException {
        log.info("Testing Avro Sink Mapper with schema generation");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream FooStream (symbol string, price float, volume double); " +
                "@sink(type='inMemory', topic='stock', @map(type='avro'))" +
                "define stream BarStream (symbol string, price float, volume double); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler input = siddhiAppRuntime.getInputHandler("FooStream");
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object obj) {
                List<ErroneousEvent> failedEvents = new ArrayList<>(0);
                Object event = AvroMessageProcessor.deserializeByteArray(((ByteBuffer) obj).array(),
                        AvroSchemaDefinitions.getFlatAvroSchema(), failedEvents);
                if (event != null) {
                    log.info(event);
                    eventArrived = true;
                    count.getAndIncrement();
                    AssertJUnit.assertEquals("WSO2", ((GenericRecord) event).get("symbol").toString());
                    innerAssertionsPass = true;
                }
            }

            @Override
            public String getTopic() {
                return "stock";
            }
        };
        InMemoryBroker.subscribe(subscriber);

        siddhiAppRuntime.start();
        input.send(new Object[]{"WSO2", 102.5f, 55.66});
        InMemoryBroker.unsubscribe(subscriber);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPass);
        AssertJUnit.assertEquals(1, count.get());
    }

    @Test(description = "Check Avro sink maps output siddhi events to avro byte[] message " +
            "with custom mapping")
    public void avroSinkMapperTest8() throws InterruptedException {
        log.info("Testing Avro Sink Mapper with custom mapping");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream FooStream (name string, age int, street string, city string, country string); " +
                "@sink(type='inMemory', topic='user', @map(type='avro', schema.def = \"\"\"" +
                "{\n" +
                "    \"type\" : \"record\",\n" +
                "    \"name\" : \"userInfo\",\n" +
                "    \"namespace\" : \"avro.userInfo\",\n" +
                "    \"fields\" : [{\"name\" : \"username\",\"type\" : \"string\"},\n" +
                "                  {\"name\" : \"age\",\"type\" : \"int\"},\n" +
                "                  {\"name\" : \"address\", \"type\" :{\"type\":\"record\",\n" +
                "                                           \"name\":\"addressField\",\n" +
                "                                           \"fields\":[\n" +
                "                                           {\"name\":\"street\",\"type\":\"string\"},\n" +
                "                                           {\"name\":\"country\",\"type\":{\"type\":\"record\",\n" +
                "                                           \"name\":\"countryField\",\n" +
                "                                           \"fields\":[\n" +
                "                                           {\"name\":\"city\",\"type\":\"string\"},\n" +
                "                                           {\"name\":\"country\",\"type\": \"string\"}]}} \n" +
                "            ]\n" +
                "        } }\n" +
                "   ]\n" +
                "}\n" +
                "\"\"\"," +
                "@payload(\"\"\"{\n\"username\":\"{{name}}\"\n," +
                "\"age\":{{age}},\n" +
                "\"address\":\n {\"street\":\"{{street}}\",\n       " +
                "\"country\":\n{\"city\":\"{{city}}\",\n         " +
                "\"country\":\"{{country}}\"\n      }\n" +
                "   }\n }\"\"\")))" +
                "define stream BarStream (name string, age int, street string, city string, country string); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object obj) {
                if (obj instanceof ByteBuffer) {
                    List<ErroneousEvent> failedEvents = new ArrayList<>(0);
                    Object message = AvroMessageProcessor.deserializeByteArray(((ByteBuffer) obj).array(),
                            AvroSchemaDefinitions.getComplexSchema(), failedEvents);
                    log.info(message);
                    eventArrived = true;
                    count.getAndIncrement();
                    switch (count.get()) {
                        case 1:
                            AssertJUnit.assertEquals("WSO2", ((GenericRecord) message).get("username").
                                    toString());
                            innerAssertionsPass = true;
                            break;
                        case 2:
                            AssertJUnit.assertEquals("WSO2-US", ((GenericRecord) message).get("username").
                                    toString());
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

        Event[] multipleEvents = new Event[2];
        Event event0 = new Event(-1, new Object[]{"WSO2", 26, "Palm Grove", "Colombo", "SriLanka"});
        Event event1 = new Event(-1, new Object[]{"WSO2-US", 25, "Castro Street", "Mountain View", "Canada"});
        multipleEvents[0] = event0;
        multipleEvents[1] = event1;
        siddhiAppRuntime.start();
        fooStream.send(multipleEvents);

        InMemoryBroker.unsubscribe(subscriber);
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPass);
        AssertJUnit.assertEquals(2, count.get());
    }

    @Test(description = "Check Avro sink maps siddhi events to avro message by retrieving the schema " +
            "from schema registry.")
    public void avroSinkMapperTest9() throws InterruptedException {
        log.info("Testing Avro Sink Mapper with schema registry");
        String streams = "" +
                "@App:name('TestApp')" +
                "define stream FooStream (username string, surname string, bDay long); " +
                "@sink(type='inMemory', topic='stock', @map(type='avro', schema.id = '22'," +
                "schema.registry = 'http://localhost:8081'))" +
                "define stream BarStream (firstName string, lastName string, birthDate long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            ConnectionTestUtil.connectToSchemaRegistry(schemaRegistryURL);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("FooStream");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    eventArrived = true;
                    count.addAndGet(events.length);
                    EventPrinter.print(events);
                    AssertJUnit.assertEquals("WSO2", events[0].getData(0));
                    innerAssertionsPass = true;
                }
            });

            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{"WSO2", "WSO2", 1989L});
            siddhiAppRuntime.shutdown();

            AssertJUnit.assertTrue(eventArrived);
            AssertJUnit.assertTrue(innerAssertionsPass);
            AssertJUnit.assertEquals(1, count.get());
        } catch (FeignException e) {
            log.warn("Schema Registry at " + schemaRegistryURL + " may not be available.");
        }
    }
}

