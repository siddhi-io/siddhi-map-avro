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
package io.siddhi.extension.map.avro.sourcemapper;

import feign.FeignException;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.util.transport.SubscriberUnAvailableException;
import io.siddhi.extension.map.avro.AvroSchemaDefinitions;
import io.siddhi.extension.map.avro.ConnectionTestUtil;
import io.siddhi.extension.map.avro.util.AvroMessageProcessor;
import io.siddhi.extension.map.avro.util.schema.RecordSchema;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class AvroSourceMapperTestCase {
    private static Logger log = Logger.getLogger(AvroSourceMapperTestCase.class);
    private static String schemaRegistryURL = "http://localhost:8081";
    private AtomicInteger count = new AtomicInteger();
    private volatile boolean eventArrived;
    private boolean innerAssertionsPass;

    @BeforeMethod
    public void init() {
        eventArrived = false;
        innerAssertionsPass = false;
        count.set(0);
    }

    @Test(description = "Check Avro source maps avro messages to siddhi events with a flat schema")
    public void avroSourceMapperTest1() throws SubscriberUnAvailableException {
        log.info("Testing Avro Source Mapper with flat schema structure");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='user', @map(type='avro', schema.def = \"\"\"" +
                "{\"namespace\": \"avro.user\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"user\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\": \"string\"},\n" +
                "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}\n" +
                " ]\n" +
                "}\"\"\"))" +
                "define stream FooStream (name string, favorite_number int); " +
                "define stream BarStream (name string, favorite_number int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                eventArrived = true;
                count.addAndGet(events.length);
                EventPrinter.print(events);
                AssertJUnit.assertEquals("WSO2", events[0].getData(0));
                AssertJUnit.assertEquals(null, events[0].getData(1));
                innerAssertionsPass = true;
            }
        });

        byte[] data = AvroSchemaDefinitions.createSimpleAvroMessage();
        siddhiAppRuntime.start();
        InMemoryBroker.publish("user", ByteBuffer.wrap(data));
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPass);
        AssertJUnit.assertEquals(1, count.get());
    }

    @Test(description = "Check Avro source mapper maps avro messages to siddhi events with default " +
            "mapping for complex schema")
    public void avroSourceMapperTest2() throws SubscriberUnAvailableException {
        log.info("Testing Avro Source Mapper with complex schema structure");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='userInfo', @map(type='avro', schema.def = \"\"\"{" +
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
                "}\"\"\"\n))" +
                "define stream FooStream (username string, age int, address object); " +
                "define stream BarStream (username string, address object); ";
        String query = "" +
                "from FooStream " +
                "select username, address " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                eventArrived = true;
                count.addAndGet(events.length);
                EventPrinter.print(events);
                AssertJUnit.assertEquals("WSO2", events[0].getData(0));
                AssertJUnit.assertEquals("{street=Palm Grove, city={city=Colombo, country=SriLanka}}",
                        events[0].getData(1).toString());
                innerAssertionsPass = true;
            }
        });

        byte[] data = AvroSchemaDefinitions.createComplexAvroMessage();

        siddhiAppRuntime.start();
        InMemoryBroker.publish("userInfo", ByteBuffer.wrap(data));
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPass);
        AssertJUnit.assertEquals(1, count.get());
    }

    @Test(description = "Check Avro source maps avro message with array schema to multiple events" +
            "with default mapping")
    public void avroSourceMapperTest3() throws SubscriberUnAvailableException {
        log.info("Testing Avro Source Mapper with multiple events");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='stock', @map(type='avro', schema.def = \"\"\"{ \"type\" : \"array\"," +
                "\"items\" : {\"type\" : \"record\", \"name\" : \"user\", \"fields\": [{\"name\":\"name\"," +
                "\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\": [\"int\", \"null\"]}]}}\"\"\"))" +
                "define stream FooStream (name string, favorite_number int); " +
                "define stream BarStream (name string, favorite_number int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                eventArrived = true;
                count.addAndGet(events.length);
                EventPrinter.print(events);
                AssertJUnit.assertEquals("WSO2", events[0].getData(0));
                AssertJUnit.assertEquals(null, events[0].getData(1));
                AssertJUnit.assertEquals("IBM", events[1].getData(0));
                AssertJUnit.assertEquals(100, events[1].getData(1));
                innerAssertionsPass = true;

            }
        });

        byte[] data = AvroSchemaDefinitions.createArrayOfAvroMessage();
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", ByteBuffer.wrap(data));
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(innerAssertionsPass);
        AssertJUnit.assertEquals(2, count.get());
    }

    @Test(description = "Check avro messages are converted to siddhi events when receiving data from kafka source")
    public void avroSourceMapperTest4() throws InterruptedException {
        try {
            log.info("Testing Avro Source Mapper with kafka Source");
            String topics[] = new String[]{"single_topic"};
            ConnectionTestUtil.createTopic(topics, 1);
            Thread.sleep(1000);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan')" +
                     "@source(type='kafka',topic.list='single_topic',group.id='test_single_topic'," +
                      "threading.option='single.thread',bootstrap.servers='localhost:9092'," +
                      "is.binary.message ='true'," +
                      "@map(type='avro', schema.def= \"\"\" " +
                      "{\"namespace\": \"avro.stock\"," +
                      "\"type\": \"record\",\"name\": \"stock\"," +
                      "\"fields\": [" +
                            "{\"name\": \"symbol\", \"type\": \"string\"},\n" +
                            "{\"name\": \"price\",  \"type\": \"float\"},\n" +
                            "{\"name\": \"volume\",  \"type\": \"double\"}," +
                            "{\"name\": \"isDelivered\",  \"type\": \"boolean\"}\n" +
                      "]}\"\"\"))" +
                      "define stream FooStream (symbol string, price float, volume double, isDelivered bool);" +
                      "define stream BarStream (symbol string, price float, volume double, isDelivered bool);" +
                      "from FooStream " +
                      "select * " +
                      "insert into BarStream;");
            siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    EventPrinter.print(events);
                    for (Event event : events) {
                        eventArrived = true;
                        count.addAndGet(events.length);
                        AssertJUnit.assertEquals("WSO2", event.getData(0));
                        AssertJUnit.assertEquals(20.5f, event.getData(1));
                        AssertJUnit.assertEquals(40.42, event.getData(2));
                        AssertJUnit.assertTrue((boolean) event.getData(3));
                        innerAssertionsPass = true;
                    }
                }
            });
            ConnectionTestUtil.initAvroSchema();
            siddhiAppRuntime.start();
            ConnectionTestUtil.kafkaPublisher(topics, 1, 1, false, null);
            Thread.sleep(1000);
            ConnectionTestUtil.deleteTopic(topics);
            siddhiAppRuntime.shutdown();

            AssertJUnit.assertTrue(eventArrived);
            AssertJUnit.assertTrue(innerAssertionsPass);
            AssertJUnit.assertEquals(1, count.get());
        } catch (ZkTimeoutException ex) {
            log.warn("No zookeeper may not be available.", ex);
        }
    }

    @Test(description = "Check Avro source drops an avro message that is incompatible with " +
            "specified avro schema ")
    public void avroSourceMapperTest5() throws SubscriberUnAvailableException {
        log.info("Testing Avro Source Mapper for incompatible avro message with avro schema");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='user', @map(type='avro', schema.def = \"\"\"" +
                "{\"namespace\": \"avro.user\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"user\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\":\"string\"},\n" +
                "     {\"name\": \"favorite_number\", \"type\": \"float\"}\n" +
                " ]\n" +
                "}\"\"\"))" +
                "define stream FooStream (name string, favorite_number int); " +
                "define stream BarStream (name string, favorite_number int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                eventArrived = true;
            }
        });

        byte[] data = AvroSchemaDefinitions.createSimpleAvroMessage();
        Logger logger = Logger.getLogger(AvroMessageProcessor.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Layout layout = new SimpleLayout();
        Appender appender = new WriterAppender(layout, out);
        logger.addAppender(appender);

        siddhiAppRuntime.start();
        InMemoryBroker.publish("user", ByteBuffer.wrap(data));

        AssertJUnit.assertEquals("ERROR - Error occured when deserializing avro byte stream " +
                "conforming to schema {\"type\":\"record\",\"name\":\"user\",\"namespace\":\"avro.user\"," +
                "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":" +
                "\"float\"}]}. Hence dropping the event.", out.toString().trim());
        AssertJUnit.assertFalse(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Check if Avro source generates avro schema from stream attributes and " +
            "convert avro messages to siddhi events")
    public void avroSourceMapperTest6() throws SubscriberUnAvailableException {
        log.info("Testing Avro Source Mapper with schema generation");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='user', @map(type='avro'))" +
                "define stream FooStream (symbol string, price float, volume double); " +
                "define stream BarStream (symbol string, price float, volume double); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                eventArrived = true;
                count.addAndGet(events.length);
                EventPrinter.print(events);
                AssertJUnit.assertEquals("WSO2", events[0].getData(0));
            }
        });

        byte[] data = AvroSchemaDefinitions.createAvroMessage();
        siddhiAppRuntime.start();
        InMemoryBroker.publish("user", ByteBuffer.wrap(data));
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(1, count.get());
    }

    @Test(description = "Check if SiddhiApp creation fails with an error when schema is generated " +
            "from stream attributes and stream has attributes of object type",
            expectedExceptions = SiddhiAppCreationException.class)
    public void avroSourceMapperTest7() {
        log.info("Testing Avro Source Mapper with schema generation with unsupported stream data types");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='user', @map(type='avro'))" +
                "define stream FooStream (symbol string, price object, volume double); " +
                "define stream BarStream (symbol string, price object, volume double); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();

        Logger logger = Logger.getLogger(RecordSchema.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Layout layout = new SimpleLayout();
        Appender appender = new WriterAppender(layout, out);
        logger.addAppender(appender);

        siddhiManager.createSiddhiAppRuntime(streams + query);

        AssertJUnit.assertEquals("ERROR - Stream attribute: price has data type:OBJECT which " +
                "is not supported by avro schema generation.", out.toString().trim());
    }

    @Test(description = "Check Avro source mapper for custom avro message mapping to siddhi events" +
            " with a complex schema")
    public void avroSourceMapperTest8() throws SubscriberUnAvailableException {
        log.info("Testing Avro Source Mapper with custom mapping");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='userInfo', @map(type='avro', schema.def = \"\"\"{" +
                "    \"type\" : \"record\",\n" +
                "    \"name\" : \"userInfo\",\n" +
                "    \"namespace\" : \"avro.userInfo\",\n" +
                "    \"fields\" : [{\"name\" : \"username\",\"type\" : \"string\"},\n" +
                "                  {\"name\" : \"age\",\"type\" : \"int\"},\n" +
                "                  {\"name\" : \"address\", \"type\" : {\"type\":\"record\",\n" +
                "                                                       \"name\":\"addressField\",\n" +
                "                                                       \"fields\":[\n" +
                "                                                        {\"name\":\"street\",\"type\":\"string\"},\n" +
                "                                                        {\"name\":\"city\",\"type\":\"string\" } \n" +
                "            ]\n" +
                "        } }\n" +
                "   ]\n" +
                "}\"\"\",\n" +
                "@attributes(name =\"username\", age =\"age\", street=\"address.street\", city =\"address.city\")))\n" +
                "define stream FooStream (name string, age int, street string, city string); " +
                "define stream BarStream (name string, age int, street string, city string); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                eventArrived = true;
                count.addAndGet(events.length);
                EventPrinter.print(events);
                AssertJUnit.assertEquals("Palm Grove", events[0].getData(2));
            }
        });

        byte[] data = AvroSchemaDefinitions.createComplexAvroMessage();

        siddhiAppRuntime.start();
        InMemoryBroker.publish("userInfo", ByteBuffer.wrap(data));
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(1, count.get());
    }

    @Test(description = "Check Avro source mapper for custom avro message mapping to siddhi events" +
            "with data type OBJECT.")
    public void avroSourceMapperTest9() throws SubscriberUnAvailableException {
        log.info("Testing Avro Source Mapper with complex schema structure");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='userInfo', @map(type='avro', schema.def = \"\"\"{" +
                "    \"type\" : \"record\",\n" +
                "    \"name\" : \"userInfo\",\n" +
                "    \"namespace\" : \"avro.userInfo\",\n" +
                "    \"fields\" : [{\"name\" : \"username\",\"type\" : \"string\"},\n" +
                "                  {\"name\" : \"age\",\"type\" : \"int\"},\n" +
                "                  {\"name\" : \"address\", \"type\" : {\"type\":\"record\",\n" +
                "                                                       \"name\":\"addressField\",\n" +
                "                                                       \"fields\":[\n" +
                "                                                        {\"name\":\"street\",\"type\":\"string\"},\n" +
                "                                                        {\"name\":\"city\",\"type\":\"string\" } \n" +
                "                                                        ]}},\n" +
                "                  {\"name\" : \"gender\",\"type\" : \"string\"}\n" +
                "   ]\n" +
                "}\"\"\",\n" +
                "@attributes(name=\"username\", age=\"age\",add=\"address\", sex=\"gender\")))\n" +
                "define stream FooStream (name string, age int, add object, sex string); " +
                "define stream BarStream (name string, add object, sex string); ";
        String query = "" +
                "from FooStream " +
                "select name, add, sex " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                eventArrived = true;
                count.addAndGet(events.length);
                EventPrinter.print(events);
                AssertJUnit.assertEquals("WSO2", events[0].getData(0));
            }
        });

        byte[] data = AvroSchemaDefinitions.createComplexAvroMessage2();

        siddhiAppRuntime.start();
        InMemoryBroker.publish("userInfo", ByteBuffer.wrap(data));
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(1, count.get());
    }

    @Test(description = "Check Avro source maps avro message with array schema to multiple events" +
            "with custom mapping")
    public void avroSourceMapperTest10() throws SubscriberUnAvailableException {
        log.info("Testing Avro Source Mapper with multiple events");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='stock', @map(type='avro', schema.def = \"\"\"{ \"type\" : \"array\"," +
                "\"items\" : {\"type\" : \"record\", \"name\" : \"user\", \"fields\": [{\"name\":\"name\"," +
                "\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\": [\"int\", \"null\"]}]}}\"\"\"," +
                "@attributes(username = \"name\",number =\"favorite_number\")))" +
                "define stream FooStream (username string, number int); " +
                "define stream BarStream (username string, number int); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                eventArrived = true;
                count.addAndGet(events.length);
                EventPrinter.print(events);
                AssertJUnit.assertEquals("WSO2", events[0].getData(0));
                AssertJUnit.assertEquals("IBM", events[1].getData(0));

            }
        });

        byte[] data = AvroSchemaDefinitions.createArrayOfAvroMessage();
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", ByteBuffer.wrap(data));
        siddhiAppRuntime.shutdown();

        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(2, count.get());
    }

    @Test(description = "Check Avro source maps avro message to siddhi events by retrieving the schema " +
            "from schema registry.")
    public void avroSourceMapperTest11() throws SubscriberUnAvailableException {
        log.info("Testing Avro Source Mapper with schema registry");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='stock', @map(type='avro', schema.id = '22'," +
                "schema.registry = 'http://localhost:8081', @attributes(username = \"firstName\",surname =" +
                "\"lastName\",bDay = \"birthDate\")))" +
                "define stream FooStream (username string, surname string, bDay long); " +
                "define stream BarStream (username string, surname string, bDay long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            ConnectionTestUtil.connectToSchemaRegistry(schemaRegistryURL);
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
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

            byte[] data = AvroSchemaDefinitions.createAvroMessagForRegistrySchema();
            siddhiAppRuntime.start();
            InMemoryBroker.publish("stock", ByteBuffer.wrap(data));
            siddhiAppRuntime.shutdown();

            AssertJUnit.assertTrue(eventArrived);
            AssertJUnit.assertTrue(innerAssertionsPass);
            AssertJUnit.assertEquals(1, count.get());
        } catch (FeignException e) {
            log.warn("Schema Registry at " + schemaRegistryURL + " may not be available.");
        }
    }

    @Test(description = "Check Avro source mapper fails when retrieving non existing schema from schema registry ",
            expectedExceptions = SiddhiAppCreationException.class)
    public void avroSourceMapperTest12() {
        log.info("Testing Avro Source Mapper with schema registry");
        String streams = "" +
                "@App:name('TestApp')" +
                "@source(type='inMemory', topic='stock', @map(type='avro', schema.id = '23'," +
                "schema.registry = 'http://localhost:8081'))" +
                "define stream FooStream (username string, surname string, bDay long); " +
                "define stream BarStream (username string, surname string, bDay long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();

        try {
            ConnectionTestUtil.connectToSchemaRegistry(schemaRegistryURL);
            siddhiManager.createSiddhiAppRuntime(streams + query);
        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertEquals("Error on 'TestApp' @ Line: 1. Position: 138, near '@source(type='inMemory', " +
                 "topic='stock', @map(type='avro', schema.id = '23',schema.registry = 'http://localhost:8081'))'. " +
                 "Error when retriving schema from schema registry. status 404 reading SchemaRegistryClient#findByID" +
                 "(String); content:\n{\"error_code\":40403,\"message\":\"Schema not found\"}", e.getMessage());
            throw e;
        } catch (FeignException e) {
            log.warn("Schema Registry at " + schemaRegistryURL + " may not be available.");
            throw new SiddhiAppCreationException("Siddi App cannot be created.");
        }
    }
}
