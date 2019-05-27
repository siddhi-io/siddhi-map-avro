siddhi-map-avro
======================================

The **siddhi-map-avro extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a>
which is used to convert Avro message to/from Siddhi events.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-avro">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-avro/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-avro/issues">Issue tracker</a>

## Latest API Docs

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-avro/api/2.0.1">2.0.1</a>.

## How to use

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support.

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this
extension you can replace the component <a target="_blank" href="https://github
.com/wso2-extensions/siddhi-map-avro/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>io.siddhi.extension.map.avro</groupId>
        <artifactId>siddhi-map-avro</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ |
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-avro/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-avro/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-avro/api/2.0.1/#avro-sink-mapper">avro</a> *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#sink-mapper">(Sink Mapper)</a>*<br><div style="padding-left: 1em;"><p>This extension is a Siddhi Event to Avro Message output mapper.Transports that publish  messages to Avro sink can utilize this extension to convert Siddhi events to Avro messages.<br>&nbsp;You can either specify the Avro schema or provide the schema registry URL and the schema reference ID as parameters in the stream definition.<br>If no Avro schema is specified, a flat Avro schema of the 'record' type is generated with the stream attributes as schema fields.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-avro/api/2.0.1/#avro-source-mapper">avro</a> *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#source-mapper">(Source Mapper)</a>*<br><div style="padding-left: 1em;"><p>This extension is an Avro to Event input mapper. Transports that accept Avro messages can utilize this extension to convert the incoming Avro messages to Siddhi events.<br>&nbsp;The Avro schema to be used for creating Avro messages can be specified as a parameter in the stream definition.<br>&nbsp;If no Avro schema is specified, a flat avro schema of the 'record' type is generated with the stream attributes as schema fields.<br>The generated/specified Avro schema is used to convert Avro messages to Siddhi events.</p></div>

## How to Contribute

  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-avro/issues">GitHub
  Issue
   Tracker</a>.

  * Send your contributions as pull requests to <a target="_blank" href="https://github
  .com/wso2-extensions/siddhi-map-avro/tree/master">master branch</a>.

## Contact us

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>.

 * Siddhi developers can be contacted via the mailing lists:

    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)

    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)

## Support

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology.

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.
