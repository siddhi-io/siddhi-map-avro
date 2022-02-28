Siddhi Map Avro
===================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-avro/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-avro/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-map-avro.svg)](https://github.com/siddhi-io/siddhi-map-avro/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-map-avro.svg)](https://github.com/siddhi-io/siddhi-map-avro/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-map-avro.svg)](https://github.com/siddhi-io/siddhi-map-avro/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-map-avro.svg)](https://github.com/siddhi-io/siddhi-map-avro/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-map-avro extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that converts AVRO messages to/from Siddhi events.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 2.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.map.avro/siddhi-map-avro/">here</a>.
* Versions 1.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.map.avro/siddhi-map-avro">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-avro/api/2.2.2">2.2.2</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-avro/api/2.2.2/#avro-sink-mapper">avro</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink-mapper">Sink Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This extension is a Siddhi Event to Avro Message output mapper.Transports that publish  messages to Avro sink can utilize this extension to convert Siddhi events to Avro messages.<br>&nbsp;You can either specify the Avro schema or provide the schema registry URL and the schema reference ID as parameters in the stream definition.<br>If no Avro schema is specified, a flat Avro schema of the 'record' type is generated with the stream attributes as schema fields.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-avro/api/2.2.2/#avro-source-mapper">avro</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source-mapper">Source Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This extension is an Avro to Event input mapper. Transports that accept Avro messages can utilize this extension to convert the incoming Avro messages to Siddhi events.<br>&nbsp;The Avro schema to be used for creating Avro messages can be specified as a parameter in the stream definition.<br>&nbsp;If no Avro schema is specified, a flat avro schema of the 'record' type is generated with the stream attributes as schema fields.<br>The generated/specified Avro schema is used to convert Avro messages to Siddhi events.</p></p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

