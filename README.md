# Druid Client [![Build Status](https://travis-ci.org/mbrtargeting/druid-client.svg?branch=master)](https://travis-ci.org/mbrtargeting/druid-client)

Druid client that simplifies interactions with the Druid API.

[Scaladoc](https://mbrtargeting.github.io/druid-client/latest/api/eu/m6r/druid/client/DruidHttpClient.html)

# Usage
You can either use the `druid-client` as a Scala library or from command-line. 

Import to your Scala project:
```scala
libraryDependencies ++= Seq("eu.m6r" %% "druid-client" % "0.1.0")
```

## CLI

### Task-Config

XML Example: 

```xml
<task:taskConfig xmlns:task="http://m6r.eu/druid/client/task-config">
  <!--1 or more repetitions:-->
  <dimensions>string</dimensions>
  <!--1 or more repetitions:-->
  <metrics>
    <type>string</type>
    <name>string</name>
    <fieldName>string</fieldName>
  </metrics>
</task:taskConfig>
```

JSON Example (json config filename has to end with `json`):
```json
{
  "taskConfig": {
    "dimensions": [
      "string"
    ],
    "metrics": [
        {
          "type": "string",
          "name": "string",
          "fieldName": "string"
        }
    ]
  }
}
```

You can use `models.Utils.parseTaskConfig(file)` to parse a task config file programmatically.

# Build

We support Scala 2.11 and 2.12. To do a cross-build that create jars for all supported Scala 
versions, do:

```bash
sbt +package
```

To build fat jars:

```bash
sbt +assembly
```

## Publish artifact

```bash
sbt +publishSigned
```


## Upload Scaladoc to Github Pages

Build and upload Scaladoc documentation to Github pages.
```bash
sbt ghpagesPushSite
```

# TODO

- Add scaladoc comments
- Add documentation to the README. e.g.
  - methods description
  - parameters description
  - Examples
  - how to contribute
- Add tests
- Write to druid mailing list
- Add to druid libraries page
- Upload to maven central

### Optional
- Support more druid functions:
    - Queries
- Build `.deb` with sbt.
- Maybe upload `.deb` to ubuntu package sources.
- Would be nice to support brew.
