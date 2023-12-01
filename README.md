# kennzeichen

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: https://quarkus.io/ .

## use cases

```
❯ cat db_min.jsonl | jq "{ts: .captureTimestamp, plate: .plateUTF8, id: .sensorProviderID, dir: .carMoveDirection, state: .carState, car: .carID}" -c | sort -n G "XX YY"
```

```json
{"ts":1694532901149,"plate":"XX YY3803","id":"ausfahrt_heck","dir":"unknown","state":"new","car":"127245"}
{"ts":1694532901189,"plate":"XX YY3803","id":"ausfahrt_heck","dir":"in","state":"update","car":"127245"}
{"ts":1694532902149,"plate":"XX YY3803","id":"ausfahrt_heck","dir":"in","state":"update","car":"127245"}
{"ts":1694532906403,"plate":"XX YY3803","id":"einfahrt_front","dir":"in","state":"update","car":"95443"}
{"ts":1694532906603,"plate":"XX YY3803","id":"einfahrt_front","dir":"in","state":"update","car":"95443"}
{"ts":1694532917647,"plate":"XX YY3803","id":"einfahrt_heck","dir":"unknown","state":"new","car":"111650"}
{"ts":1694532917727,"plate":"XX YY3803","id":"einfahrt_heck","dir":"out","state":"update","car":"111650"}
{"ts":1694534125892,"plate":"XX YY3","id":"ausfahrt_front","dir":"unknown","state":"new","car":"100347"}
{"ts":1694534125972,"plate":"XX YY38","id":"ausfahrt_front","dir":"in","state":"update","car":"100347"}
{"ts":1694534126132,"plate":"XX YY380","id":"ausfahrt_front","dir":"in","state":"update","car":"100347"}
{"ts":1694534126212,"plate":"XX YY3800","id":"ausfahrt_front","dir":"in","state":"update","car":"100347"}
{"ts":1694534126412,"plate":"XX YY3803","id":"ausfahrt_front","dir":"in","state":"update","car":"100347"}
{"ts":1694534129012,"plate":"XX YY3800","id":"ausfahrt_front","dir":"in","state":"update","car":"100347"}
{"ts":1694534139510,"plate":"XX YY3803","id":"ausfahrt_heck","dir":"unknown","state":"new","car":"127265"}
{"ts":1694534140150,"plate":"XX YY3803","id":"ausfahrt_heck","dir":"out","state":"update","car":"127265"}
```


## preprocessing steps

`cat database-dump.csv | tail -n+2 | cut -d ',' -f 2- | perl -pe 's/""/"/g' | cut -c 2- | rev | cut -c 3- | rev | jq '.[] | select(.fieldname | contains("event"))' -c > db_clean.jsonl`

* `tail -n+2` to remove the CSV header
* `cut -d ',' -f 2-` to keep only fields after the first (which is the DB ID we don't need)
* `perl -pe 's/""/"/g'` to replace empty double quotes with valid double quotes
* `cut -c 2- | rev |  cut -c 3- | rev` to trim quotes from the beginning and end of fields
* `jq '.[] | select(.fieldname | contains("event"))'` to select just the event objects, dropping the ones with binary data

`cat db_clean.jsonl | jq '.buffer | {carID: .carID | tonumber, carState: .carState, plateUnicode: .plateUnicode, plateConfidence: .plateConfidence | tonumber, carMoveDirection: .carMoveDirection, captureTimestamp: .capture_timestamp | tonumber, sensorNDL: .sensorProviderID | split("_")[0], sensorName: .sensorProviderID | split("_")[1:] | join("_")}' -c`

* `.sensorProviderID | split("_")[0]` splits the `sensorProviderID` field at the underscore and selects the first part.
*`(split("_") | .[1:] | join("_"))` splits the `sensorProviderID` field at the underscore, selects all parts after the first one, and joins them back together.
*`captureTimestamp: .capture_timestamp | tonumber` convert the timestamp string to a number

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:
```shell script
./gradlew quarkusDev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8080/q/dev/.

## Packaging and running the application

The application can be packaged using:
```shell script
./gradlew build
```
It produces the `quarkus-run.jar` file in the `build/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `build/quarkus-app/lib/` directory.

The application is now runnable using `java -jar build/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:
```shell script
./gradlew build -Dquarkus.package.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar build/*-runner.jar`.

## Creating a native executable

You can create a native executable using: 
```shell script
./gradlew build -Dquarkus.package.type=native
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using: 
```shell script
./gradlew build -Dquarkus.package.type=native -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./build/kennzeichen-1.0.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult https://quarkus.io/guides/gradle-tooling.

## Related Guides

- Apache Kafka Streams ([guide](https://quarkus.io/guides/kafka-streams)): Implement stream processing applications based on Apache Kafka
- SmallRye Reactive Messaging - Kafka Connector ([guide](https://quarkus.io/guides/kafka-reactive-getting-started)): Connect to Kafka with Reactive Messaging
- Micrometer metrics ([guide](https://quarkus.io/guides/micrometer)): Instrument the runtime and your application with dimensional metrics using Micrometer.
- Micrometer Registry Prometheus ([guide](https://quarkus.io/guides/micrometer)): Enable Prometheus support for Micrometer

## Provided Code

### Reactive Messaging codestart

Use SmallRye Reactive Messaging

[Related Apache Kafka guide section...](https://quarkus.io/guides/kafka-reactive-getting-started)

