# KaBoom - A High Performance Consumer Client for Kafka
KaBoom uses Krackle to consume from partitions of topics in Kafka and write them to boom files in HDFS.  

## Features
* Uses the [Curator Framework](http://curator.apache.org/) for  [Apache Zookeeper](zookeeper.apache.org) to distribute work amongst multiple servers
* Supports writing to secured Hadoop clusters via Kerberos based secure impersonation (conveniently pulled from [Flume](http://flume.apache.org/))
* Recovers from Kafka server failures (even when newly elected leaders weren't in-sync when elected)
* Supports consuming with either GZIP or Snappy compression
* Configurable: Each topic can be configured with a unique HDFS path template with date/time variable substitution
* Supports flagging timestamp template HDFS directories as 'Ready' when all a topic's partition's messages have been written for a given hour

## Author(s)
* [Dave Ariens](<mailto:dariens@blackberry.com>) ([Github](https://github.com/ariens))
* [Matthew Bruce](<mailto:mbruce@blackberry.com>) ([Github](https://github.com/MatthewRBruce))

## Building
Performing a Maven install produces: 

* An RPM package that currently installs RPM based Linux distributions
* A Debian package for dpkg based Linux distributions

This release contains the most significant updates to KaBoom we have introduced in a single version bump.  The most significant change is the migration of all running configuration parameters and topic configurations to ZooKeeper. The remaining confiugration continues to be read in via a property file.  The running configuration and topic configuraiton is stored at at zk://<root>/kaboom/config

## Startup versus Running Configurations

Startup configuration changes require a KaBoom service restart to be loaded, whereas the running configuration is reloaded by KaBoom as changes are made in ZooKeeper.  Updated running configuration values are then used as they are accessed by KaBoom.  For example you can change the number of HDFS replicas to store for boom files in Hadoop however it will not affect any open or previously closed files only files that are created after the new configuration has been loaded (as replicas are specified when file creating files from a file system object only).

## Topic Configurations

Unlike running configurations which are reloaded instantly topic configuration updates trigger all workers assigned to a partition of the topic to be gracefully shutdown (boom files closed, offsets, and offset timestamps  stored in ZK).  The KaBoom client will then detect and restart any gracefully shutdown workers.  Workers load their topic configuration when they are launched.


## Example Topic Configuration

The topic configurations are stored at zk://<root>/kaboom/topics/<id>, as:

```
{
        version: 1,
        id: "devtest-test3",
        hdfsRootDir: "/service/82/devtest/logs/%y%M%d/%H/devtest-test3",
        proxyUser: "dariens",
        defaultDirectory: "data",
        filterSet: [ ]
}
```

Note: The empty filterSet array is reserved for future to-be-implemented  use-cases.

### Startup Configuration

Example startup configuration (property file based):

```
######################
# KaBoom Configuration
######################

kaboom.id=1001
hadooop.fs.uri=hdfs://hadoop.company.com
#kaboom.weighting=<number> - the default is number of cores
kerberos.keytab=/opt/kaboom/config/kaboom.keytab
kerberos.principal=kaboom@COMPANY.COM
#kaboom.hostname=<name> - the default is the system's hostname
zookeeper.connection.string=r3k1.kafka.company.com:2181,r3k2.kafka.company.com:2181,r3k3.kafka.company.com:2181/KaBoomDev
kafka.zookeeper.connection.string=r3k1.kafka.company.com:2181,r3k2.kafka.company.com:2181,r3k3.kafka.company.com:2181
#kaboom.load.balancer.type=even - this is the default
#kaboom.runningConfig.zkPath=/kaboom/config - this is the default

########################
# Consumer Configuration 
########################

metadata.broker.list=r3k1.kafka.company.com:9092,r3k2.kafka.company.com:9092,r3k3.kafka.company.com:9092
fetch.message.max.bytes=10485760
fetch.wait.max.ms=5000
#fetch.min.bytes=1 - this is the default
socket.receive.buffer.bytes=10485760
auto.offset.reset=smallest
#socket.timeout.seconds=30000 - this is the default
```

### Example Configuration File: /opt/kaboom/config/kaboom-env.sh (defines runtime configuration and JVM properties)
```
JAVA=`which java`
BASEDIR=/opt/kaboom
BINDIR="$BASEDIR/bin"
LIBDIR="$BASEDIR/lib"
LOGDIR="/var/log/kaboom"
CONFIGDIR="$BASEDIR/config"
JMXPORT=9580
LOG4JPROPERTIES=$CONFIGDIR/log4j2.xml
PIDBASE=/var/run/kaboom
KABOOM_USER=kafka

JAVA_OPTS=""
JAVA_OPTS="$JAVA_OPTS -server"
JAVA_OPTS="$JAVA_OPTS -Xms6G -Xmx6G"
JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC -XX:+UseConcMarkSweepGC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly -XX:+CMSConcurrentMTEnabled -XX:+CMSScavengeBeforeRemark"
JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=30"

JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution"
JAVA_OPTS="$JAVA_OPTS -Xloggc:$LOGDIR/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M"

JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true"
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote"
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false"
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=$JMXPORT"

JAVA_OPTS="$JAVA_OPTS -Dlog4j.configurationFile=file:$LOG4JPROPERTIES"

JAVA_OPTS="$JAVA_OPTS -Dkaboom.logs.dir=$LOGDIR"

CLASSPATH=$CONFIGDIR:/etc/hadoop/conf:$LIBDIR/*
```

### Example Configuration FIle: /opt/kaboom/config/log4j2.xml (logging)
```
<?xml version="1.0" encoding="UTF-8"?>
<!-- This status="$LEVEL" on the next line  is for the logging of log4j2 as it configured tself, don't adjust it for  your application logging -->
<configuration status="WARN" monitorInterval="30">
   <appenders>
      <Console name="console" target="SYSTEM_OUT">
         <PatternLayout pattern="[%d] %p %m (%c)%n" />
      </Console>
      <RollingFile name="primary" fileName="/var/log/kaboom/server.log" filePattern="/var/log/kaboom/server.log.%d{yyyy-MM-dd-k}.log">
         <PatternLayout>
            <Pattern>[%d] %p %m (%c)%n</Pattern>
         </PatternLayout>
         <Policies>
            <TimeBasedTriggeringPolicy interval="1" modulate="true" />
         </Policies>
      </RollingFile>
   </appenders>
   <Loggers>
      <Logger name="stdout" level="info" additivity="false">
         <AppenderRef ref="console" />
      </Logger>
      <Root level="INFO">
         <AppenderRef ref="primary" />
      </Root>
   </Loggers>
</configuration>
```

## Running
After configuration simply start the kaboom service 'service kabom start'.

## Monitoring
Exposed via [Dropwizard Metric's](https://github.com/dropwizard/metrics) are metrics for monitoring message count, size, and lag (measure of how far behind KaBoom is compared to most recent message in Kafka--both in offset count and seconds):

New monitoring metrics in 0.7.1:

* Meter: boom writes (The number of boom file writes)

Kaboom (Aggregate metrics--for the KaBoom cluster):

* Gauge: max message lab sec 
* Gauge: sum message lag sec 
* Gauge: avg message lag sec 
* Gauge: max message lag 
* Gauge: sum message lag
* Gauge: avg message lag 
* Gauge: avg messages written per sec
* Gauge: total messages written per sec

Kaboom (Instance metrics -- for a KaBoom worker assigned to a topic and partition):

* Gauge: offset lag
* Gauge: seconds lag
* Gauge: messages written per second
* Gauge: early offsets received (when compression is enabled and messages are included from earlier than requested offset)
* Meter: boom writes

Krackle:

* Meter: MessageRequests
* Meter: MessageRequestsTotal
* Meter: MessagesReturned
* Meter: MessagesReturnedTotal
* Meter: BytesReturned
* Meter: BytesReturnedTotal
* Meter: MessageRequestsNoData
* Meter: MessageRequestsNoDataTotal
* Meter: BrokerReadAttempts
* Meter: BrokerReadAttemptsTotal
* Meter: BrokerReadSuccess
* Meter: BrokerReadSuccessTotal
* Meter: BrokerReadFailure
* Meter: BrokerReadFailureTotal

## Boom Files
This section contains portions from the [hadoop-logdriver](https://github.com/blackberry/hadoop-logdriver) project's description of Boom files.

A Boom file is a place where we store logs in HDFS.

The goals of Boom are:

* Be splittable by Hadoop, so that we can efficiently run MapReduce jobs against it.
* Be compressed to save storage.
* Be able to determine order of lines, even if they are processed out of order.

## File extention
The .bm file extension is used for Boom files.

## Boom File Format
A Boom file is a specific type of Avro [Object Container File](http://avro.apache.org/docs/1.6.3/spec.html#Object+Container+Files).  Familiarize yourself with those docs before you keep going.

Specifically, we always use a compression codec of 'deflate' and we always use the following Schema:

    {
      "type": "record",
      "name": "logBlock",
      "fields": [
        { "name": "second",      "type": "long" },
        { "name": "createTime",  "type": "long" },
        { "name": "blockNumber", "type": "long" },
        { "name": "logLines", "type": {
          "type": "array",
            "items": {
              "type": "record",
              "name": "messageWithMillis",
              "fields": [ 
                { "name": "ms",      "type": "long" },
                { "name": "message", "type": "string" }
              ]
            }
        }}
      ]
    }

### Basic Structure
The file contains any number of "logBlock" records.  Each logBlock contains data for multiple log lines, but all of the log lines in the record are timestamped in the same second.  Log lines in the same logBlock can have difference millisecond timestamps.

### Fields in logBlock
* second : the number of seconds since Jan 1, 1970 UTC.  All log lines in this record are timestamped with a time that occurs within this second.
* createTime : the time (in milliseconds) that this logBlock was created.  This is used for sorting logBlocks.
* blockNumber : a number indicating the sequence in which the logBlocks were written by whatever wrote the file.  This is used for sorting logBlocks.
* logLines : an array of "messageWithMillis" records, one per log line.

### Fields in messageWithMillis
* ms : the milliseconds part of the timestamp for this log line.  To get the complete timestamp, use second * 1000 + ms.
* eventId : an event identifier, reserved for future use.  Use 0 for raw log lines.
* message : the contents of the log line, excluding the timestamp and one space after the timestamp.

## Boom suggested defaults
Although no limitations should be assumed on the file beyond what has already been stated, these are sensible defaults that should be followed.

* The logLines field should contain no more that 1000 messageWithMillis entries.  If there are more than 1000 log lines within a second, then use multiple logBlock's with the same second value.
* The Avro Object Container File defines a "sync interval".  A good value for this seems to be 2MB (2147483648).
* While we are required to use the deflate codec, the compression level is configurable.  If you don't have a specific need, then level 6 is a good default.

## Sorting log lines
If the order of log lines is important, then the fields can be sorted by comparing fields in this order

* timestamp : first timestamp is first (after adding seconds and milliseconds)
* createTime : logBlocks that were written first go first.
* blockNumber : If two logBlocks were written in the same millisecond, then use them in the order they were written.
* index within logLines : If the log lines are the same timestamp, written in the same block, then the order is determined by where they are within the logLines array.

This is the default sorting for LogLineData objects.

## Contributing
To contribute code to this repository you must be [signed up as an official contributor](http://blackberry.github.com/howToContribute.html).

## Disclaimer
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
