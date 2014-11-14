# KaBoom - A High Performance Producer Client for Kafka

Klogger is a simple service that uses Krackle to receive messages and produce them to Kafka topics. 

**Design Goals**

* Basic: Small code base with few dependencies and external requirements
* Efficient: Consideration placed on minimizing object instantiation to reduce effects of GC
* Configurable: Resource consumption can be configured to tune performance 

**Limitations**

* Security: Klogger is not intended to be distributed outside a trusted security administration domain--a fancy way of saying that Klogger should be deployed only to hosts intended to have direct access to produce for your Kafka topics.  There is no built-in access control or authentication.

**Author(s)** 

* Will Chartrand (original author)
* [Dave Ariens](<mailto:dariens@blackberry.com>) (current maintainer)

**Building**

Performing a Maven install produces a Debian package that currently installs on Ubuntu based Linux distributions that support upstart-enabled services.

**Configuring**

Below is an example configuration for running a KLogger instance that receives messages for two topics (topic1, topic2) on ports 2001, 2002 respectively.  It uses 4GB  worth of heap and will buffer up to a GB of messages for both topics (in the event Kafka cannot ack) before dropping.

* /opt/klogger/config/klogger-env.sh (defines runtime configuration and JVM properties)

	JAVA=``which java``
	BASEDIR=/opt/klogger
	CONFIGDIR="$BASEDIR/config"
	LOGDIR="$BASEDIR/logs"
	PIDFILE="/var/run/klogger/klogger.pid"
	KLOGGER_USER="kafka"
	JMXPORT=9010
	LOG4JPROPERTIES=$CONFIGDIR/log4j.properties
	JAVA_OPTS=""
	JAVA_OPTS="$JAVA_OPTS -server"
	JAVA_OPTS="$JAVA_OPTS -Xms4G -Xmx4G"
	JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC -XX:+UseConcMarkSweepGC"
	JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly -XX:+CMSConcurrentMTEnabled -XX:+CMSScavengeBeforeRemark"
	JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=80"
	JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution"
	JAVA_OPTS="$JAVA_OPTS -Xloggc:$LOGDIR/gc.log"
	JAVA_OPTS="$JAVA_OPTS -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M"
	JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true"
	JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote"
	JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
	JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false"
	JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=$JMXPORT"
	JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$LOG4JPROPERTIES"

    CLASSPATH=$CONFIGDIR
    for file in ``find $BASEDIR -name "*.jar"``
    do
        if [[ "x$CLASSPATH" == "x" ]]
        then
            CLASSPATH=$file
        else
            CLASSPATH="$CLASSPATH:$file"
        fi
    done

* /opt/klogger/config/klogger.properties (defines Klogger configuration, topics, and ports)

	metadata.broker.list=kafka1.site.dc1:9092,kafka2.site.dc1:9092,kafka3.site.dc1:9092
	compression.codec=snappy
	queue.enqueue.timeout.ms=0
	use.shared.buffers=true
	kafka.rotate=true
	num.buffers=1000
	#This should be a unique character/string per klogger host.
	kafka.key="
	sources=topic1,topic2
	source.topic1.port=2001
	source.topic1=topic1
	source.topic2.port=2002
	source.topic2=topic2

* /opt/klogger/config/log4j.properties (logging)

	klogger.logs.dir=/var/log/klogger
	log4j.rootLogger=INFO, kloggerAppender

	log4j.appender.stdout=org.apache.log4j.ConsoleAppender
	log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
	log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)n

	# rolling log file
	log4j.appender.kloggerAppender=org.apache.log4j.RollingFileAppender
	log4j.appender.kloggerAppender.maxFileSize=20MB
	log4j.appender.kloggerAppender.maxBackupIndex=5
	log4j.appender.kloggerAppender.layout=org.apache.log4j.PatternLayout
	log4j.appender.kloggerAppender.layout.ConversionPattern=%5p [%t] %d{ISO8601} %m%n
	log4j.appender.kloggerAppender.File=${klogger.logs.dir}/server.log

**Running**

After configuration simply start the service 'klogger start'.  The package also creates a symbolic from /lib/init/upstart-job to /etc/init.d/klogger so existing 'service' configurations are respected.

**Monitoring**

Exposed via (Coda Hale's Metric's)[https://github.com/dropwizard/metrics] are:

Klogger:

* Meter: bytesReceived
* Meter: bytesReceivedTotal

Krackle:

* Meter: received
* Meter: receivedTotal
* Meter: sent
* Meter: sentTotal 
* Meter: doppedQueueFull
* Meter: doppedQueueFullTotal
* Meter: doppedSendFail
* Meter: droppedSendFailTotal

**Contributing**

To contribute code to this repository you must be [signed up as an official contributor](http://blackberry.github.com/howToContribute.html).

## Disclaimer

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.