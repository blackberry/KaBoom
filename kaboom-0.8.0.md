# KaBoom 0.8.0

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
hadooop.fs.uri=hdfs://hadoop.log82.bblabs
#kaboom.weighting=<number> - the default is number of cores
kerberos.keytab=/opt/kaboom/config/kaboom.keytab
kerberos.principal=flume@AD0.BBLABS
#kaboom.hostname=<name> - the default is the system's hostname
zookeeper.connection.string=r3k1.kafka.log82.bblabs:2181,r3k2.kafka.log82.bblabs:2181,r3k3.kafka.log82.bblabs:2181/KaBoomDev
kafka.zookeeper.connection.string=r3k1.kafka.log82.bblabs:2181,r3k2.kafka.log82.bblabs:2181,r3k3.kafka.log82.bblabs:2181
#kaboom.load.balancer.type=even - this is the default
#kaboom.runningConfig.zkPath=/kaboom/config - this is the default

########################
# Consumer Configuration 
########################

metadata.broker.list=r3k1.kafka.log82.bblabs:9092,r3k2.kafka.log82.bblabs:9092,r3k3.kafka.log82.bblabs:9092
fetch.message.max.bytes=10485760
fetch.wait.max.ms=5000
#fetch.min.bytes=1 - this is the default
socket.receive.buffer.bytes=10485760
auto.offset.reset=smallest
#socket.timeout.seconds=30000 - this is the default
```

## Running Configuration

Here is an example running configuration stored at zk:///<root>/kaboom/config:

```
{
	version: 8,
	allowOffsetOverrides: true,
	sinkToHighWatermark: true,
	useTempOpenFileDirectory: false,
	useNativeCompression: false,
	readyFlagPrevHoursCheck: 24,
	leaderSleepDurationMs: 600001,
	compressionLevel: 6,
	boomFileBufferSize: 16384,
	boomFileReplicas: 3,
	boomFileBlocksize: 268435456,
	boomFileTmpPrefix: "_tmp_",
	periodicHdfsFlushInterval: 30000,
	kaboomServerSleepDurationMs: 10000,
	fileCloseGraceTimeAfterExpiredMs: 30000,
	forcedZkOffsetTsUpdateMs: 600000,
	kafkaReadyFlagFilename: "_READY",
	maxOpenBoomFilesPerPartition: 5,
	workerSprintDurationSeconds: 3600,
	propagateReadyFlags: true,
	propagateReadyFlagFrequency: 600000,
	propateReadyFlagDelayBetweenPathsMs: 10
}
```

