# KaBoom Changes

## 0.9.0

* KABOOM-36: Expose login context name as a confguration option
* KABOOM-39: deprecate kerberos configs in kaboom.properties for kaboom versions >= 0.9.0
* KABOOM-40: Create KaBoom temp dir on package install and set snappy temp dir to use it

## 0.8.4-HF2

* KABOOM-37: Old timestamps are not being treated as skewed

## 0.8.4-HF1

* KABOOM-33: Provide Better Resiliency around ZK Corruption

## 0.8.4

Tickets:

* KABOOM-26 Fix Overburdening Assignment (Even Load Balancer will not assign a partition to a node if that would make the node over-burdened).  EvenLoadBalancer ensures clients meet the criteria of canTakeAnotherAssignment() and  hasLocalPartition(KaBoomPartition partition) before assigning an unassigned partition
* KABOOM-27 Implement Skewed Timestamp (Old/Future Parsed Dates) Handling
* KABOOM-28 Improve Assignment and Lock Error Handling

Features:

TimeBasedHdfsOutputPath: New method skewed() return true if the boom files date directory is too far into the past/future based on new running configuration options
TimeBasedHdfsOutputPath: OutputFile private class overwrites the boom filename, date directory, and data directory according to new running configuration options

New Metrics:

* kaboom:total:skewed time boom files // the total number of skewed boom files for the node
* kaboom:partitions:<partition>:skewed time boom files // the total number of skewed boom files for the partiton

## 0.8.3 (never released outside of labs)

* kaboom-api dependency now on version 0.8.4
* AsyncAssignee: moved node cache listener on assignment path to attribute and closes on release lock
* AsyncAssignee: moved connection state listener to attribute and removes on release lock
* TimeBasedHdfsOutputPath now waits for existing open files to close waiting nodeOpenFileWaittimeMs between isFileClosed()
* TimeBasedHdfsOutputPath gives up and deletes existing open files if not closed after nodeOpenFileForceDeleteSeconds
* TimeBasedHdfsOutputPath now has an instance of the Worker as an attribute and can respond to pings when it's busy
* Leader has a new method called refreshMetadata() that clears all convenience mappings and refreshes all metadata
* Leader has a new method called pauseOnFirstDisconnectedAssignee() that iterates through all assignments and waits leaderNodeDisconnectionWaittimeSeconds on the first disconnected assigned node before calling refreshMetadata() and returning
* Leader's main loop now calls refreshMetadata() and then pauseOnFirstDisconnectedAssignee()
* Leader worst case running time increased by at least leaderNodeDisconnectionWaittimeSeconds


## 0.8.2-HF4

* Updates the kaboom-api dependency to 0.8.3 (which resolves leaking Kafka simple consumer sockets)
* Exposes the startup config's node cache listener as an accessible attribute
* Removes unnecessary and unused mapping of proxy user to file system

## 0.8.2-HF3

* Fixes KABOOM-20 - Maps Used by Leader Not Emptied
* Fixes KABOOM-21 - Ensure Exceptions Are Not Swallowed And Abort Accordingly
* Fixes KABOOM-22 - Bug in Closing LRU Boom File Closing Most Recently Used v2

## 0.8.2-HF2

* Fixes IPGBD-4245/KABOOM-18 - Bug closing LRU Output File


## 0.8.2-HF1

* Moves the node cache listener to an attribute of worker and closes off in the finally block executed after the run

## 0.8.2:

* TODO: Re-write the change log

## 0.8.1

* New metric, kaboom:total:gracefully restarted workers

## 0.8.0

* Refactored configuration into startup/running, running configuration migrated to ZK
* Introduction of worker sprints
* Intended to be manage via Kontroller API and web interface

## 0.7.16-HF1

* Improves exception handling and reduce file corruption when closing boom files to ensure that any problems closing the file result in the worker aborting the file.

## 0.7.16

* Adds a new stand-alone utility to write a boom file for a specific partition, start offset and end offset to a specific destination
* Adds a new meter metric: kaboom:total:dead workers

## 0.7.15-HF1

* Fixes IPGBD-3830 [Kaboom] Bug when handling zero padded date field

## 0.7.15

* Ensures that only topics with unique HDFS root paths are examined
* New configuration option: kaboom.propagate.ready.flags.delay.ms (long, how often wait between paths, default 0) can be used to ease the burden on the name nodes if there's a massive amount of HDFS path traversal required

## 0.7.14

* Moved the READY flag propagation to KaBoom (from an older to-be-deprecated project
* New JMX metric: kaboom:topic:<topicName>:flag propagator timer (timer for recursive HDFS directory traversal)
* New JMX metric: kaboom:topic:<topicName>:flag propagator paths checked (merter for number of paths checked)
* New JMX metric: kaboom:topic:<topicName>:flags written (merter for number of flags created)
* New configuration option: kaboom.propagate.ready.flags (boolean, default false)
* New configuration option: kaboom.propagate.ready.flags.frequency (long, how often in ms to spawn propagator thread, default 10 * 60 * 1000)
* Supports bdp-common 0,0.6 which provides all logging and monitoring deps
* Instruments log4j2 with io.dropwizard.metrics
* Adds a new API

## 0.7.13

* Added kafkaOffset argument to FastBoomWriter.writeLine()
* Added a lastKafkaOffset to FastBoomWriter
* FastBoomWriter now keeps track of lastKafkaOffset and lastMessageTimestamp
* Changed the periodicCloseExpiredPoll in TimeBasedHdfsOutputPath to store the expired boom file's lastKafakaOffset and lastMessageTimestamp instead of the worker's current values
* Added a new global configuration property max.open.boom.files.per.partition (default: 5)  to limit the number of open boom files per partition
* Modified TimeBasedHdfsOutputPath to close off the oldest FastBoomWriter when the number of open boom files is greater than max.open.boom.files.per.partition, and update zookeeper with lastKafakaOffset and lastMessageTimestamp

## 0.7.12-HF4

* ReadFlagWriter now doesn't check for the existence of the data directory when it's writing flags

## 0.7.12-HF3

* New configuration option boom.file.expired.grace.time.ms (default: 30 * 1000, thirty seconds): The time after a TimeBasedHdfsOutputPath output file expires before it's closed via the periodic file close interval triggered upon each message consumed from Kafka.  This option is introduced to make the previously hard coded value configurable and uses the same default value as the previous hard coded value.
* New configuration option forced.zk.offsetTimestamp.update.time.ms (default: 10 * 60 * 1000, ten minutes): Ensure that very quiet partitions are updating their offset timestamp in ZK even when they are not receiving any messages.  If the last received message was during the previous hour and it's been more than this amount of  milliseconds then write the start of the hour's timestamp into ZK for the partition (providing it hasn't already been stored for the current hour already).
* New configuration option kaboom.server.sleep.duration.ms (default: 10 000, ten seconds): Exposes a configuration option for a previously hard coded property value with the same default value.  This is the time waited after a worker processes it's work assignments before it fetches another update from ZK and processes again.
* Fixes the NullPointerException caused by improper handling of the NoNodeException that very rarely occurs when a worker fetches it's assignments.  The null assignee is now logged and ignored and skipped until the next round of worker assignments is reviewed (after the newly introduced: kaboom.server.sleep.duration.ms).
* Fixes the "hung KaBoom workers" by sending a basic ping() health check to each KaBoom worker.  The worker has until kaboom.server.sleep.duration.ms to respond with a pong before the KaBoom server will send it a kill request and send an interrupt to the worker's thread.  The worker (if it comes back to acknowledge it's been killed) will throw an exception that will get caught and trigger the abort sequence (delete files, and stop).

## 0.7.12

* Fixes bug that threw an NPE when there was no work assigned to a client and the load balancer tries to check if it's over worked

## 0.7.11

* Adds an additional kafka ready flag in the topic root
* Logs a warning if the load balancer is still running after leader.sleep.duration.ms

## 0.7.10

* Adds a new optional configuration option (String) kaboom.kafkaReady.flag.filename, default=_KAFKA_READY

## 0.7.9

* Fixes a ReadyFlagWriter bug that would write flags to the current hour
* Adds logging around the maxTimestamps that are stored in ZK
* Depends on common-utils-dev 0.0.5 (to get the new ZK get/set utility)

## 0.7.8

* Adds a total compression ratio histogram

## 0.7.7

* Adds new optional configuration option (Short): kaboom.deflate.compression.level, default=6
* Adds new optional configuration option (Short): topic.<topic>.compression.level
* Adds new histogram metric for compression ratio

## 0.7.6

* Resolves hostname bug in the LocalLoadBalancer

## 0.7.5

* Improved ReadyFlagWriter logic: Previous versions were buggy and had too many operations included in their hourly loop instead of in the topic loop
* Removes checks on _READY flag in ReadyFlagWriter (_READY flags from LogDriver's LogMaintenance are deprecated by KaBoom 0.7.1 and later)
* Fixes bugs related to concurrent access on shared TimeBasedHdfsOutputPath objects (each worker now instantiates their own)
* Reduced INFO level log messages throughout to limit logs to more important messages
* Separates the pre-install script for DEB and RPM (as DEB's don't require the /var/run/kaboom)
* Adds new optional configuration option (Long): leader.sleep.duration.ms, default=10 * 60 * 1000
* Adds new optional configuration option (String) kaboom.load.balancer.type, default=even

## 0.7.4

* Abstracts load balancing and adds two implementation: even and local

## 0.7.3

* Adds native compression

## 0.7.2

* Bumps Krackle dependency to 0.7.10 to have the consumer's broker socket have the keep alive flag set

## 0.7.1

* New timer metrics for HDFS flush time for topic-partition, topic, and total per server
* New meter metrics for boom writes for topic-partition, topic, and total per server
* Adds new required configuration option: hadoop.fs.uri
* Adds new required topic configuration for HDFS root directory (string): topic.<topicName>.hdfsRootDir
* Supports multiple numbered template based HDFS output paths per topic
* Topic HDFS output paths are now configurable to be left open for specific durations
* Adds new optional configuration option (boolean): kaboom.useTempOpenFileDirectory
* Adds new optional configuration option (Integer): boom.file.buffer.size, default=16384
* Adds new optional configuration option (Short): boom.file.replicas, default=3
* Adds new optional configuration option (Long): boom.file.block.size=268435456
* Adds new optional configuration option (String): boom.file.temp.prefix, default=_tmp_
* Adds new optional configuration option (Long): boom.file.flush.interval, default=30000
* Adds new optional configuration option (Long): boom.file.close.expired.interval, default=60000

## 0.7.0

* Deprecates all CDH-specific content, configuration, and project files
* New dependency on Krackle 0.7.7 for configuring socket timeouts
* New KaboomConfiguration class that encapsulates all the configuration
* New accessor methods for instantiating CuratorFramework objects
* Project builds produce an RPM artifact
* Fixes synchronization on non-final workersLock object (used when instantiating metrics)
* Removes unused imports
* Worker.java, int length; byte version; int pos; are no longer initialized with default values that are never used
* New method: private Map<String, String> getTopicPathsFromProps(Properties props)
* New method: private Map<String, String> getTopicProxyUsersFromProps(Properties props)
* new method: private Properties getProperties()

## 0.6.10

* Re-formats source for Kaboom and Worker class
* Adds offset overrides feature for single partitions to be set to specific offsets in ZK
* Adds  feature and configuration property to sink to lower offsets when offsets surpass the high watermark
* Re-writes the offset handling code for when last offsets do not match expected offset
* Adds new dependency to the new com.blackberry.common.props that simplifies parsing property files and will eventually be enhanced with ZK support
