# Ready Flags

## Introduction

KaBoom parses the date and time from each message it consumes from Kafka.  The HDFS directories that it writes boom files within are based on a path template (`TimeBasedHdfsOutputPath`) that can contain date and time symbols.  With a typical confifguration this allows KaBoom to create boom files in a path that would look something simmilar to this:

`hdfs://hadoop.company.com/logs/<YYYY>-<MM>-<DD>/<HH>/<topic>/data/<boom file>`

This allows workflows based off time periods to easily watch the HDFS file system and kick off jobs that then read boom files knowing they contain messages pertaining to their date/time parts of the path.

However, when there are multiple partions per Kafka topic it's hard to know when these files are ready for consumption.  If your workflow or job is assuming that all the data for the respective time period exists then it will need an indicator that KaBoom is finished writing boom files for a specific hour.   In addition, it's nice to know when KaBoom is finished with an entire day (i.e. all the messages for the hours for that day have all been written).

## KaBoom 0.8.2 Workers - Starting Their Shifts

KaBoom assigns partitons to clients that start a worker for each partition.  A partition will belong to a given topic, so for discussion consider a partiton really a combination of topic and partition.

When a worker is first created and begins work on a particular partition it starts a shift.  Shift's keep track of two important pieces of information, their `currentOffset` and the maximum observed timestamp of a message during the shift (`maxTimestamp`).  

For the offset, when the first shift is created it looks into ZooKeeper and grabs the offset that it needs to start consuming at.  If no offset is found, it starts at 0.  If that offset is out of range, then the behavior of the startup configuration property `auto.offset.reset` determines wether it starts consuming from the latest (most recent) or earliest (oldest) offset.  However, we'll just assume that the first shift created finds an offset in ZK and that it's within a valid range for the partition.

The `maxTimestamp` of each shift starts with `maxTimestamp = -1`.

Shifts have a duration (let's assume it's one hour) and calculate their start/end times based when that shift should have started (had it started on time).  For example, if KaBoom starts/restarts at 5:52pm, it determines that the start time would have been 5:00pm and the end time would have been 6:00pm (actual equation is `ts - ts % duration` where `ts` represents `System.currentTimeMillis()`).

## KaBoom 0.8.2 - Worker Shift Numbers

Shifts are numbered, the first shift of a worker is `#1` and each subsequent shift that gets started increments that counter by 1.

## KaBoom 0.8.2 - Message Consume Loop

When a worker consumes a message, it writes the message to a boom file received from `TimeBasedHdfsOutputPath` that is associated with the sprint number that it was created for.  The worker then compares the message's timestamp to the current shift's `maxTimestamp`.  If it's greater, then `maxTimestamp` is set to the timestamp of the current message.  This logic is contained in a loop that doesn't break until one of the worker's `stop()` or `abort()` methods are called.

At the top of this loop and before a message is consumed the worker checks whether `currentShift.isOver()`. If the current shift is over then it assigns `currentShift` to `previousShift` and then sets `currentShift` to a new shift created with a starting offset of `previousShift.getOffset()` and a shift number of `previousShift.shiftNumber + 1`.

If the current shift isn't over, it checks to see if a `previousShift != null`  and if there's one still hanging around it checks if `previousShift.isTimeToFinish()`.  It's time to finish when the shift's end time is greater than the running configuration option `fileCloseGraceTimeAfterExpiredMs` (default: 30000).  If it's time to finish the previous shift the worker calls `previousShift.finish(true)`.  This call instructs the worker's `TimeBasedHdfsOutputPath` to close off all boom files associated to `previousSprint.sprintNumber`.  After a boom file is successfully closed off, `TimeBasedHdfsOutputPath` has no record of it anymore.  The `true` indicates that after successfully closing all boom files created during the sprint that the metadata (`offset`/`maxTimestamp`) should be updated in ZooKeeper.  If an exception is thrown while the previous shift is finishing, then the message consume loop exits, and the worker calls `abort()`. Once the worker calls `previousShift.finish(true)` finishes the worker sets `previousShift = null`, making the current shift the only shift object instantiated within the worker.

## Persisting Metadata

When a shift persists it's metadata, it's important to understand what that entails.  For the offset, that's pretty easy, the offset is always the next offset that the Worker is going to attempt to consume.  These are always incremented by 1 after each message is consumed from Kafka (normally--leader failovers aside) and have nothing to do with the concept of time.

The `maxTimestamp` however, is a little different.  If `maxTimestamp == -1` then the timestamp that gets stored is `sprint.endTime`.  This ensures that if there are no messages for KaBoom to consume that KaBoom will be able to reliably indicate that the hour's (shift duration) boom files are eady for processsing  by external/downstream workflows (at least for the  partiton--there are likely more partitions in the topic). 

## KaBoom 0.8.2 - Shutting Down

Once `stop()` or `abort()` have been called, the next message consume loop iteration will break.  The only other way for the message consume loop to break is if an exception occurs, in which case, `abort()` is called. 

Immediatley following the message consume loop, `shutdown()` is called.  This method performs some housekeeping (removing gauge metrics, etc) and then determines if it's gracefully shutting down or aborting.

Graceful shutdowns check if `previousShift != null` and if so, then it calls `previousShift.finish()`.  The lack of the `true` boolean parameter indicates that the previousShift shouldn't persist it's metadata upon successfull closure of it's boom files.  It then calls `currentShift.finish(true)`, which does store the `offset` and `maxTimestamp` to ZK.

Aborting instructs the worker's `TimeBasedHdfsOutputPath` to delete all open/unfinished boom files regardless of associated sprint.

## Why This Matters

With the above design implemented, ZooKeeper is guaranteed to contain metadata for each partiton that get's updated hourly.  The metadata updates exist 100% independant of the timestamps within the messages that KaBoom recieves.  KaBoom will only ever persist an offest for a partion once the previous hour's boom files have been closed off succesfully.  It'll remember the latest timestamp that it ever parsed from a message within that hour, and reliably store that timestamp.

## Ready Flag Writer

The `ReadyFlagWriter` grabs all the topics that KaBoom is configured for, and then determines the earliest of the stored `maxTimestamps` for all it's partitons.   It then, starts at the beginning of the top of the previous hour and checks if 

* A) There's a `_READY` flag in that hour already
* B) That the timesatmp of the top of the hour is < `earliestMaxTimestamp`

Only if a ready flag doesn't already exist AND if all the partitions have `maxTimestamp`'s greater (i.e. later) than the top of that hour, would it write the `_READY` flag.

This flag gets written into the `hdfs://hadoop.company.com/logs/<YYYY>-<MM>-<DD>/<HH>/<topic>/data` directory AND to the `hdfs://hadoop.company.com/logs/<YYYY>-<MM>-<DD>/<HH>/<topic>` directory.

This is a per topic flag that is written into a specific hourly directory.

The hourly directory is created if one doesn't exist.

## Ready Flag Propagation

Topics often share a common HDFS root directory in their `TimeBasedHdfsOutputPath`, first KaBoom gathers a unique list of topics with common HDFS output paths and then assigns those topics for `_READY` flag propagation evenly amongst all connected KaBoom clients.

A topic-specific propagator thread is spawned every 10 minutes (if a earlier one isn't still running) and it performs a depth first traversal of the topic's HDFS root dir.  

If the path is an hourly directory (i.e. `hdfs://hadoop.company.com/logs/<YYYY>-<MM>-<DD>/<HH>`) it then it checks all child directories (topics, in our example) for a `_READY` flag.  If they all do it creates `hdfs://hadoop.company.com/logs/<YYYY>-<MM>-<DD>/<HH>/_READY`.

If the path is a daily directory (i.e. `hdfs://hadoop.company.com/logs/<YYYY>-<MM>-<DD>`) it then checks all the child directories (hours, in our example) for a `_READY` flag.  If they all do it creates `hdfs://hadoop.company.com/logs/<YYYY>-<MM>-<DD>/_READY`.