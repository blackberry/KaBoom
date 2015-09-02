# Ready Flags

### Introduction

KaBoom parses the date and time from each message it consumes from Kafka.  The HDFS directories that it writes boom files within are based on a path template (`TimeBasedHdfsOutputPath`) that can contain date and time symbols.  With a typical confifguration this allows KaBoom to create boom files in a path that would look something simmilar to this:

hdfs://hadoop.company.com/logs/<topic>/<YYYY>-<MM>-<DD>/<HH>/<boom file>.bm

This allows workflows based off time periods to easily watch the HDFS file system and kick off jobs that then read boom files knowing they contain messages pertaining to their date/time parts of the path.

However, when there are multiple partions per Kafka topic it's hard to know when these files are ready for consumption.  If your workflow or job is assuming that all the data for the respective time period exists then it will need an indicator that KaBoom is finished writing boom files for a specific hour.   In addition, it's nice to know when KaBoom is finished with an entire day (i.e. all the messages for the hours for that day have all been written).

### KaBoom 0.8.2 Workers - Starting Their Shifts

KaBoom assigns partitons to clients that start a worker for each partition.  A partition will belong to a given topic, so for discussion consider a partiton really a combination of topic and partition.

When a worker is first created and begins work on a particular partition it starts a shift.  Workers keep track of two important pieces of information, their current offset, and the maximum observed timestamp of a message during the shift.  

For the offset, when the first shift created it looks into ZooKeeper and grabs the offset that it needs to start consuming at.  If no offset is found, it starts at 0.  If that offset is out of range, then the behavior of `auto.offset.reset` determines wether it start consuming from the latest (most recent) or earliest (oldest) offset.  However, we'll just assume that the shift finds an offset in ZK and that it's within range.

For the maximum observed timestamp each shift that starts sets `maxTimestamp = -1`.

Shifts have a duration (let's assume it's one hour) and calculate their start/end times based when that shift should have started (had it started on time).  For example, if KaBoom starts/restarts at 5:52pm, it determines that the start time would have been 5:00pm and the end time would have been 6:00pm (actual equation is `ts - ts % duration` where `ts` represents `System.currentTimeMillis()`).

### KaBoom 0.8.2 - Worker Shift Numbers

Shifts are numbered, the first shift of a worker is `#1` and each subsequent shift that gets started increments that counter by.

### KaBoom 0.8.2 - Message Consume Loop

When a worker consumes a message, it gets a boom file from it's `TimeBasedHdfsOutputPath` which itself contains an association of all the boom files that get opened and which sprint created them.

The worker then compares the message's timestamp to the current shift's `maxTimestamp`.  If it's greater, then `maxTimestamp` is set to the timestamp of the current message.  

It then writes the message to the boom file.  This logic is contained in a loop that doesn't break until the worker's `stop()` or `abort()` methods are called.

Within this loop (at the top) and before a message is even consumed, the worker checks whether `currentShift.isOver()`. If the current shift is over then it assigns `currentShift` to `previousShift` and then sets `currentShift` to a new shift with a starting offset of `previousShift.getOffset()` and shift number of `previousShift.shiftNumber + 1`.

If the current shift isn't over, it checks to see if a `previousShift` is hanging around and if so, it checks to see if `previousShift.isTimeToFinish()`.  It's time to finish when the shift's end time is greater than the running configuration option `fileCloseGraceTimeAfterExpiredMs` (default: 30000).  If it's time for the previous shift to finish, the worker calls `previousShift.finish(true)`.  The `true` indicates that if all goes well that it should persist it's metadata (`offset`/`maxTimestamp`) to ZooKeeper.  

The call to `previousShift.finish(true)` instructs it's `TimeBasedHdfsOutputPath` to close off all boom files associated to `previousSprint.sprintNumber`.  

Only if all boom files associated to `previousSprint.sprintNumber` are closed off successfully does the metadata get persisted.