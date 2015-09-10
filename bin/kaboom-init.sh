#!/bin/bash
#
#  /etc/rc.d/init.d/kaboom
#
# Starts the kaboom daemon
#
# chkconfig: - 95 5
# description: Collects logs, and writes them to Kafka.
### BEGIN INIT INFO
# Provides:       kaboom
# Required-Start: $local_fs $remote_fs $network
# Required-Stop:  $local_fs $remote_fs $network
# Default-Start:
# Default-Stop:
# Description:    Start the kaboom service
### END INIT INFO

setup() {
  if [ "x$CONFIGDIR" == "x" ]
  then
    CONFIGDIR=/opt/kaboom/config
  fi
  . $CONFIGDIR/kaboom-env.sh

  PROG="kaboom"

  OS=unknown

  if [ -e "/etc/SuSE-release" ]
  then
    OS=suse
  elif [ -e "/etc/redhat-release" ]
  then
    OS=redhat
  else
    echo "Could not determine OS."
  fi

  # Source function library.
  [ "$OS" == "redhat" ] && . /etc/init.d/functions
  [ "$OS" == "suse"   ] && . /etc/rc.status

  RETVAL=0
}

start() {
  setup

  # Check if kaboom is already running
  PIDFILE="$PIDBASE/kaboom.pid"
  if [ -f $PIDFILE ]
  then
    PID=`head -1 $PIDFILE`
    if [ -e /proc/$PID ]
    then
      echo "$PROG is already running (PID $PID)"
      return 1
    else
      rm -f $PIDFILE
    fi
  fi

  echo -n $"Starting $PROG: "
  # kdestroy first to ensure that we're not logged in as anyone else.
  # Shouldn't be, if this is a dedicated user.
  # Also, ignore the message we get when the crentials cache is empty.
  /usr/bin/kdestroy 2>&1 | grep -v 'kdestroy: No credentials cache found while destroying cache'
  . $CONFIGDIR/kaboom-env.sh

  nohup $JAVA $JAVA_OPTS -classpath "$CLASSPATH" com.blackberry.bdp.kaboom.KaBoom $CONFIGDIR/kaboom.properties >$LOGDIR/server.out 2>&1 &

  RETVAL=$?
  PID=$!

  if [ $RETVAL -eq 0 ]
  then
    [ "$OS" == "redhat"  ] && success
    [ "$OS" == "suse"    ] && echo -n $rc_done
    [ "$OS" == "unknown" ] && echo -n "... done"
    echo $PID > $PIDFILE
  else
    failure
  fi
  echo
}

stop() {
  setup

  echo -n $"Stopping $PROG: "
  PIDFILE="$PIDBASE/kaboom.pid"

  if [ -f $PIDFILE ]
  then
    PID=`head -1 $PIDFILE`
    if [ -e /proc/$PID ]
    then
      kill $PID
      for i in `seq 1 60`
      do
        sleep 1

        if [ ! -e /proc/$PID ]
        then
          rm -f $PIDFILE
          [ "$OS" == "redhat"  ] && success
          [ "$OS" == "suse"    ] && echo -n $rc_done
          [ "$OS" == "unknown" ] && echo -n "... done"
          RETVAL=0
          break
        fi
      done

      if [ -e /proc/$PID ]
      then
        echo -n "Trying kill -9 "
        kill -9 $PID

        for i in `seq 1 60`
        do
          sleep 1

          if [ ! -e /proc/$PID ]
          then
            rm -f $PIDFILE
            [ "$OS" == "redhat"  ] && success
            [ "$OS" == "suse"    ] && echo -n $rc_done
            [ "$OS" == "unknown" ] && echo -n "... done"
            RETVAL=0
            break
          fi
        done
      fi

      if [ -e /proc/$PID ]
      then
        echo "Could not kill "
        [ "$OS" == "redhat"  ] && failure
        [ "$OS" == "suse"    ] && echo -n $rc_failed
        [ "$OS" == "unknown" ] && echo -n "... failed"
        RETVAL=1
      fi

    else
      echo -n "PID $PID is not running "
      rm -f $PIDFILE
      [ "$OS" == "redhat"  ] && success
      [ "$OS" == "suse"    ] && echo -n $rc_done
      [ "$OS" == "unknown" ] && echo -n "... done"
      RETVAL=0
    fi

  else
    echo -n "Could not find $PIDFILE"
    [ "$OS" == "redhat"  ] && failure
    [ "$OS" == "suse"    ] && echo -n $rc_failed
    [ "$OS" == "unknown" ] && echo -n "... failed"
    RETVAL=1

  fi

  echo
}

restart() {
  stop
  start
}

_status() {
  setup

  PIDFILE="$PIDBASE/kaboom.pid"
  status -p $PIDFILE $PROG
}

# make functions available under su
export -f setup
export -f start
export -f stop
export -f restart
export -f _status

setup
case "$1" in
start)
  if [ "x$KABOOM_USER" != "x" ]
  then
    su $KABOOM_USER -c start
    service epagent restart
  else
    start
  fi
  ;;
stop)
  if [ "x$KABOOM_USER" != "x" ]
  then
    su $KABOOM_USER -c stop
  else
    stop
  fi
  ;;
restart)
  if [ "x$KABOOM_USER" != "x" ]
  then
    su $KABOOM_USER -c restart
    service epagent restart
  else
    restart
  fi
  ;;
status)
  if [ "x$KABOOM_USER" != "x" ]
  then
    su $KABOOM_USER -c _status
  else
    _status
  fi
  ;;
*)
  echo $"Usage: $0 {start|stop|restart|status}"
  exit 1
esac

exit $?
