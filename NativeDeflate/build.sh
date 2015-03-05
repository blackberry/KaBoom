$JAVA_HOME/bin/javah -cp "target/*"  com.blackberry.bdp.kaboom.FastBoomWriter

gcc  -I $JAVA_HOME/include/ -I $JAVA_HOME/include/linux/ -shared -o libNativeDeflate.so NativeDeflate.c -fPIC -lz
