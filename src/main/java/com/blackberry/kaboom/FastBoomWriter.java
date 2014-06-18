package com.blackberry.kaboom;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.zip.Deflater;

public class FastBoomWriter {
  private static final Charset UTF8 = Charset.forName("UTF8");

  private static final byte[] MAGIC_NUMBER = new byte[] { 'O', 'b', 'j', 1 };
  private static final String SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"logBlock\","
      + "\"fields\":["
      + "{\"name\":\"second\",\"type\":\"long\"},"
      + "{\"name\":\"createTime\",\"type\":\"long\"},"
      + "{\"name\":\"blockNumber\",\"type\":\"long\"},"
      + "{\"name\":\"logLines\",\"type\":"
      + "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"messageWithMillis\",\"fields\":["
      + "{\"name\":\"ms\",\"type\":\"long\"},"
      + "{\"name\":\"message\",\"type\":\"string\"}]}}}]}";
  private static final byte[] SCHEMA_BYTES = SCHEMA_STRING.getBytes(UTF8);

  private final byte[] syncMarker;

  private OutputStream out;

  public FastBoomWriter(OutputStream out) throws IOException {
    this.out = out;

    Random rand = new Random();
    syncMarker = new byte[16];
    rand.nextBytes(syncMarker);

    writeHeader();
  }

  private void writeHeader() throws IOException {
    out.write(MAGIC_NUMBER);

    // 2 entries in the metadata
    encodeLong(2L);
    out.write(longBytes, 0, longBuffer.position());

    // Write schema
    writeBytes("avro.schema".getBytes(UTF8));
    writeBytes(SCHEMA_BYTES);

    // Write codec
    writeBytes("avro.codec".getBytes(UTF8));
    writeBytes("deflate".getBytes(UTF8));

    // End the map
    encodeLong(0L);
    out.write(longBytes, 0, longBuffer.position());

    out.write(syncMarker);
  }

  private void writeBytes(byte[] bytes) throws IOException {
    encodeLong(bytes.length);
    out.write(longBytes, 0, longBuffer.position());
    out.write(bytes);
  }

  private byte[] longBytes = new byte[10];
  private ByteBuffer longBuffer = ByteBuffer.wrap(longBytes);

  private void encodeLong(long n) {
    longBuffer.clear();
    n = (n << 1) ^ (n >> 63);
    if ((n & ~0x7FL) != 0) {
      longBuffer.put((byte) ((n | 0x80) & 0xFF));
      n >>>= 7;
      if (n > 0x7F) {
        longBuffer.put((byte) ((n | 0x80) & 0xFF));
        n >>>= 7;
        if (n > 0x7F) {
          longBuffer.put((byte) ((n | 0x80) & 0xFF));
          n >>>= 7;
          if (n > 0x7F) {
            longBuffer.put((byte) ((n | 0x80) & 0xFF));
            n >>>= 7;
            if (n > 0x7F) {
              longBuffer.put((byte) ((n | 0x80) & 0xFF));
              n >>>= 7;
              if (n > 0x7F) {
                longBuffer.put((byte) ((n | 0x80) & 0xFF));
                n >>>= 7;
                if (n > 0x7F) {
                  longBuffer.put((byte) ((n | 0x80) & 0xFF));
                  n >>>= 7;
                  if (n > 0x7F) {
                    longBuffer.put((byte) ((n | 0x80) & 0xFF));
                    n >>>= 7;
                    if (n > 0x7F) {
                      longBuffer.put((byte) ((n | 0x80) & 0xFF));
                      n >>>= 7;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    longBuffer.put((byte) n);
  }

  private long ms;
  private long second;

  private long blockNumber = 0L;

  private long logBlockSecond = 0L;
  private byte[] logBlockBytes = new byte[1024];
  private ByteBuffer logBlockBuffer = ByteBuffer.wrap(logBlockBytes);

  private long logLineCount;
  private byte[] logLinesBytes = new byte[2 * 1024 * 1024];
  private ByteBuffer logLinesBuffer = ByteBuffer.wrap(logLinesBytes);

  public void writeLine(long timestamp, byte[] message, int offset, int length)
      throws IOException {
    ms = timestamp % 1000l;
    second = timestamp / 1000l;

    // If the buffer is too full to hold it, or the second has changed, then
    // write out the block first.
    if ((logBlockBuffer.position() > 0 && second != logBlockSecond)
        || logBlockBytes.length - logBlockBuffer.position() < 10 + 10 + length) {
      writeLogBlock();
    }

    if (logBlockBuffer.position() == 0) {
      logBlockBuffer.clear();
      logLinesBuffer.clear();
      logLineCount = 0;

      // second
      encodeLong(second);
      logBlockBuffer.put(longBytes, 0, longBuffer.position());

      // createTime
      encodeLong(System.currentTimeMillis());
      logBlockBuffer.put(longBytes, 0, longBuffer.position());

      // block number
      encodeLong(blockNumber++);
      logBlockBuffer.put(longBytes, 0, longBuffer.position());
    }

    encodeLong(ms);
    logLinesBuffer.put(longBytes, 0, longBuffer.position());

    encodeLong(length);
    logLinesBuffer.put(longBytes, 0, longBuffer.position());
    logLinesBuffer.put(message, offset, length);

    logLineCount++;
  }

  private long avroBlockRecordCount = 0L;
  private byte[] avroBlockBytes = new byte[2 * 1024 * 1024];
  private ByteBuffer avroBlockBuffer = ByteBuffer.wrap(avroBlockBytes);

  private void writeLogBlock() throws IOException {
    // We need room for the logBlockBuffer, the number of records in
    // logLinesBuffer (up to 10) and the logLinesBuffer. If not, then we need to
    // flush.
    if (avroBlockBytes.length - avroBlockBuffer.position() < logBlockBuffer
        .position() + 10 + logLinesBuffer.position()) {
      writeAvroBlock();
    }

    avroBlockBuffer.put(logBlockBytes, 0, logBlockBuffer.position());
    encodeLong(logLineCount);
    avroBlockBuffer.put(longBytes, 0, longBuffer.position());
    avroBlockBuffer.put(logLinesBytes, 0, logLinesBuffer.position());
    encodeLong(0L);
    avroBlockBuffer.put(longBytes, 0, longBuffer.position());

    avroBlockRecordCount++;

    logBlockBuffer.clear();
    logLineCount = 0L;
    logLinesBuffer.clear();
  }

  private int compressedSize;
  private byte[] compressedBlockBytes = new byte[2 * 1024 * 1024];
  private Deflater deflater = new Deflater(6, true);

  private void writeAvroBlock() throws IOException {
    encodeLong(avroBlockRecordCount);
    out.write(longBytes, 0, longBuffer.position());

    deflater.reset();
    deflater.setInput(avroBlockBytes, 0, avroBlockBuffer.position());
    deflater.finish();
    compressedSize = deflater.deflate(compressedBlockBytes, 0,
        compressedBlockBytes.length);

    encodeLong(compressedSize);
    out.write(longBytes, 0, longBuffer.position());

    out.write(compressedBlockBytes, 0, compressedSize);

    out.write(syncMarker);

    avroBlockBuffer.clear();
    avroBlockRecordCount = 0L;
  }

  public void close() throws IOException {
    if (logBlockBuffer.position() > 0) {
      writeLogBlock();
    }
    if (avroBlockBuffer.position() > 0) {
      writeAvroBlock();
    }
    out.close();
  }
}
