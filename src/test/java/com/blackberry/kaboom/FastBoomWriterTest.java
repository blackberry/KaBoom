package com.blackberry.kaboom;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastBoomWriterTest {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
      .getLogger(FastBoomWriterTest.class);
  private static final Charset UTF8 = Charset.forName("UTF8");
  private static final Random rand = new Random();

  @Test
  public void testWriteFile() throws IOException {
    FileOutputStream out = new FileOutputStream("/tmp/test.bm");
    FastBoomWriter writer = new FastBoomWriter(out);

    byte[] message = "This is a test.  Let's make the line a bit longer by writing some stuff here."
        .getBytes(UTF8);

    writer.writeLine(1397268894000L, message, 0, message.length);

    writer.close();
  }

  @Test
  public void testWriteBigFile() throws IOException {
    FileOutputStream out = new FileOutputStream("/tmp/test2.bm");
    FastBoomWriter writer = new FastBoomWriter(out);

    byte[] chars = "abc".getBytes(UTF8);
    List<byte[]> messages = new ArrayList<byte[]>();
    for (int i = 0; i < 151; i++) {
      StringBuilder sb = new StringBuilder("This is a test. ");
      int extra = rand.nextInt() % 500;
      for (int j = 0; j < extra; j++) {
        sb.append(chars[rand.nextInt(chars.length)]);
      }
      messages.add(sb.toString().getBytes(UTF8));
    }

    byte[] message;
    for (int i = 0; i < 100000; i++) {
      message = messages.get(i % messages.size());
      writer.writeLine(System.currentTimeMillis(), message, 0, message.length);
    }

    writer.close();
  }
  // @Test
  // public void encode() throws IOException, ParseException {
  // OutputStream out = new BufferedOutputStream(new FileOutputStream(
  // "/tmp/test3.bm"));
  // FastBoomWriter writer = new FastBoomWriter(out);
  //
  // BufferedReader in = new BufferedReader(new InputStreamReader(
  // new FileInputStream("/tmp/testlogs")));
  //
  // TimestampParser tsp = new Rfc5424TimestampParser();
  // String line;
  // String[] tsAndMsg;
  // byte[] msg;
  // while ((line = in.readLine()) != null) {
  // tsAndMsg = tsp.splitLine(line);
  // msg = tsAndMsg[1].getBytes(UTF8);
  // writer.writeLine(tsp.parseTimestatmp(tsAndMsg[0]), msg, 0, msg.length);
  // }
  //
  // }
}
