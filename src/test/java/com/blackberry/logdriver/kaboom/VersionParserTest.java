package com.blackberry.logdriver.kaboom;

import static org.junit.Assert.*;

import org.junit.Test;

import com.blackberry.logdriver.kaboom.VersionParser;

public class VersionParserTest {
  @Test
  public void testSkipPri() {
    VersionParser ver = new VersionParser();

    byte[] msg;

    // Test all good values
    for (int i = 1; i <= 999; i++) {
      msg = (i + " This is a test.").getBytes();
      assertTrue(ver.parseVersion(msg, 0, msg.length));
      assertEquals(("" + i).length(), ver.getVersionLength());

      msg = ("<123>" + i + " This is a test.").getBytes();
      assertTrue(ver.parseVersion(msg, 5, msg.length));
      assertEquals(("" + i).length(), ver.getVersionLength());
    }

    // Various bad values
    assertFalse(ver.parseVersion("12This is a test".getBytes(), 0, 15));
    assertFalse(ver.parseVersion("1234 This is a test".getBytes(), 0, 15));

    assertFalse(ver.parseVersion("01 This is a test".getBytes(), 0, 15));
    assertFalse(ver.parseVersion("0 This is a test".getBytes(), 0, 15));
  }
}
