package com.blackberry.kaboom;

public class IntParser {
  private int intFromBytesPos;
  private int intFromBytesReturn;

  public int intFromBytes(byte[] b, int start, int length) {
    intFromBytesReturn = 0;
    intFromBytesPos = start;
    while (intFromBytesPos < start + length) {
      intFromBytesReturn = 10 * intFromBytesReturn + (b[intFromBytesPos] - '0');
      intFromBytesPos++;
    }
    return intFromBytesReturn;
  }
}
