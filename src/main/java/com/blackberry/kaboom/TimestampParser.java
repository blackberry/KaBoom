package com.blackberry.kaboom;

import java.text.ParseException;
import java.util.Calendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampParser {
  private static final Logger LOG = LoggerFactory
      .getLogger(TimestampParser.class);

  public static final int NO_ERROR = 0;
  public static final int ERROR = 1;

  private int error = NO_ERROR;

  private IntParser intParser = new IntParser();

  private byte[] bytes;
  private int pos;
  private int limit;
  private int length;
  private Calendar cal;
  private final TimeZone UTC = TimeZone.getTimeZone("UTC");

  public TimestampParser() {
    cal = Calendar.getInstance(UTC);
  }

  public void parse(byte[] bytes, int i, int length) {
    try {
      cal.clear();
      this.bytes = bytes;
      pos = i;
      limit = i + length;
      error = NO_ERROR;

      if (length == 0) {
        LOG.error("Can't parse date from zero length byte array.");
        error = ERROR;
        return;
      }

      // It it start with a number assume YYYY MM DD HH MM SS[.ffffff][TZ] with
      // arbitrary separators.
      if (bytes[pos] >= '0' && bytes[pos] <= '9') {
        parseYyyyMmDd();

      }
      // If it starts with a month name assume MMM DD HH MM SS[.ffffff][TZ] with
      // arbitrary separators
      else if (bytes[pos] >= 'A' && bytes[pos] <= 'S') {
        parseMmmDd();

      } else {
        LOG.info("Can't parse line starting with {}", new String(bytes, pos, 1));
        error = ERROR;
        return;
      }

      advance();
      parseHour();
      advance();
      parseMinute();
      advance();
      parseSeconds();
      parseFractions();
      parseTZ();

      this.length = pos - i;
    } catch (Throwable t) {
      LOG.error("Error parsing timestamp.", t);
      error = ERROR;
    }
  }

  int tzDir;
  int tzHour;
  int tzMinute;

  private void parseTZ() {
    // Either Z, or +/-HH[[:]MM]
    if (bytes[pos] == 'Z') {
      pos++;
      return;
    }

    if (bytes[pos] == '-') {
      tzDir = 1;
      pos++;
    } else if (bytes[pos] == '+') {
      tzDir = -1;
      pos++;
    } else {
      return;
    }

    if (bytes[pos] == '0') {
      tzHour = intParser.intFromBytes(bytes, pos + 1, 1);
      cal.add(Calendar.HOUR_OF_DAY, tzDir * tzHour);
      pos += 2;
    } else {
      tzHour = intParser.intFromBytes(bytes, pos, 2);
      cal.add(Calendar.HOUR_OF_DAY, tzDir * tzHour);
      pos += 2;
    }

    if (bytes[pos] == ':') {
      pos++;
    }

    if (bytes[pos] == '0') {
      tzMinute = intParser.intFromBytes(bytes, pos + 1, 1);
      cal.add(Calendar.MINUTE, tzDir * tzMinute);
      pos += 2;
    } else if (bytes[pos] >= '0' && bytes[pos] <= '9') {
      tzMinute = intParser.intFromBytes(bytes, pos, 2);
      cal.add(Calendar.MINUTE, tzDir * tzMinute);
      pos += 2;
    }
  }

  int fracStart;
  int fracLength;

  private void parseFractions() {
    if (bytes[pos] == '.') {
      pos++;
      fracStart = pos;
      while (pos < limit && bytes[pos] >= '0' && bytes[pos] <= '9') {
        pos++;
      }
      fracLength = pos - fracStart;
      if (fracLength == 0) {
        // dot and no numbers? I'll allow it.
        cal.set(Calendar.MILLISECOND, 0);
      } else if (fracLength == 1) {
        cal.set(Calendar.MILLISECOND,
            100 * intParser.intFromBytes(bytes, fracStart, fracLength));
      } else if (fracLength == 2) {
        cal.set(Calendar.MILLISECOND,
            10 * intParser.intFromBytes(bytes, fracStart, fracLength));
      } else {
        // longer than 3, just read three digits
        cal.set(Calendar.MILLISECOND,
            intParser.intFromBytes(bytes, fracStart, 3));
      }
    }
  }

  private void parseSeconds() {
    // two digits, zero padded.
    if (bytes[pos] == '0') {
      cal.set(Calendar.SECOND, intParser.intFromBytes(bytes, pos + 1, 1));
    } else {
      cal.set(Calendar.SECOND, intParser.intFromBytes(bytes, pos, 2));
    }
    pos += 2;
  }

  private void parseMinute() {
    // two digits, zero padded.
    if (bytes[pos] == '0') {
      cal.set(Calendar.MINUTE, intParser.intFromBytes(bytes, pos + 1, 1));
    } else {
      cal.set(Calendar.MINUTE, intParser.intFromBytes(bytes, pos, 2));
    }
    pos += 2;
  }

  private void parseHour() {
    // two digits, zero padded.
    if (bytes[pos] == '0') {
      cal.set(Calendar.HOUR_OF_DAY, intParser.intFromBytes(bytes, pos + 1, 1));
    } else {
      cal.set(Calendar.HOUR_OF_DAY, intParser.intFromBytes(bytes, pos, 2));
    }
    pos += 2;
  }

  private int areEqualPos;

  // Compare two byte array slices
  private boolean areEqual(byte[] b1, int p1, byte[] b2, int p2, int length) {
    areEqualPos = 0;
    while (areEqualPos < length) {
      if (b1[p1 + areEqualPos] != b2[p2 + areEqualPos]) {
        return false;
      }
      areEqualPos++;
    }
    return true;
  }

  private byte b;

  private void advance() throws ParseException {
    while (true) {
      b = bytes[pos];
      if (b >= '0' && b <= '9') {
        break;
      }
      if (b == ' ' || b == '-' || b == ':' || b == 'T') {
        pos++;
      } else {
        throw new ParseException("Bad separator in timestamp: "
            + new String(bytes, pos, 1), pos);
      }
    }
  }

  private void parseMmmDd() throws ParseException {
    assumeYear();
    parseMmm();
    advance();
    parseDd();
  }

  private void parseDd() {
    // Choices for day include two digits, space padded or zero padded.
    if (bytes[pos] == ' ' || bytes[pos] == '0') {
      cal.set(Calendar.DAY_OF_MONTH, intParser.intFromBytes(bytes, pos + 1, 1));
    } else {
      cal.set(Calendar.DAY_OF_MONTH, intParser.intFromBytes(bytes, pos, 2));
    }
    pos += 2;
  }

  private static final byte[][] months = new byte[][] { "Jan".getBytes(),
      "Feb".getBytes(), "Mar".getBytes(), "Apr".getBytes(), "May".getBytes(),
      "Jun".getBytes(), "Jul".getBytes(), "Aug".getBytes(), "Sep".getBytes(),
      "Oct".getBytes(), "Nov".getBytes(), "Dec".getBytes() };

  private int parseMmmPos;

  private void parseMmm() throws ParseException {
    for (parseMmmPos = 0; parseMmmPos < months.length; parseMmmPos++) {
      if (areEqual(months[parseMmmPos], 0, bytes, pos, 3)) {
        pos += 3;
        cal.set(Calendar.MONTH, parseMmmPos);
        return;
      }
    }
    throw new ParseException("Unrecognized month string.", pos);
  }

  private void parseYyyyMmDd() throws ParseException {
    parseYyyy();
    advance();
    parseMm();
    advance();
    parseDd();

  }

  private void parseMm() {
    // two digits, zero padded.
    if (bytes[pos] == '0') {
      cal.set(Calendar.MONTH, intParser.intFromBytes(bytes, pos + 1, 1) - 1);
    } else {
      cal.set(Calendar.MONTH, intParser.intFromBytes(bytes, pos, 2) - 1);
    }
    pos += 2;
  }

  private void parseYyyy() {
    // Hopefully we won't have to deal with zero padded years.
    cal.set(Calendar.YEAR, intParser.intFromBytes(bytes, pos, 4));
    pos += 4;
  }

  private void assumeYear() {
    cal.set(Calendar.YEAR, getCurrentCal().get(Calendar.YEAR));
  }

  private Calendar currentCal = Calendar.getInstance(UTC);

  private Calendar getCurrentCal() {
    currentCal.setTimeInMillis(System.currentTimeMillis());
    return currentCal;
  }

  public int getError() {
    return error;
  }

  public long getTimestamp() {
    return cal.getTimeInMillis();
  }

  public int getLength() {
    return length;
  }

}