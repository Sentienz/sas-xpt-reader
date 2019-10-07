package com.sentienz.sas.xpt;

public class XPTTypes {

  public static final int XPORT_MIN_DOUBLE_SIZE = 3;
  public static final int XPORT_MAX_DOUBLE_SIZE = 8;

  public static final int CN_TYPE_NATIVE = 0;
  public static final int CN_TYPE_XPORT = 1;
  public static final int CN_TYPE_IEEEB = 2;
  public static final int CN_TYPE_IEEEL = 3;

  public static String[] XPORT_MONTHS =
      {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};

  public static class XPTHeader {
    // size 9 bytes
    public String name;

    public int num1;
    public int num2;
    public int num3;
    public int num4;
    public int num5;
    public int num6;
  }

  public static class XPTNameString {
    public short ntype;
    public short nhfun;
    public short nlng;
    public short nvar0;

    // size 8 bytes
    public String nname;
    // size 40 bytes
    public String nlabel;
    // size 8 bytes
    public String nform;

    public short nfl;
    public short nfd;
    public short nfj;

    // size 2 bytes
    public String nfill;
    // size 8 bytes
    public String niform;

    public short nifl;
    public short nifd;
    public int npos;

    // size 32 bytes
    public String longname;

    public short labeln;

    // size 18 bytes
    public String rest;
  }

  public static class XPTContext {

    public long file_size;
    public long timestamp;

    public int obs_count;
    public int var_count;
    public int row_limit;
    public int row_length;
    public int parsed_row_count;

    // size 40*4 +1
    public String file_label;
    // size 32*4 +1
    public String table_name;

    public int version;

    public ReadStatVariable[] variables;
  }

  public static class TimeStamp {
    public int tm_isdst = -1;
    public short tm_mday;
    public short tm_mon;
    public short tm_year;
    public short tm_hour;
    public short tm_min;
    public short tm_sec;
  }

  public static class ReadStatVariable {
    public ReadstatType type;
    public int index;

    // size 300 bytes
    public String name;
    // size 256 bytes
    public String format;
    // size 1024 bytes
    public String label;

    public int offset;
    public int storage_width;
    public int user_width;

    public ReadstatLabelSet label_set;

    public ReadstatMissingness missingness;
    public ReadstatMeasure measure;
    public ReadstatAlignment alignment;

    public int display_width;
    public int decimals;
    public int skip;
    public int index_after_skipping;
  }

  public static class ReadstatMissingness {
    // size 32s
    ReadstatValue missing_ranges[];
    long missing_ranges_count;
  }

  public static class ReadstatValue {
    String tvalue;
    double value;
    public ReadstatType type;
    byte tag;
    int is_system_missing;
    int is_tagged_missing;
  };

  public static class ReadstatLabelSet {
    public ReadstatType type;
    // size 256 bytes
    String name;

    ReadstatValueLabel value_labels;
    long value_labels_count;
    long value_labels_capacity;

    long variables_count;
    long variables_capacity;
  }

  public static class ReadstatValueLabel {
    double double_key;
    int int32_key;
    char tag;

    String string_key;
    int string_key_len;

    String label;
    int label_len;
  }

  public static enum ReadstatType {
    READSTAT_TYPE_STRING, READSTAT_TYPE_INT8, READSTAT_TYPE_INT16, READSTAT_TYPE_INT32, READSTAT_TYPE_FLOAT, READSTAT_TYPE_DOUBLE, READSTAT_TYPE_STRING_REF
  };

  public static enum ReadstatAlignment {
    READSTAT_ALIGNMENT_UNKNOWN, READSTAT_ALIGNMENT_LEFT, READSTAT_ALIGNMENT_CENTER, READSTAT_ALIGNMENT_RIGHT
  };

  public static enum ReadstatMeasure {
    READSTAT_MEASURE_UNKNOWN, READSTAT_MEASURE_NOMINAL, READSTAT_MEASURE_ORDINAL, READSTAT_MEASURE_SCALE
  };

}
