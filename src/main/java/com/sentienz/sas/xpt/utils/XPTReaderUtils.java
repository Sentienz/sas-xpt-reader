package com.sentienz.sas.xpt.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class XPTReaderUtils {

	public static SimpleDateFormat DATE5_FORMAT = new SimpleDateFormat("ddMMM ");
	public static SimpleDateFormat DATE6_FORMAT = new SimpleDateFormat(" ddMMM ");
	public static SimpleDateFormat DATE7_FORMAT = new SimpleDateFormat("ddMMMYY ");
	public static SimpleDateFormat DATE8_FORMAT = new SimpleDateFormat(" ddMMMYY ");
	public static SimpleDateFormat DATE9_FORMAT = new SimpleDateFormat("ddMMMYYYY");
	public static SimpleDateFormat DATE11_FORMAT = new SimpleDateFormat("dd-MMM-YYYY");

	public static String getString(byte[] line, int offset, int len) {

		byte[] data = new byte[len];
		for (int i = 0; i < len; i++) {
			data[i] = line[offset + i];
		}
		return new String(data).trim();
	}

	public static int getInteger(byte[] line, int offset, int len) {
		String val = getString(line, offset, len);
		if (val.length() <= 0)
			return 0;
		return Integer.parseInt(val);
	}

	public static short getShort(byte[] line, int offset, int len) {
		String val = getString(line, offset, 2);
		if (val.length() <= 0)
			return 0;
		return Short.parseShort(val);
	}

	public static int getPrimitiveInteger(byte[] buffer, int offset) {

		int val = ((buffer[offset + 0] & 0xFF) << 24) | ((buffer[offset + 1] & 0xFF) << 16)
				| ((buffer[offset + 2] & 0xFF) << 8) | ((buffer[offset + 3] & 0xFF) << 0);
		return val;
	}

	public static short getPrimitiveShort(byte[] buffer, int offset) {
		short val = (short) (((buffer[offset + 0] & 0xFF) << 8) | ((buffer[offset + 1] & 0xFF) << 0));
		return val;
	}

	public static String convertSASDate9ToString(String dtformat, double date) {
		int num = (int) date;
		SimpleDateFormat format = DATE9_FORMAT;
		if ("date5".equals(dtformat))
			format = DATE5_FORMAT;
		else if ("date6".equals(dtformat))
			format = DATE6_FORMAT;
		else if ("date7".equals(dtformat))
			format = DATE7_FORMAT;
		else if ("date8".equals(dtformat))
			format = DATE8_FORMAT;
		else if ("date9".equals(dtformat))
			format = DATE9_FORMAT;
		else if ("date11".equals(dtformat))
			format = DATE11_FORMAT;

		Calendar cal = Calendar.getInstance();
		cal.set(1960, 0, 1);
		cal.add(Calendar.DAY_OF_YEAR, num);

		String formatted = format.format(cal.getTime());
		return formatted;
	}
}
