package com.sentienz.sas.xpt.utils;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Contains static convenience methods for low level operations (typically close
 * to IO).
 * 
 * @author Kasper Sørensen
 */
public class IO {

	private static final String CHARSET_NAME = "windows-1252";

	private IO() {
		// prevent instantiation
	}

	/**
	 * Converts an int-array to a byte-array. Makes it more convenient to use int
	 * literals in code.
	 * 
	 * @param arr
	 * @return
	 */
	public static byte[] toBytes(int... arr) {
		if (arr == null) {
			return null;
		}
		byte[] result = new byte[arr.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = (byte) arr[i];
		}
		return result;
	}

	public static String readString(byte[] buffer, int off, int len) {
		byte[] subset = readBytes(buffer, off, len);

		String str = getString(subset, CHARSET_NAME).trim();
		return str;
	}

	private static String getString(byte[] bytes, String encoding) {
		try {
			InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(bytes), encoding);
			char[] chars = new char[bytes.length * 2];
			int read = reader.read(chars);
			chars = Arrays.copyOf(chars, read);
			return new String(chars);
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static byte readByte(byte[] buffer, int off) {
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		return bb.get(off);
	}

	public static int readInt(byte[] buffer, int off) {
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		return bb.getInt(off);
	}

	public static double readDouble(byte[] buffer, int off) {
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		return bb.getDouble(off);
	}

	public static byte[] readBytes(byte[] data, int off, int len) {
		if (data.length < off + len) {
			throw new RuntimeException(
					"readBytes failed! data.length: " + data.length + ", off: " + off + ", len: " + len);
		}
		byte[] subset = new byte[len];
		System.arraycopy(data, off, subset, 0, len);
		return subset;
	}

	public static short readShort(byte[] buffer, int off) {
		ByteBuffer bb = ByteBuffer.wrap(buffer);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		return bb.getShort(off);
	}

	public static Number readNumber(byte[] buffer, int off, int len) {
		if (len == 1) {
			return readByte(buffer, off);
		} else if (len == 2) {
			return readShort(buffer, off);
		} else if (len == 4) {
			return readInt(buffer, off);
		} else if (len == 8) {
			return readDouble(buffer, off);
		} else {
			throw new UnsupportedOperationException("Number byte-length not supported: " + len);
		}
	}

	public static byte[] concat(byte[] arr1, byte[] arr2) {
		byte[] result = new byte[arr1.length + arr2.length];
		System.arraycopy(arr1, 0, result, 0, arr1.length);
		System.arraycopy(arr2, 0, result, arr1.length, arr2.length);
		return result;
	}
}
