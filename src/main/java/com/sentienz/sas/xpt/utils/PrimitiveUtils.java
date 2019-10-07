package com.sentienz.sas.xpt.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class PrimitiveUtils {

	/**
	 * Byte swap a single short value.
	 * 
	 * @param value Value to byte swap.
	 * @return Byte swapped representation.
	 */
	public static short swap(short value) {
		int b1 = value & 0xff;
		int b2 = (value >> 8) & 0xff;

		return (short) (b1 << 8 | b2 << 0);
	}

	/**
	 * Byte swap a single int value.
	 * 
	 * @param value Value to byte swap.
	 * @return Byte swapped representation.
	 */
	public static int swap(int value) {
		int b1 = (value >> 0) & 0xff;
		int b2 = (value >> 8) & 0xff;
		int b3 = (value >> 16) & 0xff;
		int b4 = (value >> 24) & 0xff;

		return b1 << 24 | b2 << 16 | b3 << 8 | b4 << 0;
	}

	/**
	 * Byte swap a single long value.
	 * 
	 * @param value Value to byte swap.
	 * @return Byte swapped representation.
	 */
	public static long swap(long value) {
		long b1 = (value >> 0) & 0xff;
		long b2 = (value >> 8) & 0xff;
		long b3 = (value >> 16) & 0xff;
		long b4 = (value >> 24) & 0xff;
		long b5 = (value >> 32) & 0xff;
		long b6 = (value >> 40) & 0xff;
		long b7 = (value >> 48) & 0xff;
		long b8 = (value >> 56) & 0xff;

		return b1 << 56 | b2 << 48 | b3 << 40 | b4 << 32 | b5 << 24 | b6 << 16 | b7 << 8 | b8 << 0;
	}

	/**
	 * Byte swap a single float value.
	 * 
	 * @param value Value to byte swap.
	 * @return Byte swapped representation.
	 */
	public static float swap(float value) {
		int intValue = Float.floatToIntBits(value);
		intValue = swap(intValue);
		return Float.intBitsToFloat(intValue);
	}

	/**
	 * Byte swap a single double value.
	 * 
	 * @param value Value to byte swap.
	 * @return Byte swapped representation.
	 */
	public static double swap(double value) {
		long longValue = Double.doubleToLongBits(value);
		longValue = swap(longValue);
		return Double.longBitsToDouble(longValue);
	}

	/**
	 * Byte swap an array of shorts. The result of the swapping is put back into the
	 * specified array.
	 *
	 * @param array Array of values to swap
	 */
	public static void swap(short[] array) {
		for (int i = 0; i < array.length; i++)
			array[i] = swap(array[i]);
	}

	/**
	 * Byte swap an array of ints. The result of the swapping is put back into the
	 * specified array.
	 * 
	 * @param array Array of values to swap
	 */
	public static void swap(int[] array) {
		for (int i = 0; i < array.length; i++)
			array[i] = swap(array[i]);
	}

	/**
	 * Byte swap an array of longs. The result of the swapping is put back into the
	 * specified array.
	 * 
	 * @param array Array of values to swap
	 */
	public static void swap(long[] array) {
		for (int i = 0; i < array.length; i++)
			array[i] = swap(array[i]);
	}

	/**
	 * Byte swap an array of floats. The result of the swapping is put back into the
	 * specified array.
	 * 
	 * @param array Array of values to swap
	 */
	public static void swap(float[] array) {
		for (int i = 0; i < array.length; i++)
			array[i] = swap(array[i]);
	}

	/**
	 * Byte swap an array of doubles. The result of the swapping is put back into
	 * the specified array.
	 * 
	 * @param array Array of values to swap
	 */
	public static void swap(double[] array) {
		for (int i = 0; i < array.length; i++)
			array[i] = swap(array[i]);
	}

	public static byte[] intToBytes(final int num) {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(num);
		return bb.array();
	}

	public static byte[] longToBytes(final long num) {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(num);
		return bb.array();
	}

	public static double toDouble(byte[] bytes) {
		return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble();
	}

	public static long toLong(byte[] bytes) {
		return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong();
	}

	public static long toLongLittle(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getLong();
	}

	public static void memset(byte[] buffer, byte val, int len) {
		for (int i = 0; i < len; i++) {
			buffer[i] = val;
		}
	}

	public static boolean memcmp(byte[] tgt, int tgt_off, byte[] src, int src_off, int len) {
		for (int i = 0; i < len; i++) {
			if (tgt[i + tgt_off] != src[i + src_off])
				return false;
		}
		return true;
	}

	public static void memcpy(byte[] tgt, int tgt_off, byte[] src, int src_off, int len) {
		for (int i = 0; i < len; i++) {
			tgt[i + tgt_off] = src[i + src_off];
		}
	}

	public static void memreverse(byte[] intp, int len) {
		int i, j;
		byte save;
		j = len / 2;
		for (i = 0; i < j; i++) {
			save = intp[i];
			intp[i] = intp[len - i - 1];
			intp[len - i - 1] = save;
		}
	}

	public static double xpt2ieeeSimple(byte[] xport) {
		long ibm = toLong(xport);

		long sign = ibm & 0x8000000000000000l;
		long exponent = (ibm & 0x7f00000000000000l) >> 56;
		long mantissa = ibm & 0x00ffffffffffffffl;

		if (mantissa == 0) {
			if (xport[0] == 0x00)
				return 0.0d;
			else
				return Double.NaN;
		}
		int shift = 3;
		if ((ibm & 0x0080000000000000l) != 0)
			shift = 3;
		else if ((ibm & 0x0040000000000000l) != 0)
			shift = 2;
		else if ((ibm & 0x0020000000000000l) != 0)
			shift = 1;
		else
			shift = 0;
		mantissa >>= shift;
		mantissa &= 0xffefffffffffffffl;

		exponent -= 65;
		exponent <<= 2;
		exponent += shift + 1023;

		long ieee = sign | (exponent << 52) | mantissa;
		byte[] out = longToBytes(ieee);

		return toDouble(out);
	}

}
