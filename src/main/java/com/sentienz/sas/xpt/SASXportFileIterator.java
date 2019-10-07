package com.sentienz.sas.xpt;

import java.io.InputStream;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.sentienz.sas.xpt.XPTTypes.ReadstatValue;

public class SASXportFileIterator extends SASXportConverter implements Iterator<List<String>> {

	private List<String> crecord = null;
	private List<ReadstatValue> cPrimitiveRecord = null;
	private byte crow[] = null;

	public SASXportFileIterator(String fileName) throws Exception {
		super(fileName);
		init();
	}

	public SASXportFileIterator(InputStream is) throws Exception {
		super(is);
		init();
	}

	public SASXportFileIterator(String fileName, int offset) throws Exception {
		this(fileName);
		seek(offset);
	}

	public boolean hasNext() {
		return !isDone();
	}

	public List<String> next() {
		crecord = getRecord();
		cPrimitiveRecord = getPrimitiveRecord();
		crow = getRow();
		try {
			readNextRecord();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return crecord;
	}

	public List<ReadstatValue> nextPrimitive() {
		next();
		return cPrimitiveRecord;
	}

	public byte[] nextRaw() {
		next();
		return crow;
	}

	public static void main(String[] args) {
		try {

			SASXportFileIterator iterator = new SASXportFileIterator("/grid/data/xpt/test3.sasxpt");
			while (iterator.hasNext()) {
				List<String> row = iterator.next();
				System.out.println(new Gson().toJson(row));
			}
			System.out.println("Total Rows: " + iterator.getRowCount());
			iterator.close();

			Calendar cal = Calendar.getInstance();
			cal.set(1960, 1, 1);
			cal.add(Calendar.DAY_OF_YEAR, 19778);
			System.out.println(cal.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
