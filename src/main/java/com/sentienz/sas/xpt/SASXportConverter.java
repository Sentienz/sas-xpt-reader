package com.sentienz.sas.xpt;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.google.gson.Gson;
import com.sentienz.sas.xpt.XPTTypes.ReadStatVariable;
import com.sentienz.sas.xpt.XPTTypes.ReadstatAlignment;
import com.sentienz.sas.xpt.XPTTypes.ReadstatType;
import com.sentienz.sas.xpt.XPTTypes.ReadstatValue;
import com.sentienz.sas.xpt.XPTTypes.TimeStamp;
import com.sentienz.sas.xpt.XPTTypes.XPTContext;
import com.sentienz.sas.xpt.XPTTypes.XPTHeader;
import com.sentienz.sas.xpt.XPTTypes.XPTNameString;
import com.sentienz.sas.xpt.utils.PrimitiveUtils;
import com.sentienz.sas.xpt.utils.XPTReaderUtils;
import com.sentienz.sas.xpt.utils.IO;

public class SASXportConverter implements Closeable {

	public static int LINE_LEN = 80;

	public static int NAMESTR_LEN = 140;

	public static short SAS_COLUMN_TYPE_CHR = 0x02;

	protected boolean debug = false;
	protected boolean processBlankRecords = false;
	protected boolean done = false;
	protected boolean convertDate9ToString = true;
	protected InputStream rawin;

	protected XPTContext ctx;
	protected DataInputStream in;

	protected byte[] DUMMY_BUFFER = new byte[LINE_LEN];

	protected long offset = 0;
	protected int num_blank_rows = 0;
	protected int rowCount = 0;

	protected byte row[] = null;
	protected byte blank_row[] = null;

	protected List<String> record = null;
	protected List<ReadstatValue> primitiveRecord = null;

	public SASXportConverter(String fileName) throws Exception {

		this(new FileInputStream(fileName));

	}

	public SASXportConverter(InputStream in) {
		this.rawin = in;
	}

	protected void init() throws Exception {
		in = new DataInputStream(rawin);
		ctx = new XPTContext();
		PrimitiveUtils.memset(blank_row, (byte) ' ', ctx.row_length);
		readMeta();
		readNextRecord();
	}

	private void xport_read_record(byte[] record) throws IOException {
		read_bytes(record, LINE_LEN);
	}

	private XPTHeader xport_read_header_record() throws IOException {

		XPTHeader header = new XPTHeader();
		byte line[] = createDefaultBuffer();

		xport_read_record(line);

		int offset = 20;
		header.name = IO.readString(line, offset, 8);
		offset += (8 + 20);
		header.num1 = Integer.parseInt(IO.readString(line, offset, 5));
		offset += 5;
		header.num2 = Integer.parseInt(IO.readString(line, offset, 5));
		offset += 5;
		header.num3 = Integer.parseInt(IO.readString(line, offset, 5));
		offset += 5;
		header.num4 = Integer.parseInt(IO.readString(line, offset, 5));
		offset += 5;
		header.num5 = Integer.parseInt(IO.readString(line, offset, 5));
		offset += 5;
		header.num6 = Integer.parseInt(IO.readString(line, offset, 5));
		offset += 5;
		return header;
	}

	private XPTHeader xport_read_library_record() throws InvalidObjectException, IOException {

		XPTHeader xrecord = xport_read_header_record();

		if ("LIBRARY".equalsIgnoreCase(xrecord.name)) {
			ctx.version = 5;
		} else if ("LIBV8".equalsIgnoreCase(xrecord.name)) {
			ctx.version = 8;
		} else {
			throw new InvalidObjectException("Unknows XPT File Version - " + ctx.version);
		}
		return xrecord;
	}

	private void xport_skip_record() throws IOException {
		xport_read_record(DUMMY_BUFFER);
	}

	private void xport_skip_rest_of_record(int pos) throws IOException {
		int len = LINE_LEN - (pos % LINE_LEN);

		if (len == LINE_LEN)
			return;
		read_bytes(DUMMY_BUFFER, len);
	}

	private TimeStamp xport_read_timestamp_record() throws IOException {

		byte[] line = createDefaultBuffer();

		TimeStamp ts = new TimeStamp();
		String month;

		xport_read_record(line);

		int offset = 0;
		ts.tm_mday = Short.parseShort(IO.readString(line, offset, 2));
		offset += 2;
		month = IO.readString(line, offset, 3);
		offset += 3;
		ts.tm_year = Short.parseShort(IO.readString(line, offset, 2));
		offset += 3;
		ts.tm_hour = Short.parseShort(IO.readString(line, offset, 2));
		offset += 3;
		ts.tm_min = Short.parseShort(IO.readString(line, offset, 2));
		offset += 3;
		ts.tm_sec = Short.parseShort(IO.readString(line, offset, 2));
		offset += 3;
		for (short i = 0; i < XPTTypes.XPORT_MONTHS.length; i++) {
			if (XPTTypes.XPORT_MONTHS[i].equalsIgnoreCase(month)) {
				ts.tm_mon = i;
				break;
			}
		}
		if (ts.tm_year < 60) {
			ts.tm_year += 2000;
		} else if (ts.tm_year < 100) {
			ts.tm_year += 1900;
		}
		ctx.timestamp = createTS(ts);
		return ts;
	}

	private byte[] createDefaultBuffer() {
		return createBuffer(LINE_LEN);
	}

	private byte[] createBuffer(int len) {
		byte line[] = new byte[len];
		return line;
	}

	private long createTS(TimeStamp ts) {

		Calendar cal = Calendar.getInstance();
		cal.set(ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour, ts.tm_min, ts.tm_sec);
		return cal.getTimeInMillis();
	}

	private XPTHeader xport_expect_header_record(String v5_name, String v8_name)
			throws IOException, InvalidObjectException {

		XPTHeader xrecord = xport_read_header_record();

		if (ctx.version == 5 && !v5_name.equalsIgnoreCase(xrecord.name)) {
			throw new InvalidObjectException("Wrong XPT Header Record - " + xrecord.name);
		} else if (ctx.version == 8 && !v8_name.equalsIgnoreCase(xrecord.name)) {
			throw new InvalidObjectException("Wrong XPT Header Record - " + xrecord.name);
		}

		return xrecord;
	}

	private void xport_read_table_name_record() throws Exception {

		byte[] line = createDefaultBuffer();
		xport_read_record(line);

		byte[] dst = createBuffer(129);
		int src_len = ctx.version == 5 ? 8 : 32;
		ctx.table_name = IO.readString(dst, 8, src_len);
	}

	private void xport_read_file_label_record() throws Exception {

		byte[] line = createDefaultBuffer();
		xport_read_record(line);

		byte[] dst = createBuffer(161);
		int src_len = 40;
		ctx.file_label = IO.readString(dst, 32, src_len);
	}

	private void xport_read_namestr_header_record() throws Exception {

		XPTHeader xrecord = xport_read_header_record();

		if (ctx.version == 5 && !"NAMESTR".equalsIgnoreCase(xrecord.name)) {
			throw new InvalidObjectException("Wrong XPT Header Record - " + xrecord.name);
		} else if (ctx.version == 8 && !"NAMSTV8".equalsIgnoreCase(xrecord.name)) {
			throw new InvalidObjectException("Wrong XPT Header Record - " + xrecord.name);
		}

		ctx.var_count = xrecord.num2;
		ctx.variables = new ReadStatVariable[ctx.var_count];
	}

	private List<XPTNameString> xport_read_variables() throws Exception {

		List<XPTNameString> nstr = new ArrayList<XPTTypes.XPTNameString>();
		int read = 0;
		for (int i = 0; i < ctx.var_count; i++) {

			byte buffer[] = new byte[NAMESTR_LEN];
			int bytes = read_bytes(buffer, NAMESTR_LEN);
			read += bytes;
			XPTNameString namestr = xport_namestr_bswap(buffer);

			ReadStatVariable variable = new ReadStatVariable();

			variable.index = i;
			variable.type = namestr.ntype == SAS_COLUMN_TYPE_CHR ? ReadstatType.READSTAT_TYPE_STRING
					: ReadstatType.READSTAT_TYPE_DOUBLE;
			variable.storage_width = namestr.nlng;
			variable.display_width = namestr.nfl;
			variable.decimals = namestr.nfd;
			variable.alignment = namestr.nfj > 0 ? ReadstatAlignment.READSTAT_ALIGNMENT_RIGHT
					: ReadstatAlignment.READSTAT_ALIGNMENT_LEFT;

			variable.name = namestr.nname;
			variable.label = namestr.nlabel;
			variable.format = xport_construct_format(namestr.nform, variable.display_width, variable.decimals);
			// todo format;
			ctx.variables[i] = variable;

			nstr.add(namestr);
		}

		xport_skip_rest_of_record(read);

		if (ctx.version == 5) {
			xport_read_obs_header_record();
		} else {
			XPTHeader xrecord = xport_read_header_record();
			if ("OBSV8".equalsIgnoreCase(xrecord.name)) {
				/* void */
			} else if ("LABELV8".equalsIgnoreCase(xrecord.name)) {
				xport_read_labels_v8(xrecord.num1);
			} else if ("LABELV9".equalsIgnoreCase(xrecord.name)) {
				xport_read_labels_v9(xrecord.num1);
			}
		}

		ctx.row_length = 0;

		int index_after_skipping = 0;

		for (int i = 0; i < ctx.var_count; i++) {
			ReadStatVariable variable = ctx.variables[i];
			variable.index_after_skipping = index_after_skipping;
			// todo deleted code for index after skipping
			ctx.row_length += variable.storage_width;
		}
		return nstr;
	}

	private void xport_read_labels_v8(int label_count) throws Exception {

		byte labeldef[] = new byte[6];
		int read = 0;
		for (int i = 0; i < label_count; i++) {
			int index, name_len, label_len;
			int bytes = read_bytes(labeldef, 6);
			read += bytes;
			ByteBuffer bb = ByteBuffer.wrap(labeldef).order(ByteOrder.BIG_ENDIAN);
			index = bb.getShort();
			name_len = bb.getShort();
			label_len = bb.getShort();

			if (index >= ctx.var_count) {
				throw new InvalidObjectException("Invalid index");
			}

			byte name[] = new byte[name_len];
			byte label[] = new byte[label_len];
			ReadStatVariable variable = ctx.variables[index];

			read_bytes(name, name_len);
			read_bytes(label, label_len);
			variable.name = IO.readString(name, 0, name_len);
			variable.label = IO.readString(label, 0, label_len);
		}
		xport_skip_rest_of_record(read);
		xport_read_obs_header_record();
	}

	private void xport_read_labels_v9(int label_count) throws Exception {

		byte labeldef[] = new byte[10];
		int read = 0;
		for (int i = 0; i < label_count; i++) {
			int index, name_len, format_len, informat_len, label_len;
			int bytes = read_bytes(labeldef, 10);
			read += bytes;
			ByteBuffer bb = ByteBuffer.wrap(labeldef).order(ByteOrder.BIG_ENDIAN);
			index = bb.getShort();
			name_len = bb.getShort();
			format_len = bb.getShort();
			informat_len = bb.getShort();
			label_len = bb.getShort();

			if (index >= ctx.var_count) {
				throw new InvalidObjectException("Invalid index");
			}

			byte name[] = new byte[name_len];
			byte format[] = new byte[format_len];
			byte informat[] = new byte[informat_len];
			byte label[] = new byte[label_len];

			ReadStatVariable variable = ctx.variables[index];

			read_bytes(name, name_len);
			read_bytes(format, format_len);
			read_bytes(informat, informat_len);
			read_bytes(label, label_len);

			variable.name = IO.readString(name, 0, name_len);
			variable.label = IO.readString(label, 0, label_len);
			variable.format = IO.readString(label, 0, label_len);
			variable.format = xport_construct_format(variable.format, variable.display_width, variable.decimals);
		}
		xport_skip_rest_of_record(read);
		xport_read_obs_header_record();
	}

	private XPTHeader xport_read_obs_header_record() throws Exception {
		return xport_expect_header_record("OBS", "OBSV8");
	}

	private XPTNameString xport_namestr_bswap(byte[] buffer) {

		XPTNameString namestr = new XPTNameString();

		int offset = 0;
		ByteBuffer bb = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN);

		namestr.ntype = bb.getShort();
		offset += 2;
		namestr.nhfun = bb.getShort();
		offset += 2;
		namestr.nlng = bb.getShort();
		offset += 2;
		namestr.nvar0 = bb.getShort();
		offset += 2;

		namestr.nname = IO.readString(buffer, offset, 8);
		offset += 8;
		namestr.nlabel = IO.readString(buffer, offset, 40);
		offset += 40;
		namestr.nform = IO.readString(buffer, offset, 8);
		offset += 8;

		bb.position(offset);
		namestr.nfl = bb.getShort();
		offset += 2;
		namestr.nfd = bb.getShort();
		offset += 2;
		namestr.nfj = bb.getShort();
		offset += 2;

		namestr.nfill = IO.readString(buffer, offset, 2);
		offset += 2;
		namestr.niform = IO.readString(buffer, offset, 8);
		offset += 8;

		bb.position(offset);
		namestr.nifl = bb.getShort();
		offset += 2;
		namestr.nifd = bb.getShort();
		offset += 2;
		namestr.npos = bb.getInt();
		offset += 4;

		namestr.longname = IO.readString(buffer, offset, 32);
		offset += 32;

		bb.position(offset);
		namestr.labeln = bb.getShort();
		offset += 2;

		namestr.rest = IO.readString(buffer, offset, 18);
		offset += 18;
		return namestr;
	}

	private String xport_construct_format(String format, int width, int decimals) {

		if (decimals > 0) {
			return String.format("%s%d.%d", format, width, decimals);
		} else if (width > 0) {
			return String.format("%s%d", format, width);
		} else {
			return String.format("%s", format);
		}
	}

	protected void readstat_convert(byte[] dst, int dst_len, byte[] src, int src_off, int src_len) throws Exception {

		while (src_len > 0 && src[src_off + src_len - 1] == ' ') {
			src_len--;
		}

		if (dst_len == 0) {
			throw new Exception("Destination lenght is 0.");
		} else if (src_len + 1 > dst_len) {
			throw new Exception("Source Lenght is greater than Destination lenght.");
		} else {
			for (int i = 0; i < src_len; i++) {
				dst[i] = src[src_off + i];
			}
		}
	}

	private boolean isBlankRow(byte[] row) {
		boolean row_is_blank = true;
		for (int pos = 0; pos < ctx.row_length; pos++) {
			if (row[pos] != ' ') {
				row_is_blank = false;
				break;
			}
		}
		return row_is_blank;
	}

	private boolean sas_validate_tag(byte tag) {
		if (tag == '_' || (tag >= 'A' && tag <= 'Z'))
			return true;

		return false;
	}

	private int read_bytes(byte[] buffer, int len) throws IOException {
		int off = len;
		try {
			in.readFully(buffer, 0, len);
		} catch (Exception e) {
			System.out.println("!!WARN!! Reached EOF before read_fully, Offset: " + offset);
			return -1;
		}
		offset += off;
		return off;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public boolean isDone() {
		return done;
	}

	public XPTContext getMetaData() {
		return ctx;
	}

	public long getOffset() {
		return offset;
	}

	public byte[] getRow() {
		return row;
	}

	public int getRowCount() {
		return rowCount;
	}

	public List<String> getRecord() {
		return record;
	}

	public List<ReadstatValue> getPrimitiveRecord() {
		return primitiveRecord;
	}

	public void close() throws IOException {
		done = true;
		if (in == null)
			return;

		in.close();
		in = null;
	}

	protected void seek(int offset) throws IOException {
		int len = 0;
		do {
			len = Math.min(ctx.row_length, offset);
			int read = read_bytes(DUMMY_BUFFER, len);
			if (read <= 0) {
				done = true;
				close();
				break;
			}
			offset -= read;
		} while (offset > 0);
	}

	protected void readNextRecord() throws Exception {

		if (done)
			return;

		while (true) {
			rowCount++;
			int bytes_read = read_bytes(row, ctx.row_length);
			if (bytes_read < ctx.row_length) {
				done = true;
				break;
			}
			if (isBlankRow(row)) {
				num_blank_rows++;
				continue;
			} else {
				break;
			}
		}
		if (done) {
			close();
			return;
		}
		if (processBlankRecords) {
			while (num_blank_rows > 0) {
				processRecord(blank_row, ctx.row_length);
				if (++(ctx.parsed_row_count) == ctx.row_limit) {
					done = true;
					throw new RuntimeException("Invalid read situation.");
				}
				num_blank_rows--;
			}
		}

		processRecord(row, ctx.row_length);

		if (++(ctx.parsed_row_count) == ctx.row_limit) {
			done = true;
		}
	}

	protected void processRecord(byte[] row, int row_length) {

		int pos = 0;
		String string = null;
		record = new ArrayList<String>();
		primitiveRecord = new ArrayList<ReadstatValue>();

		for (int i = 0; i < ctx.var_count; i++) {
			ReadStatVariable variable = ctx.variables[i];
			ReadstatValue value = new ReadstatValue();
			value.type = variable.type;

			if (variable.type == ReadstatType.READSTAT_TYPE_STRING) {
				string = IO.readString(row, pos, variable.storage_width);
				if (debug)
					System.out.print(" < " + string + " >, ");
				value.tvalue = string;
				record.add(string);
			} else {
				double dval = 0.0d;
				if (variable.storage_width <= XPTTypes.XPORT_MAX_DOUBLE_SIZE
						&& variable.storage_width >= XPTTypes.XPORT_MIN_DOUBLE_SIZE) {
					byte full_value[] = new byte[8];
					if (PrimitiveUtils.memcmp(full_value, 1, row, pos + 1, variable.storage_width - 1)
							&& (row[pos] == '.' || sas_validate_tag(row[pos]))) {
						if (row[pos] == '.') {
							value.is_system_missing = 1;
						} else {
							value.tag = row[pos];
							value.is_tagged_missing = 1;
						}
					} else {
						PrimitiveUtils.memcpy(full_value, 0, row, pos, variable.storage_width);
						dval = PrimitiveUtils.xpt2ieeeSimple(full_value);
					}
				}
				value.value = dval;
				String val = "" + dval;
				if (convertDate9ToString && dval != 0 && variable.format.toLowerCase().contains("date")) {
					val = XPTReaderUtils.convertSASDate9ToString(variable.format.toLowerCase(), dval);
				}
				record.add(val);
				if (debug)
					System.out.print(value.value + ", ");
			}
			primitiveRecord.add(value);
			pos += variable.storage_width;
		}
		if (debug)
			System.out.println();
	}

	public void readMeta() throws Exception {

		XPTHeader header = xport_read_library_record();
		System.out.println(new Gson().toJson(header));

		xport_skip_record();

		TimeStamp ts = xport_read_timestamp_record();
		System.out.println(new Gson().toJson(ts));

		XPTHeader memberHeader = xport_expect_header_record("MEMBER", "MEMBV8");
		System.out.println(new Gson().toJson(memberHeader));

		XPTHeader descHeader = xport_expect_header_record("DSCRPTR", "DSCPTV8");
		System.out.println(new Gson().toJson(descHeader));

		xport_read_table_name_record();

		xport_read_file_label_record();

		xport_read_namestr_header_record();

		List<XPTNameString> nstrs = xport_read_variables();
		System.out.println(new Gson().toJson(nstrs));

		System.out.println(new Gson().toJson(ctx));

		if (ctx.row_length == 0) {
			done = true;
			close();
		} else {
			row = new byte[ctx.row_length];
			blank_row = new byte[ctx.row_length];
		}
	}

	public static void main(String[] args) {
		try {
			SASXportConverter converter = new SASXportConverter("/Users/ravi1/Downloads/test.sasxpt");
			converter.init();
			converter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}