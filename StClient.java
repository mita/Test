//package chunkc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import sun.misc.BASE64Encoder;

class ChunkFlags {
	public static final byte CHF_NONE = 0;
	public static final byte CHF_SYNC = 1 << 0;
	public static final byte CHF_TBL_CREAT = 1 << 1;
	public static final byte CHF_TBL_EXCL = 1 << 2;
}

class ChunkErrCode {
	public static final byte Success = 0;
	public static final byte AccessDenied = 1;
	public static final byte InternalError = 2;
	public static final byte InvalidArgument = 3;
	public static final byte InvalidURI = 4;
	public static final byte NoSuchKey = 5;
	public static final byte SignatureDoesNotMatch = 6;
	public static final byte InvalidKey = 7;
	public static final byte InvalidTable = 8;
	public static final byte Busy = 9;
}

class ChunkServerResponse {
	static final int CHD_MAGIC_SZ = 8;
	static final int CHD_CSUM_SZ = 64;

	byte magic[] = new byte[CHD_MAGIC_SZ];
	public byte resp_code;
	byte rsv1[] = new byte[3];
	int nonce;
	public long data_len;
	byte checksum[] = new byte[CHD_CSUM_SZ];

	public ChunkServerResponse(ByteBuffer buffer) {
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		buffer.get(magic);
		// check magic

		resp_code = buffer.get();
		buffer.get(rsv1);
		nonce = buffer.getInt();
		data_len = buffer.getLong();
		buffer.get(checksum);
	}

	public static final int length = 88;
}

class ChunkServerRequest {
	static final int CHD_SIG_SZ = 64;

	private byte magic[] = { 'C', 'H', 'U', 'N', 'K', 'D', 'v', '1' };
	public byte op;
	public byte flags;
	private short key_len;
	private int nonce;
	public long data_len;
	private byte sig[] = new byte[CHD_SIG_SZ];
	private byte key[] = null;

	static final byte CHO_NOP = 0;
	static final byte CHO_GET = 1;
	static final byte CHO_PUT = 3;
	static final byte CHO_DEL = 4;
	static final byte CHO_LIST = 5;
	static final byte CHO_LOGIN = 6;
	static final byte CHO_TABLE_OPEN = 7;

	public ChunkServerRequest(byte op) {
		nonce = new Random().nextInt();
		this.op = op;
	}

	public ChunkServerRequest(byte op, byte[] key) {
		this(op);
		setKey(key);
	}

	public void setKey(byte[] key) {
		this.key = new byte[key.length];
		// check key length
		this.key_len = (short)key.length;

		System.arraycopy(key, 0, this.key, 0, key.length);
	}

	public void sign(byte[] keyBytes) throws Exception {
		SecretKeySpec key = new SecretKeySpec(keyBytes, "HmacSHA1");
		Mac mac = Mac.getInstance("HmacSHA1");

		mac.init(key);
		byte[] signBytes = mac.doFinal(encode());

		signBytes = new BASE64Encoder().encode(signBytes).getBytes("UTF-8");

		System.arraycopy(signBytes, 0, sig, 0, signBytes.length);
		sig[signBytes.length] = 0;
	}

	public int length() {
		if (key != null)
			return key.length + 88;

		return 88;
	}

	public byte[] encode() {
		ByteBuffer buffer = ByteBuffer.allocate(length());

		buffer.order(ByteOrder.LITTLE_ENDIAN);
		buffer.put(magic);
		buffer.put(op);
		buffer.put(flags);
		buffer.putShort(key_len);
		buffer.putInt(nonce);
		buffer.putLong(data_len);
		buffer.put(sig);

		if (key != null)
			buffer.put(key);

		return buffer.array();
	}
}

class StObject {
	public byte[] name;
	public String time_mod;
	public String etag;
	public long size;
	public String owner;

	public StObject() {
	}

	public String toString() {
		return new String(name);
	}
};

class StKeylist {
	public String name;
	public List<StObject> contents;

	public StKeylist(String name, List<StObject> contents) {
		this.name = name;
		this.contents = contents;
	}
}

public class StClient {
	private SocketChannel channel = null;
	private byte[] user;
	private byte[] key;

	public StClient(String host, int port, String user, String secretKey,
			boolean encrypt) throws Exception {
		byte[] userBytes = user.getBytes("UTF-8");

		this.user = new byte[userBytes.length + 1];
		System.arraycopy(userBytes, 0, this.user, 0, userBytes.length);
		this.user[userBytes.length] = 0;

		this.key = secretKey.getBytes("UTF-8");

		InetSocketAddress address = new InetSocketAddress(host, port);

		channel = SocketChannel.open(address);

		login();
	}

	public void free() throws IOException {
		if (channel != null)
			channel.close();
	}

	private void signRequest(ChunkServerRequest req) throws Exception {
		req.sign(key);
	}

	private void writeRequest(ChunkServerRequest req) throws Exception {
		channel.write(ByteBuffer.wrap(req.encode()));
	}

	private ChunkServerResponse readResponse() throws Exception {
		ByteBuffer buffer =
			ByteBuffer.allocate(ChunkServerResponse.length);

		channel.read(buffer);
		buffer.flip();

		return new ChunkServerResponse(buffer);
	}

	private boolean login() throws Exception {
		ChunkServerRequest req =
		new ChunkServerRequest(ChunkServerRequest.CHO_LOGIN, user);

		signRequest(req);
		writeRequest(req);
		ChunkServerResponse resp = readResponse();

		if (resp.resp_code != ChunkErrCode.Success)
			return false;

		return true;
	}

	public boolean tableOpen(byte[] key, byte flags) throws Exception {
		ChunkServerRequest req =
		new ChunkServerRequest(ChunkServerRequest.CHO_TABLE_OPEN, key);

		req.flags = (byte)(flags & (ChunkFlags.CHF_TBL_CREAT | ChunkFlags.CHF_TBL_EXCL));

		signRequest(req);
		writeRequest(req);
		ChunkServerResponse resp = readResponse();

		if (resp.resp_code != ChunkErrCode.Success)
			return false;

		return true;
	}

	public boolean get(byte[] key, WritableByteChannel writable)
			throws Exception {
		ChunkServerRequest req =
		new ChunkServerRequest(ChunkServerRequest.CHO_GET, key);

		signRequest(req);
		writeRequest(req);
		ChunkServerResponse resp = readResponse();

		if (resp.resp_code != ChunkErrCode.Success)
			return false;

		ByteBuffer mtime = ByteBuffer.allocate(8);
		channel.read(mtime);

		long len = resp.data_len;
		ByteBuffer buffer = ByteBuffer.allocate(4096);

		while (len > 0) {
			channel.read(buffer);
			buffer.flip();
			writable.write(buffer);
			len -= buffer.position();
			buffer.clear();
		}

		return true;
	}

	class InlineWritable implements WritableByteChannel {
		private ByteArrayOutputStream out;

		public InlineWritable() {
			out = new ByteArrayOutputStream();
		}

		public int write(ByteBuffer dst) throws IOException {
			int pos = dst.position();

			out.write(dst.array(), pos, dst.remaining());
			dst.position(dst.limit());

			return dst.position() - pos;
		}

		public void close() throws IOException {
			out.close();
		}

		public boolean isOpen() {
			return true;
		}

		public byte[] toByteArray() {
			return out.toByteArray();
		}
	}

	public byte[] getInline(byte[] key) throws Exception {
		InlineWritable buffer = new InlineWritable();

		if (get(key, buffer))
			return buffer.toByteArray();

		return null;
	}

	public boolean del(byte[] key) throws Exception {
		ChunkServerRequest req =
			new ChunkServerRequest(ChunkServerRequest.CHO_DEL, key);

		signRequest(req);
		writeRequest(req);
		ChunkServerResponse resp = readResponse();

		if (resp.resp_code != ChunkErrCode.Success)
			return false;

		return true;
	}

	public boolean ping() throws Exception {
		ChunkServerRequest req =
			new ChunkServerRequest(ChunkServerRequest.CHO_NOP);

		signRequest(req);
		writeRequest(req);
		ChunkServerResponse resp = readResponse();

		if (resp.resp_code != ChunkErrCode.Success)
			return false;

		return true;
	}

	private String getFirstChildValue(Element child) {
		return child.getFirstChild().getNodeValue();
	}

	private long parseContentsSize(Element size) {
		return Long.parseLong(size.getFirstChild().getNodeValue());
	}

	private byte[] parseContentsName(Element name) throws Exception {
		return name.getFirstChild().getNodeValue().getBytes("UTF-8");
	}

	private StObject parseContents(Element contents) throws Exception {
		NodeList children = contents.getChildNodes();
		StObject obj = new StObject();

		for (int i = 0; i < children.getLength(); i++) {
			if (!(children.item(i) instanceof Element))
				continue;
			Element child = (Element)children.item(i);

			if (child.getTagName().equals("Name"))
				obj.name = parseContentsName(child);
			else if (child.getTagName().equals("LastModified"))
				obj.time_mod = getFirstChildValue(child);
			else if (child.getTagName().equals("ETag"))
				obj.etag = getFirstChildValue(child);
			else if (child.getTagName().equals("Size"))
				obj.size = parseContentsSize(child);
			else if (child.getTagName().equals("Owner"))
				obj.owner = getFirstChildValue(child);
		}
		return obj;
	}

	private StKeylist parseListVolumeResult(Element listVolumeResult)
			throws Exception {
		NodeList children = listVolumeResult.getChildNodes();
		String name = null;
		List<StObject> contents = new ArrayList<StObject>();

		for (int i = 0; i < children.getLength(); i++) {
			if (!(children.item(i) instanceof Element))
				continue;
			Element child = (Element)children.item(i);

			if (child.getTagName().equals("Name"))
				name = getFirstChildValue(child);
			else if (child.getTagName().equals("Contents"))
				contents.add(parseContents(child));
		}
		return new StKeylist(name, contents);
	}

	private StKeylist parseXML(InputStream in) throws Exception {
		DocumentBuilderFactory factory =
			DocumentBuilderFactory.newInstance();

		factory.setIgnoringComments(true);

		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(in);
		Element root = doc.getDocumentElement();

		if (root.getTagName().equals("ListVolumeResult"))
			return parseListVolumeResult(root);

		return null;
	}

	public StKeylist keys() throws Exception {
		ChunkServerRequest req =
			new ChunkServerRequest(ChunkServerRequest.CHO_LIST);

		signRequest(req);
		writeRequest(req);
		ChunkServerResponse resp = readResponse();

		if (resp.resp_code != ChunkErrCode.Success)
			return null;

		long len = resp.data_len;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteBuffer buffer = ByteBuffer.allocate(4096);

		while (len > 0) {
			channel.read(buffer);
			buffer.flip();
			out.write(buffer.array(), buffer.position(),
					buffer.remaining());
			len -= buffer.remaining();
			buffer.clear();
		}

		return parseXML(new ByteArrayInputStream(out.toByteArray()));
	}

	public boolean put(byte[] key, ReadableByteChannel readable,
			long len, byte flags) throws Exception {
		ChunkServerRequest req =
		new ChunkServerRequest(ChunkServerRequest.CHO_PUT, key);

		req.flags = (byte)(flags & ChunkFlags.CHF_SYNC);
		req.data_len = len;

		signRequest(req);
		writeRequest(req);

		ByteBuffer buffer = ByteBuffer.allocate(4096);
		while (len > 0) {
			readable.read(buffer);
			buffer.flip();
			channel.write(buffer);
			len -= buffer.position();
			buffer.clear();
		}

		ChunkServerResponse resp = readResponse();

		if (resp.resp_code != ChunkErrCode.Success)
			return false;

		return true;
	}

	class InlineReadable implements ReadableByteChannel {
		ByteBuffer buffer;

		public InlineReadable(byte[] bytes) {
			buffer = ByteBuffer.wrap(bytes);
			buffer.position(0);
		}

		public int read(ByteBuffer dst) throws IOException {
			int pos = buffer.position();

			dst.put(buffer);

			return buffer.position() - pos;
		}

		public void close() throws IOException {
			buffer.reset();
		}

		public boolean isOpen() {
			return true;
		}
	}

	public boolean putInline(byte[] key, byte[] data, byte flags)
			throws Exception {
		ReadableByteChannel readable = new InlineReadable(data);

		return put(key, readable, (long)data.length, flags);
	}

	public static void main(String args[]) throws Exception {
		StClient client =
			new StClient("localhost", 21200, "USR", "USR", false);
		byte[] tableBytes = "table".getBytes("UTF-8");
		byte[] key = "key".getBytes("UTF-8");
		byte[] value = "value".getBytes("UTF-8");

		client.tableOpen(tableBytes, ChunkFlags.CHF_TBL_CREAT);
		client.ping();
		client.del(key);
		client.putInline(key, value, ChunkFlags.CHF_SYNC);

		StKeylist keylist = client.keys();
		for (StObject obj: keylist.contents) {
			System.out.println(new String(obj.name));
			System.out.println(new String(client.getInline(key)));
		}
	}
}
