import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.net.InetSocketAddress;

import tokyotyrant.RDB;
import tokyotyrant.MRDB;
import tokyotyrant.networking.NodeAddress;
import tokyotyrant.transcoder.ByteArrayTranscoder;

public class TokyoTyrantTest {

	static class KeyGenerator {
		long seed;
		Random random;

		public KeyGenerator(long seed) {
			this.seed = seed;
			random = new Random(seed);
		}

		public byte[] nextKey() throws Exception {
			String key = String.format("0x%016x-%016x",
						seed, random.nextLong());

			return key.getBytes("UTF-8");
		}
	}

	static RDB openRDB(String host, int port) throws Exception {
		RDB db = new RDB();

		db.open(new InetSocketAddress(host, port));
		db.setKeyTranscoder(new ByteArrayTranscoder());
		db.setValueTranscoder(new ByteArrayTranscoder());

		return db;
	}

	static MRDB openMRDB(String host, int port) throws Exception {
		MRDB db = new MRDB();
		String address = String.format("tcp://%s:%d", host, port);

		db.open(NodeAddress.addresses(address));
		db.setGlobalTimeout(Long.MAX_VALUE);
		db.setKeyTranscoder(new ByteArrayTranscoder());
		db.setValueTranscoder(new ByteArrayTranscoder());

		return db;
	}

	static void nullTest(int num, long seed) throws Exception {
		KeyGenerator keys = new KeyGenerator(seed);

		for (int i = 0; i < num; i++)
			keys.nextKey();
	}

	static void putTest(String host, int port, int num, int vsiz,
			long seed) throws Exception {
		KeyGenerator keys = new KeyGenerator(seed);
		RDB db = openRDB(host, port);
		byte[] value = new byte[vsiz];

		for (int i = 0; i < num; i++)
			db.put(keys.nextKey(), value);

		db.close();
	}

	static void mputTest(String host, int port, int num, int vsiz,
			long seed) throws Exception {
		KeyGenerator keys = new KeyGenerator(seed);
		MRDB db = openMRDB(host, port);
		byte[] value = new byte[vsiz];
		List<Future<Boolean>> futures =
				new ArrayList<Future<Boolean>>();

		for (int i = 0; i < num; i++) {
			futures.add(db.put(keys.nextKey(), value));
			if (futures.size() >= batchCount) {
				for (Future<Boolean> future: futures)
					future.get();

				futures.clear();
			}
		}
		for (Future<Boolean> future: futures)
			future.get();

		db.close();
	}

	static void putlistTest(String host, int port, int num, int vsiz,
			long seed) throws Exception {
		KeyGenerator keys = new KeyGenerator(seed);
		RDB db = openRDB(host, port);
		byte[] value = new byte[vsiz];
		List<byte[]> list = new ArrayList<byte[]>();

		for (int i = 0; i < num; i++) {
			list.add(keys.nextKey());
			list.add(value);

			if (list.size() / 2 >= batchCount) {
				db.misc("putlist", list, 0);
				list.clear();
			}
		}
		db.misc("putlist", list, 0);

		db.close();
	}

	static void outTest(String host, int port, int num, long seed)
			throws Exception {
		KeyGenerator keys = new KeyGenerator(seed);
		RDB db = openRDB(host, port);

		for (int i = 0; i < num; i++)
			db.out(keys.nextKey());

		db.close();
	}

	static void moutTest(String host, int port, int num, long seed)
			throws Exception {
		KeyGenerator keys = new KeyGenerator(seed);
		MRDB db = openMRDB(host, port);
		List<Future<Boolean>> futures =
				new ArrayList<Future<Boolean>>();

		for (int i = 0; i < num; i++) {
			futures.add(db.out(keys.nextKey()));
			if (futures.size() >= batchCount) {
				for (Future<Boolean> future: futures)
					future.get();

				futures.clear();
			}
		}
		for (Future<Boolean> future: futures)
			future.get();

		db.close();
	}

	static void outlistTest(String host, int port, int num, long seed)
			throws Exception {
		KeyGenerator keys = new KeyGenerator(seed);
		RDB db = openRDB(host, port);
		List<byte[]> list = new ArrayList<byte[]>();

		for (int i = 0; i < num; i++) {
			list.add(keys.nextKey());

			if (list.size() >= batchCount) {
				db.misc("outlist", list, 0);
				list.clear();
			}
		}
		db.misc("outlist", list, 0);

		db.close();
	}

	static String command = "";
	static String host = "localhost";
	static int port = 1978;
	static int num = 400000;
	static int vsiz = 100;
	static long seed = new Random().nextLong();
	static int batchCount = 1000;

	static void parseOptions(String[] args) throws Exception {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-command")) {
				command = args[++i];
			} else if (args[i].equals("-host")) {
				host = args[++i];
			} else if (args[i].equals("-port")) {
				port = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-num")) {
				num = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-vsiz")) {
				vsiz = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-seed")) {
				seed = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-batch")) {
				batchCount = Integer.parseInt(args[++i]);
			} else {
				System.err.println("Invalid command option");
				System.exit(1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		parseOptions(args);

		if (command.equals("put"))
			putTest(host, port, num, vsiz, seed);
		else if (command.equals("mput"))
			mputTest(host, port, num, vsiz, seed);
		else if (command.equals("putlist"))
			putlistTest(host, port, num, vsiz, seed);
		else if (command.equals("out"))
			outTest(host, port, num, seed);
		else if (command.equals("mout"))
			moutTest(host, port, num, seed);
		else if (command.equals("outlist"))
			outlistTest(host, port, num, seed);
		else if (command.equals("null"))
			nullTest(num, seed);
		else
			System.err.println("Invalid command: " + command);
	}
}
