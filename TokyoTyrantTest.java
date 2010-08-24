import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.net.InetSocketAddress;

import tokyotyrant.RDB;
import tokyotyrant.MRDB;
import tokyotyrant.networking.NodeAddress;
import tokyotyrant.transcoder.ByteArrayTranscoder;

interface KeyGenerator {
	byte[] nextKey() throws Exception;
}

class SequentialKeyGenerator implements KeyGenerator {
	long seed;
	long counter;

	public SequentialKeyGenerator(long seed) {
		this.seed = seed;
		this.counter = 0;
	}

	public byte[] nextKey() throws Exception {
		String key = String.format("0x%016x-%016x", seed, counter++);

		return key.getBytes("UTF-8");
	}
}

class RandomKeyGenerator implements KeyGenerator {
	long seed;
	Random random;

	public RandomKeyGenerator(long seed) {
		this.seed = seed;
		random = new Random(seed);
	}

	public byte[] nextKey() throws Exception {
		String key = String.format("0x%016x-%016x",
					seed, random.nextLong());

		return key.getBytes("UTF-8");
	}
}

public class TokyoTyrantTest {

	static {
		/*
		 * To avoid the chicken-and-egg problem
		 * http://www.slf4j.org/codes.html#substituteLogger
		 */
		org.slf4j.Logger logger =
			org.slf4j.LoggerFactory.getLogger(TokyoTyrantTest.class);

		/*
		 * this is to avoid the jvm bug:
		 * NullPointerException in Selector.open()
		 * http://bugs.sun.com/view_bug.do?bug_id=6427854
		 */
		try {
			java.nio.channels.Selector.open().close();
		} catch (java.io.IOException ie) {
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
		KeyGenerator keys = getKeyGenerator(seed);

		for (int i = 0; i < num; i++)
			keys.nextKey();
	}

	static void putTest(String host, int port, int num, int vsiz,
			long seed) throws Exception {
		KeyGenerator keys = getKeyGenerator(seed);
		RDB db = openRDB(host, port);
		byte[] value = new byte[vsiz];

		for (int i = 0; i < num; i++)
			db.put(keys.nextKey(), value);

		db.close();
	}

	static void mputTest(String host, int port, int num, int vsiz,
			long seed) throws Exception {
		KeyGenerator keys = getKeyGenerator(seed);
		MRDB db = openMRDB(host, port);
		byte[] value = new byte[vsiz];
		List<Future<Boolean>> futures =
				new ArrayList<Future<Boolean>>();

		for (int i = 0; i < num; i++) {
			futures.add(db.put(keys.nextKey(), value));
			if (futures.size() >= batch) {
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
		KeyGenerator keys = getKeyGenerator(seed);
		RDB db = openRDB(host, port);
		byte[] value = new byte[vsiz];
		List<byte[]> list = new ArrayList<byte[]>();

		for (int i = 0; i < num; i++) {
			list.add(keys.nextKey());
			list.add(value);

			if (list.size() / 2 >= batch) {
				db.misc("putlist", list, 0);
				list.clear();
			}
		}
		db.misc("putlist", list, 0);

		db.close();
	}

	static final int miscBatch = 8;

	static void mputlistTest(String host, int port, int num, int vsiz,
			long seed) throws Exception {
		KeyGenerator keys = getKeyGenerator(seed);
		MRDB db = openMRDB(host, port);
		byte[] value = new byte[vsiz];
		List<byte[]> list = new ArrayList<byte[]>();
		List<Future<List<byte[]>>> futures =
				new ArrayList<Future<List<byte[]>>>();

		for (int i = 0; i < num; i++) {
			list.add(keys.nextKey());
			list.add(value);

			if (list.size() / 2 >= batch) {
				futures.add(db.misc("putlist", list, 0));
				list = new ArrayList<byte[]>();

				if (futures.size() >= miscBatch) {
					for (Future<List<byte[]>> future: futures)
						future.get();
					futures.clear();
				}
			}
		}
		futures.add(db.misc("putlist", list, 0));

		for (Future<List<byte[]>> future: futures)
			future.get();

		db.close();
	}

	static void outTest(String host, int port, int num, long seed)
			throws Exception {
		KeyGenerator keys = getKeyGenerator(seed);
		RDB db = openRDB(host, port);

		for (int i = 0; i < num; i++)
			db.out(keys.nextKey());

		db.close();
	}

	static void moutTest(String host, int port, int num, long seed)
			throws Exception {
		KeyGenerator keys = getKeyGenerator(seed);
		MRDB db = openMRDB(host, port);
		List<Future<Boolean>> futures =
				new ArrayList<Future<Boolean>>();

		for (int i = 0; i < num; i++) {
			futures.add(db.out(keys.nextKey()));
			if (futures.size() >= batch) {
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
		KeyGenerator keys = getKeyGenerator(seed);
		RDB db = openRDB(host, port);
		List<byte[]> list = new ArrayList<byte[]>();

		for (int i = 0; i < num; i++) {
			list.add(keys.nextKey());

			if (list.size() >= batch) {
				db.misc("outlist", list, 0);
				list.clear();
			}
		}
		db.misc("outlist", list, 0);

		db.close();
	}

	static void moutlistTest(String host, int port, int num, long seed)
			throws Exception {
		KeyGenerator keys = getKeyGenerator(seed);
		MRDB db = openMRDB(host, port);
		List<byte[]> list = new ArrayList<byte[]>();
		List<Future<List<byte[]>>> futures =
				new ArrayList<Future<List<byte[]>>>();

		for (int i = 0; i < num; i++) {
			list.add(keys.nextKey());

			if (list.size() >= batch) {
				futures.add(db.misc("outlist", list, 0));
				list = new ArrayList<byte[]>();

				if (futures.size() >= miscBatch) {
					for (Future<List<byte[]>> future: futures)
						future.get();
					futures.clear();
				}
			}
		}
		futures.add(db.misc("outlist", list, 0));

		for (Future<List<byte[]>> future: futures)
			future.get();

		db.close();
	}

	/*
	 * Benchmark parameters
	 */
	static String command = "";
	static String host = "localhost";
	static int port = 1978;
	static int num = 5000000;
	static int vsiz = 100;
	static long seed = new Random().nextLong();
	static int batch = 1000;

	static String keyGenerator = "sequential";
	static int thnum = 1;

	static KeyGenerator getKeyGenerator(long seed) {
		if (keyGenerator.equals("random"))
			return new RandomKeyGenerator(seed);

		return new SequentialKeyGenerator(seed);
	}

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
				batch = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-key")) {
				keyGenerator = args[++i];
			} else if (args[i].equals("-thnum")) {
				thnum = Integer.parseInt(args[++i]);
			} else {
				System.err.println("Invalid command option");
				System.exit(1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		parseOptions(args);

		Test[] thread = new Test[thnum];

		for (int i = 0; i < thread.length; i++) {
			thread[i] = new Test(command, host, port, num, vsiz,
						seed + i);
		}
		for (int i = 0; i < thread.length; i++)
			thread[i].start();
		for (int i = 0; i < thread.length; i++)
			thread[i].join();

		long max = Long.MIN_VALUE;
		long min = Long.MAX_VALUE;
		long sum = 0;

		for (int i = 0; i < thread.length; i++) {
			max = Math.max(max, thread[i].elapsed);
			min = Math.min(min, thread[i].elapsed);
			sum += thread[i].elapsed;
		}
		long avg = sum / thread.length;

		System.out.printf("## %d.%03d %d.%03d %d.%03d\n",
					avg / 1000, avg % 1000,
					min / 1000, min % 1000,
					max / 1000, max % 1000);
		System.exit(0);
	}
}

class Test extends Thread {
	String command;
	String host;
	int port;
	int num;
	int vsiz;
	long seed;

	public long elapsed;

	public Test(String command, String host, int port, int num, int vsiz,
			long seed) throws Exception {
		this.command = command;
		this.host = host;
		this.port = port;
		this.num = num;
		this.vsiz = vsiz;
		this.seed = seed;
	}

	public void run() {
		long start = System.currentTimeMillis();

		try {
			__run();
		} catch (Exception e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();

		elapsed = end - start;
	}

	void __run() throws Exception {
		if (command.equals("put"))
			TokyoTyrantTest.putTest(host, port, num, vsiz, seed);
		else if (command.equals("mput"))
			TokyoTyrantTest.mputTest(host, port, num, vsiz, seed);
		else if (command.equals("putlist"))
			TokyoTyrantTest.putlistTest(host, port, num, vsiz, seed);
		else if (command.equals("mputlist"))
			TokyoTyrantTest.mputlistTest(host, port, num, vsiz, seed);
		else if (command.equals("out"))
			TokyoTyrantTest.outTest(host, port, num, seed);
		else if (command.equals("mout"))
			TokyoTyrantTest.moutTest(host, port, num, seed);
		else if (command.equals("outlist"))
			TokyoTyrantTest.outlistTest(host, port, num, seed);
		else if (command.equals("moutlist"))
			TokyoTyrantTest.moutlistTest(host, port, num, seed);
		else if (command.equals("null"))
			TokyoTyrantTest.nullTest(num, seed);
		else
			throw new Exception("Invalid command: " + command);
	}
}
