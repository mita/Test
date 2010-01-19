import com.danga.MemCached.*;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class MemCachedBenchmark extends Thread {
	public int id;
	public long time_ms;

	public MemCachedBenchmark(int i) {
		super();
		id = i;
	}

	public void runBenchmark() throws Exception {
		byte[] value;
		MemCachedClient mc;

		value = new byte[MemCachedBenchmark.valueLength];
		Arrays.fill(value, (byte)'*');

		mc = new MemCachedClient();
		for (long i = 0; i < MemCachedBenchmark.numRequests; i++) {
			String key = String.format("%04d-%011d", id, i);

			if (MemCachedBenchmark.command == 'w') {
				mc.set(key, value);
			} else {
				mc.get(key);
			}
		}
	}

	public void run() {
		long start = System.currentTimeMillis();
		try {
			runBenchmark();
		} catch (Exception e) {
			System.err.println("benchmark failed: " + e);
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();

		time_ms = end - start;
	}

	public static void benchmark() throws Exception {
		MemCachedBenchmark[] threads;
		SockIOPool pool = SockIOPool.getInstance();
		long sum = 0;
		long min_ms = Long.MAX_VALUE;
		long max_ms = 0;
		long avg_ms;

		pool.setServers(MemCachedBenchmark.servers);
		if (MemCachedBenchmark.tcp_nodelay) {
			pool.setNagle(false);
		} else {
			pool.setNagle(true);
		}
		pool.initialize();

		if (!MemCachedBenchmark.verbose) {
			Logger logger = Logger.getLogger(MemCachedClient.class.getName());
			logger.setLevel(Logger.LEVEL_WARN);
		}

		threads = new MemCachedBenchmark[numThreads];
		for (int i = 0; i < numThreads; i++) {
			threads[i] = new MemCachedBenchmark(i);
		}
		for (int i = 0; i < numThreads; i++) {
			threads[i].start();
		}
		for (int i = 0; i < numThreads; i++) {
			threads[i].join();
		}
		for (int i = 0; i < numThreads; i++) {
			long ms = threads[i].time_ms;

			sum += ms;
			min_ms = Math.min(min_ms, ms);
			max_ms = Math.max(max_ms, ms);
		}
		avg_ms = sum / numThreads;

		System.out.println(numThreads +" " + avg_ms / 1000.0 + " " +
				min_ms / 1000.0 + " " + max_ms / 1000.0);
	}

	private static int valueLength = 100;
	private static long numRequests = 100000L;
	/* 'r' for Read, 'w' for Write request */
	private static char command = 'w';
	private static int numThreads = 8;
	private static boolean verbose = false;
	private static String[] servers = { "127.0.0.1:21201" };
	private static boolean tcp_nodelay = false;

	public static void parseOptions(String args[]) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-n")) {
				numRequests = Long.parseLong(args[i + 1]);
				i++;
			} else if (args[i].equals("-l")) {
				valueLength = Integer.parseInt(args[i + 1]);
				i++;
			} else if (args[i].equals("-s")) {
				servers[0] = args[i + 1];
				i++;
			} else if (args[i].equals("-t")) {
				numThreads = Integer.parseInt(args[i + 1]);
				i++;
			} else if (args[i].equals("-r")) {
				command = 'r';
			} else if (args[i].equals("-w")) {
				command = 'w';
			} else if (args[i].equals("-v")) {
				verbose = true;
			} else if (args[i].equals("-d")) {
				tcp_nodelay = true;
			} else {
				System.err.println("invalid option");
			}
		}
	}

	public static void main(String args[]) throws Exception {
		parseOptions(args);
		benchmark();
	}
}
