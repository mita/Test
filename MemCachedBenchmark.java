import com.danga.MemCached.*;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class MemcachedBenchmark extends Thread {
	public int id;
	public long time_ms;

	public MemcachedBenchmark(int i) {
		super();
		id = i;
	}

	public void runBenchmark() throws Exception {
		byte[] value;
		MemCachedClient mc;

		value = new byte[MemcachedBenchmark.valueLength];
		Arrays.fill(value, (byte)'*');

		mc = new MemCachedClient();
		for (long i = 0; i < MemcachedBenchmark.numRequests; i++) {
			String key = String.format("%04d-%011d", id, i);

			if (MemcachedBenchmark.command == 'w') {
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
		MemcachedBenchmark[] threads;
		SockIOPool pool = SockIOPool.getInstance();
		long sum = 0;
		long min_ms = Long.MAX_VALUE;
		long max_ms = 0;
		long avg_ms;

		pool.setServers(MemcachedBenchmark.servers);
		//pool.setNagle(false);
		pool.initialize();
		if (MemcachedBenchmark.verbose == 0) {
			Logger logger = Logger.getLogger(MemCachedClient.class.getName());
			logger.setLevel(Logger.LEVEL_WARN);
		}

		threads = new MemcachedBenchmark[numThreads];
		for (int i = 0; i < numThreads; i++) {
			threads[i] = new MemcachedBenchmark(i);
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
	private static int verbose = 0;
	private static String[] servers = { "127.0.0.1:21201" };
	private static int tcp_nodelay = 0;

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
				verbose = 1;
			} else if (args[i].equals("-d")) {
				tcp_nodelay = 1;
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
