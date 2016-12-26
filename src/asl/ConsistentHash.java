package asl;

import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class implements consistent hashing for the request keys for the purpose
 * of uniformly distributing the requests across the servers. The usual design
 * with the circle is used.
 */
public class ConsistentHash {

	private final HashFunction hashFunction;
	private final int numberOfServers;
	private final SortedMap<Long, Integer> circle = new TreeMap<Long, Integer>();

	public ConsistentHash(HashFunction hashFunction, int numberOfServers) {
		this.hashFunction = hashFunction;
		this.numberOfServers = numberOfServers;

	}

	/**
	 * Add server points to the circle at equally spaced intervals.
	 */
	public void addServers() {
		long space = (long) (Math.pow(2, 32) / numberOfServers);

		for (int i = 0; i < numberOfServers; i++) {
			circle.put(i * space, i);
		}
	}

	/**
	 * Hashes the given key and returns the index of the server following that
	 * hash in the circle.
	 */
	public int get(byte[] key) {
		if (circle.isEmpty()) {
			return -1;
		}

		long hash = 0;
		try {
			hash = hashFunction.hash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		if (!circle.containsKey(hash)) {
			SortedMap<Long, Integer> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}

}
