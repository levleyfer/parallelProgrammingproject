
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;

class MppRunnerCode {
	private final int numShards;
	private final Lock[] localLocks;
	private final Map<String, Integer>[] localData;
	private final Lock globalLock;
	private final AtomicInteger globalCounter;
	private int localReads = 0;
	private int localWrites = 0;

	public MppRunnerCode(int numShards) {
		this.numShards = numShards;
		this.localLocks = new ReentrantLock[numShards];
		this.localData = new ConcurrentHashMap[numShards];
		for (int i = 0; i < numShards; i++) {
			this.localLocks[i] = new ReentrantLock();
			this.localData[i] = new ConcurrentHashMap<>();
		}
		this.globalLock = new ReentrantLock();
		this.globalCounter = new AtomicInteger(0);
	}

	private int getShardIndex(String key) {
		return Math.abs(key.hashCode()) % numShards;
	}

	public void localWrite(String key, int value) {
		int shardIndex = getShardIndex(key);
		localLocks[shardIndex].lock();
		try {
			localData[shardIndex].merge(key, value, Integer::sum);
			localWrites++;
		} finally {
			localLocks[shardIndex].unlock();
		}
	}

	public Integer localRead(String key) {
		int shardIndex = getShardIndex(key);
		localLocks[shardIndex].lock();
		try {
			localReads++;
			return localData[shardIndex].getOrDefault(key, null);
		} finally {
			localLocks[shardIndex].unlock();
		}
	}

	public void criticalUpdate(int increment) {
		globalLock.lock();
		try {
			globalCounter.addAndGet(increment);
		} finally {
			globalLock.unlock();
		}
	}

	public void hybridOperation(String key, int value) {
		localWrite(key, value);
		criticalUpdate(value);
	}

	public void printStats() {
		System.out.println("Total Reads: " + localReads + ", Total Writes: " + localWrites);
	}

	public int getGlobalCounter() {
		return globalCounter.get();
	}

	public int getTotalLocalSum() {
		return Arrays.stream(localData).mapToInt(shard -> shard.values().stream().mapToInt(Integer::intValue).sum())
				.sum();
	}
}



