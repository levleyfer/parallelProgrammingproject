import threading
import time
import random

class HybridDataStructure:
    def __init__(self, num_shards=4):
        self.num_shards = num_shards
        self.local_locks = [threading.Lock() for _ in range(num_shards)]
        self.local_data = [{} for _ in range(num_shards)]
        self.global_lock = threading.Lock()
        self.global_counter = 0
        self.local_reads = 0
        self.local_writes = 0
    
    def _get_shard_index(self, key):
        return hash(key) % self.num_shards
    
    def local_write(self, key, value):
        shard_index = self._get_shard_index(key)
        with self.local_locks[shard_index]:
            if key in self.local_data[shard_index]:
                self.local_data[shard_index][key] += value
            else:
                self.local_data[shard_index][key] = value
            self.local_writes += 1
    
    def local_read(self, key):
        shard_index = self._get_shard_index(key)
        with self.local_locks[shard_index]:
            self.local_reads += 1
            return self.local_data[shard_index].get(key, None)
    
    def critical_update(self, increment=1):
        with self.global_lock:
            self.global_counter += increment
    
    def hybrid_operation(self, key, value):
        self.local_write(key, value)
        self.critical_update(increment=value)
    
    def print_stats(self):
        print(f"Total Reads: {self.local_reads}, Total Writes: {self.local_writes}")

def demo_local_operations(hybrid_obj, thread_id):
    for _ in range(5):
        key = f"user_{random.randint(1, 3)}"
        if random.random() < 0.5:
            val = random.randint(100, 999)
            hybrid_obj.local_write(key, val)
        else:
            hybrid_obj.local_read(key)
        time.sleep(random.random() * 0.1)

def demo_critical_operation(hybrid_obj, thread_id):
    for _ in range(5):
        hybrid_obj.critical_update(increment=1)
        time.sleep(random.random() * 0.1)

def demo_hybrid_operation(hybrid_obj, thread_id):
    for _ in range(5):
        key = f"resource_{thread_id}"
        val = random.randint(1000, 2000)
        hybrid_obj.hybrid_operation(key, val)
        time.sleep(random.random() * 0.1)

def run_test(num_threads, hybrid_obj):
    threads = []
    
    for i in range(num_threads // 3):
        t = threading.Thread(target=demo_local_operations, args=(hybrid_obj, i))
        threads.append(t)
    
    for i in range(num_threads // 3):
        t = threading.Thread(target=demo_critical_operation, args=(hybrid_obj, i))
        threads.append(t)
    
    for i in range(num_threads // 3):
        t = threading.Thread(target=demo_hybrid_operation, args=(hybrid_obj, i))
        threads.append(t)
    
    while len(threads) < num_threads:
        t = threading.Thread(target=demo_hybrid_operation, args=(hybrid_obj, len(threads)))
        threads.append(t)
    
    start_time = time.time()
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()
    end_time = time.time()
    
    total_local_sum = sum(sum(shard.values()) for shard in hybrid_obj.local_data)
    difference = hybrid_obj.global_counter - total_local_sum
    
    print(f"Test with {num_threads} threads completed in {end_time - start_time:.4f} seconds")
    print(f"Final Global Counter = {hybrid_obj.global_counter}")
    print(f"Total Local Sum = {total_local_sum}")
    print(f"Difference = {difference}")
    print("Final Local Data:")
    for index, shard in enumerate(hybrid_obj.local_data):
        print(f"Shard {index}: {shard}")
    hybrid_obj.print_stats()
    print("-" * 40)

if __name__ == "__main__":
    thread_counts = [1, 2, 4, 8, 16, 32, 64]
    for count in thread_counts:
        hybrid_ds = HybridDataStructure(num_shards=4)
        run_test(count, hybrid_ds)
