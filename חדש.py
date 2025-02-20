import threading
import time
import random
import queue
import matplotlib.pyplot as plt

class HybridDataStructure:
    def __init__(self, num_shards=4):
        self.num_shards = num_shards
        self.local_locks = [threading.Lock() for _ in range(num_shards)]
        self.local_data = [{} for _ in range(num_shards)]
        self.global_lock = threading.Lock()
        self.global_counter = 0
        self.local_reads = 0
        self.local_writes = 0
        self.lock_wait_time = 0  
        self.write_conflicts = 0  

    def _get_shard_index(self, key):
        return hash(key) % self.num_shards
    
    def local_write(self, key, value):
        shard_index = self._get_shard_index(key)
        start_time = time.perf_counter()
        try:
            with self.local_locks[shard_index]:
                lock_time = time.perf_counter() - start_time
                self.lock_wait_time += lock_time  

                if key in self.local_data[shard_index]:
                    self.write_conflicts += 1  

                self.local_data[shard_index][key] = self.local_data[shard_index].get(key, 0) + value
                self.local_writes += 1
        except Exception as e:
            print(f"Error in local_write: {e}")

    def local_read(self, key):
        shard_index = self._get_shard_index(key)
        try:
            with self.local_locks[shard_index]:
                self.local_reads += 1
                return self.local_data[shard_index].get(key, None)
        except Exception as e:
            print(f"Error in local_read: {e}")

    def critical_update(self, increment=1):
        start_time = time.perf_counter()
        try:
            with self.global_lock:
                lock_time = time.perf_counter() - start_time
                self.lock_wait_time += lock_time  
                self.global_counter += increment
        except Exception as e:
            print(f"Error in critical_update: {e}")

    def hybrid_operation(self, key, value):
        self.local_write(key, value)
        self.critical_update(increment=value)

    def print_stats(self):
        print(f"Total Reads: {self.local_reads}, Total Writes: {self.local_writes}")
        print(f"Total Lock Wait Time: {self.lock_wait_time:.6f} sec")
        print(f"Total Write Conflicts: {self.write_conflicts}")

def worker(hybrid_obj, task_queue):
    print(f"Thread {threading.current_thread().name} started.")
    while True:
        try:
            task_type, key, value = task_queue.get(timeout=2)  # אם אין משימה אחרי 2 שניות -> יציאה
            print(f"Thread {threading.current_thread().name} processing {task_type}")
            if task_type == "local":
                if random.random() < 0.5:
                    hybrid_obj.local_write(key, value)
                else:
                    hybrid_obj.local_read(key)
            elif task_type == "critical":
                hybrid_obj.critical_update(increment=value)
            elif task_type == "hybrid":
                hybrid_obj.hybrid_operation(key, value)
            task_queue.task_done()
        except queue.Empty:
            print(f"Thread {threading.current_thread().name} exiting - queue empty.")
            return  # יציאה מהלולאה

def run_test(num_threads, num_operations, hybrid_obj):
    task_queue = queue.Queue()
    
    for _ in range(num_operations):
        task_type = random.choices(["local", "critical", "hybrid"], weights=[0.4, 0.2, 0.4])[0]
        key = f"resource_{random.randint(1, 10)}"
        value = random.randint(100, 2000)
        task_queue.put((task_type, key, value))

    threads = []

    print("Starting threads...")
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(hybrid_obj, task_queue))
        threads.append(t)
        t.start()

    print("Waiting for threads to finish...")
    for t in threads:
        t.join(timeout=5)  # מונע תקיעה

    total_local_sum = sum(sum(shard.values()) for shard in hybrid_obj.local_data)
    difference = hybrid_obj.global_counter - total_local_sum

    print(f"Test with {num_threads} threads and {num_operations} operations completed.")
    print(f"Final Global Counter = {hybrid_obj.global_counter}")
    print(f"Total Local Sum = {total_local_sum}")
    print(f"Difference = {difference}")
    hybrid_obj.print_stats()
    print("-" * 40)
    
    return num_threads, difference, hybrid_obj.lock_wait_time, hybrid_obj.write_conflicts

if __name__ == "__main__":
    thread_counts = [1, 2, 4, 8, 16, 32, 64]
    num_operations = 1000  
    results = []

    for count in thread_counts:
        hybrid_ds = HybridDataStructure(num_shards=4)
        results.append(run_test(count, num_operations, hybrid_ds))

    thread_nums, consistency_gaps, lock_waits, write_conflicts = zip(*results)

    plt.figure(figsize=(12, 6))
    
    plt.subplot(1, 3, 1)
    plt.plot(thread_nums, consistency_gaps, marker='o', color='r', label="Consistency Gap")
    plt.xlabel("Number of Threads")
    plt.ylabel("Global Counter - Local Sum")
    plt.title("Consistency Gap vs Threads")
    plt.legend()

    plt.subplot(1, 3, 2)
    plt.plot(thread_nums, lock_waits, marker='o', color='g', label="Lock Wait Time")
    plt.xlabel("Number of Threads")
    plt.ylabel("Time (s)")
    plt.title("Lock Wait Time vs Threads")
    plt.legend()

    plt.subplot(1, 3, 3)
    plt.plot(thread_nums, write_conflicts, marker='o', color='b', linestyle="dashed", label="Write Conflicts")
    plt.xlabel("Number of Threads")
    plt.ylabel("Conflict Count")
    plt.title("Write Conflicts vs Threads")
    plt.legend()

    plt.tight_layout()
    plt.show()
