import threading
import time
import random
import queue
import psutil  # For CPU utilization
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

def worker(hybrid_obj, task_queue):
    while True:
        try:
            task_type, key, value = task_queue.get(timeout=2)
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
            return

def scalability_test(hybrid_obj, num_operations, thread_counts):
    results = []
    
    for num_threads in thread_counts:
        task_queue = queue.Queue()
        
        for _ in range(num_operations):
            task_type = random.choices(["local", "critical", "hybrid"], weights=[0.4, 0.2, 0.4])[0]
            key = f"resource_{random.randint(1, 10)}"
            value = random.randint(100, 2000)
            task_queue.put((task_type, key, value))
        
        threads = []
        
        start_time = time.perf_counter()
        cpu_usage_before = psutil.cpu_percent(interval=None)
        
        for _ in range(num_threads):
            t = threading.Thread(target=worker, args=(hybrid_obj, task_queue))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        execution_time = time.perf_counter() - start_time
        cpu_usage_after = psutil.cpu_percent(interval=None)
        throughput = num_operations / execution_time
        avg_lock_wait = hybrid_obj.lock_wait_time / max(1, num_operations)
        
        results.append((num_threads, execution_time, throughput, avg_lock_wait, cpu_usage_after))
        
        print(f"Threads: {num_threads}, Time: {execution_time:.4f}s, Throughput: {throughput:.2f} ops/s, Lock Wait: {avg_lock_wait:.6f}s, CPU: {cpu_usage_after}%")
    
    return results

if __name__ == "__main__":
    thread_counts = [1, 2, 4, 8, 16, 32, 64]
    num_operations = 1000  
    hybrid_ds = HybridDataStructure(num_shards=4)
    results = scalability_test(hybrid_ds, num_operations, thread_counts)
    
    thread_nums, exec_times, throughputs, lock_waits, cpu_usages = zip(*results)
    
    plt.figure(figsize=(12, 6))
    
    plt.subplot(1, 3, 1)
    plt.plot(thread_nums, exec_times, marker='o', color='r', label="Execution Time")
    plt.xlabel("Number of Threads")
    plt.ylabel("Time (s)")
    plt.title("Execution Time vs Threads")
    plt.legend()
    
    plt.subplot(1, 3, 2)
    plt.plot(thread_nums, throughputs, marker='o', color='g', label="Throughput")
    plt.xlabel("Number of Threads")
    plt.ylabel("Operations per second")
    plt.title("Throughput vs Threads")
    plt.legend()
    
    plt.subplot(1, 3, 3)
    plt.plot(thread_nums, lock_waits, marker='o', color='b', label="Lock Wait Time")
    plt.xlabel("Number of Threads")
    plt.ylabel("Time (s)")
    plt.title("Lock Wait Time vs Threads")
    plt.legend()
    
    plt.tight_layout()
    plt.show()
