# Hybrid Parallel Data Structure

This project demonstrates a hybrid parallel data structure that combines **local linearizability** and **strong linearizability**.

## Idea

- Local shards reduce lock contention and improve performance.
- A global counter handles critical strongly consistent updates.
- Hybrid operations update both local and global state.

## Files

- `HybridDataStructure.py` – basic Python demo.
- `hybrid_scalability_benchmark.py` – Python scalability/performance test.
- `hybrid_consistency_metrics_test.py` – Python consistency gap and lock metrics test.
- `Main.java` – Java implementation/test for the hybrid parallel queue.
- `MppRunner.java` – Java runner file.
- `MppRunnerCode.java` – additional Java runner/code version.
- `Hybrid Parallel Data Structure.pdf` – project report.
- `mpp.out` – output/results file.

## Metrics

The project measures:

- Execution time
- Throughput
- Lock wait time
- Write conflicts
- Global counter
- Local shard sum
- Consistency gap

## Conclusion

The project shows the trade-off between scalability and consistency. Sharding improves performance, but with more threads, the gap between local and global state can increase.
