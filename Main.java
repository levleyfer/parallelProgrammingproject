package project;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {
    private static final int TOTAL_OPERATIONS = 10000; // Total operations to be shared among threads
    private static final int SEGMENT_SIZE = 100;        // Slots per segment

    public static void main(String[] args) {
        int[] threadCounts = {1, 2, 4, 8, 16, 32, 64}; // Thread counts to test
        for (int numThreads : threadCounts) {
            runTest(numThreads);
        }
    }

    private static void runTest(int numThreads) {
        HybridParallelQueue<Integer> queue = new HybridParallelQueue<>(numThreads, SEGMENT_SIZE);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger totalEnqueued = new AtomicInteger(0);
        AtomicInteger totalDequeued = new AtomicInteger(0);
        
        // Calculate operations per thread
        int operationsPerThread = TOTAL_OPERATIONS / numThreads;
        System.out.println("Starting test with " + numThreads + " threads...");
        
        long startTime = System.currentTimeMillis();
        
        // Create a list to store each thread's execution time
        List<Long> threadExecutionTimes = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            threadExecutionTimes.add(0L);
        }
        
        // Submit worker tasks
        for (int i = 0; i < numThreads; i++) {
            int threadIndex = i;
            executor.submit(() -> {
                long threadStartTime = System.currentTimeMillis();
                int localEnqueued = 0;
                int localDequeued = 0;
                
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        // Alternate between enqueue and dequeue operations
                        if (j % 2 == 0) {
                            int value = counter.getAndIncrement();
                            boolean success = queue.enqueue(value);
                            if (success) {
                                localEnqueued++;
                                totalEnqueued.incrementAndGet();
                            }
                        } else {
                            Integer item = queue.dequeue();
                            if (item != null) {
                                localDequeued++;
                                totalDequeued.incrementAndGet();
                            } else {
                                // If dequeue fails, try to enqueue instead
                                int value = counter.getAndIncrement();
                                queue.enqueue(value);
                                localEnqueued++;
                                totalEnqueued.incrementAndGet();
                            }
                        }
                        
                        // Log progress every 100 operations
                        if (j % 100 == 0) {
                            System.out.println("Thread " + threadIndex + " processed " + j + 
                                               " operations. Queue size ~" + queue.size());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Thread " + threadIndex + " error: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    long threadEndTime = System.currentTimeMillis();
                    long elapsed = threadEndTime - threadStartTime;
                    threadExecutionTimes.set(threadIndex, elapsed);
                }
            });
        }
        
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Threads: " + numThreads + 
                           ", Total Time: " + totalTime + " ms" +
                           ", Enqueued: " + totalEnqueued.get() + 
                           ", Dequeued: " + totalDequeued.get() +
                           ", Final size: " + queue.size());
        
        // Print execution time per thread
        for (int i = 0; i < threadExecutionTimes.size(); i++) {
            System.out.println("Thread " + i + " execution time: " + threadExecutionTimes.get(i) + " ms");
        }
    }

    private static int getThreadId(int numThreads) {
        return (int) (Thread.currentThread().getId() % numThreads);
    }
}

class HybridParallelQueue<T> {
    private final Segment<T>[] segments;
    private final AtomicInteger globalCounter;
    private final int k;
    private final int numThreads;
    // Backup queue for when segments are full
    private final ConcurrentLinkedQueue<T> backupQueue = new ConcurrentLinkedQueue<>();

    @SuppressWarnings("unchecked")
    public HybridParallelQueue(int numThreads, int k) {
        this.numThreads = Math.max(1, numThreads);
        this.k = k;
        this.segments = new Segment[this.numThreads];
        this.globalCounter = new AtomicInteger(0);
        for (int i = 0; i < this.numThreads; i++) {
            segments[i] = new Segment<>(k);
        }
    }

    public boolean enqueue(T item) {
        if (item == null) {
            throw new NullPointerException("Cannot enqueue null items");
        }
        
        int threadId = getThreadId();
        Segment<T> segment = segments[threadId];
        int seqNum = globalCounter.getAndIncrement();
        
        // Try current segment
        int tail = segment.getTail();
        if (tail < k && segment.compareAndSet(tail, null, item)) {
            segment.advanceTail(tail);
            segment.storeSeqNum(tail, seqNum);
            return true;
        }
        
        // Try other segments
        for (int i = 0; i < segments.length; i++) {
            int targetId = (threadId + i + 1) % segments.length;
            Segment<T> targetSegment = segments[targetId];
            
            tail = targetSegment.getTail();
            if (tail < k && targetSegment.compareAndSet(tail, null, item)) {
                targetSegment.advanceTail(tail);
                targetSegment.storeSeqNum(tail, seqNum);
                return true;
            }
        }
        
        // Fall back to backup queue if all segments are full
        backupQueue.offer(item);
        return true;
    }

    public T dequeue() {
        int threadId = getThreadId();
        
        // Try own segment first
        T item = dequeueFromSegment(segments[threadId]);
        if (item != null) {
            return item;
        }
        
        // Try other segments
        for (int i = 0; i < segments.length; i++) {
            int targetId = (threadId + i + 1) % segments.length;
            item = dequeueFromSegment(segments[targetId]);
            if (item != null) {
                return item;
            }
        }
        
        // Try backup queue
        return backupQueue.poll();
    }
    
    private T dequeueFromSegment(Segment<T> segment) {
        int head = segment.getHead();
        if (head < segment.getTail()) {
            T item = segment.getItem(head);
            if (item != null && segment.compareAndSet(head, item, null)) {
                segment.advanceHead(head);
                return item;
            }
        }
        return null;
    }

    public int size() {
        int count = backupQueue.size();
        for (Segment<T> segment : segments) {
            count += segment.getTail() - segment.getHead();
        }
        return count;
    }

    public T[] getGlobalSnapshot() {
        List<ItemWithSeq<T>> itemsWithSeq = new ArrayList<>();
        
        // Add items from segments
        for (Segment<T> segment : segments) {
            for (int i = segment.getHead(); i < segment.getTail(); i++) {
                T item = segment.getItem(i);
                int seq = segment.getSeqNum(i);
                if (item != null) {
                    itemsWithSeq.add(new ItemWithSeq<>(item, seq));
                }
            }
        }
        
        // Add items from backup queue (no sequence numbers available)
        int maxSeq = -1;
        for (ItemWithSeq<T> item : itemsWithSeq) {
            maxSeq = Math.max(maxSeq, item.seq);
        }
        
        int nextSeq = maxSeq + 1;
        for (T item : backupQueue) {
            itemsWithSeq.add(new ItemWithSeq<>(item, nextSeq++));
        }

        // Sort by sequence number
        itemsWithSeq.sort((a, b) -> Integer.compare(a.seq, b.seq));

        @SuppressWarnings("unchecked")
        T[] snapshot = (T[]) new Object[itemsWithSeq.size()];
        for (int i = 0; i < itemsWithSeq.size(); i++) {
            snapshot[i] = itemsWithSeq.get(i).item;
        }
        return snapshot;
    }

    private static class ItemWithSeq<T> {
        T item;
        int seq;
        ItemWithSeq(T item, int seq) {
            this.item = item;
            this.seq = seq;
        }
    }

    private int getThreadId() {
        return (int) (Thread.currentThread().getId() % numThreads);
    }
}

class Segment<T> {
    private final AtomicReference<T>[] slots;
    private final AtomicInteger[] seqNums;
    private final AtomicInteger head;
    private final AtomicInteger tail;

    @SuppressWarnings("unchecked")
    public Segment(int size) {
        slots = new AtomicReference[size];
        seqNums = new AtomicInteger[size];
        for (int i = 0; i < size; i++) {
            slots[i] = new AtomicReference<>(null);
            seqNums[i] = new AtomicInteger(-1);
        }
        head = new AtomicInteger(0);
        tail = new AtomicInteger(0);
    }

    public int getTail() {
        return tail.get();
    }

    public int getHead() {
        return head.get();
    }

    public void advanceTail(int oldTail) {
        tail.compareAndSet(oldTail, oldTail + 1);
    }

    public void advanceHead(int oldHead) {
        head.compareAndSet(oldHead, oldHead + 1);
    }

    public boolean compareAndSet(int index, T expected, T newValue) {
        return slots[index].compareAndSet(expected, newValue);
    }

    public T getItem(int index) {
        return slots[index].get();
    }

    public void storeSeqNum(int index, int seq) {
        seqNums[index].set(seq);
    }

    public int getSeqNum(int index) {
        return seqNums[index].get();
    }
}



