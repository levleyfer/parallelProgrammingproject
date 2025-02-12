
import java.util.*;
import java.util.concurrent.*;

class MppRunner {
    private static final Random random = new Random();

    private static void demoLocalOperations(MppRunnerCode hybridObj) {
        for (int i = 0; i < 5; i++) {
            String key = "user_" + (random.nextInt(3) + 1);
            if (random.nextBoolean()) {
                int val = random.nextInt(900) + 100;
                hybridObj.localWrite(key, val);
            } else {
                hybridObj.localRead(key);
            }
            try { Thread.sleep((long) (random.nextDouble() * 100)); } catch (InterruptedException ignored) {}
        }
    }

    private static void demoCriticalOperation(MppRunnerCode hybridObj) {
        for (int i = 0; i < 5; i++) {
            hybridObj.criticalUpdate(1);
            try { Thread.sleep((long) (random.nextDouble() * 100)); } catch (InterruptedException ignored) {}
        }
    }

    private static void demoHybridOperation(MppRunnerCode hybridObj, int threadId) {
        for (int i = 0; i < 5; i++) {
            String key = "resource_" + threadId;
            int val = random.nextInt(1001) + 1000;
            hybridObj.hybridOperation(key, val);
            try { Thread.sleep((long) (random.nextDouble() * 100)); } catch (InterruptedException ignored) {}
        }
    }

    public static void runTest(int numThreads, MppRunnerCode hybridObj) {
        List<Thread> threads = new ArrayList<>();
        int partitionSize = numThreads / 3;

        for (int i = 0; i < partitionSize; i++) {
            final int index = i; // Capture the index for each thread
            threads.add(new Thread(() -> demoLocalOperations(hybridObj)));
        }

        for (int i = 0; i < partitionSize; i++) {
            final int index = i; // Capture the index for each thread
            threads.add(new Thread(() -> demoCriticalOperation(hybridObj)));
        }

        for (int i = 0; i < partitionSize; i++) {
            final int index = i; // Capture the index for each thread
            threads.add(new Thread(() -> demoHybridOperation(hybridObj, index)));
        }

        while (threads.size() < numThreads) {
            final int index = threads.size(); // Use the current thread size as the index
            threads.add(new Thread(() -> demoHybridOperation(hybridObj, index)));
        }

        long startTime = System.currentTimeMillis();
        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try { t.join(); } catch (InterruptedException ignored) {}
        });
        long endTime = System.currentTimeMillis();

        int totalLocalSum = hybridObj.getTotalLocalSum();
        int difference = hybridObj.getGlobalCounter() - totalLocalSum;

        System.out.println("Test with " + numThreads + " threads completed in " + (endTime - startTime) + " ms");
        System.out.println("Final Global Counter = " + hybridObj.getGlobalCounter());
        System.out.println("Total Local Sum = " + totalLocalSum);
        System.out.println("Difference = " + difference);
        hybridObj.printStats();
        System.out.println("----------------------------------------");
    }

    public static void main(String[] args) {
        int[] threadCounts = {1, 2, 4, 8, 16, 32, 64};
        for (int count : threadCounts) {
            MppRunnerCode hybridObj = new MppRunnerCode(4);  // Fixed instantiation
            runTest(count, hybridObj);
        }
    }
}
