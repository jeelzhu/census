import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Implement the two methods below. We expect this class to be stateless and thread safe.
 */
public class Census {
    /**
     * Number of cores in the current machine.
     */
    private static final int CORES = Runtime.getRuntime().availableProcessors();

    /**
     * Output format expected by our tests.
     */
    public static final String OUTPUT_FORMAT = "%d:%d=%d"; // Position:Age=Total

    /**
     * Factory for iterators.
     */
    private final Function<String, Census.AgeInputIterator> iteratorFactory;

    /**
     * Creates a new Census calculator.
     *
     * @param iteratorFactory factory for the iterators.
     */
    public Census(Function<String, Census.AgeInputIterator> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }



    /**
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {
        AgeInputIterator iterator = iteratorFactory.apply(region);
        Map<Integer, Integer> ageQuantityMap = new HashMap<>();
        try {
            while (iterator.hasNext()) {
                int age = iterator.next();
                if (age >= 0) {
                    ageQuantityMap.put(age, ageQuantityMap.getOrDefault(age, 0) + 1);
                } else {
                    // Handle invalid ages
                    System.out.println("Invalid age: " + age);
                }
            }
        } finally {
            try {
                iterator.close();
            } catch (Exception e) {
                // Handle potential close exception
                System.out.println("Exception closing iterator");
            }
        }
        return retrieveTopAges(ageQuantityMap, 3);
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {
        ExecutorService threadPool = Executors.newFixedThreadPool(CORES);
        List<Future<Map<Integer, Integer>>> futures = new ArrayList<>();

        for (String region : regionNames) {
            futures.add(threadPool.submit(() -> {
                Map<Integer, Integer> ageQuantityMap = new HashMap<>();
                try (AgeInputIterator iterator = iteratorFactory.apply(region)) {
                    while (iterator.hasNext()) {
                        int age = iterator.next();
                        if (age >= 0) {
                            ageQuantityMap.put(age, ageQuantityMap.getOrDefault(age, 0) + 1);
                        }
                    }
                } catch (IOException e) {
                    // Handle any exceptions during processing
                    System.out.println("Exception processing region: " + region);
                }
                return ageQuantityMap;
            }));
        }

        // Combine results from all regions
        Map<Integer, Integer> allRegionsAgeQuantityMap = new HashMap<>();
        for (Future<Map<Integer, Integer>> future : futures) {
            try {
                Map<Integer, Integer> result = future.get();
                for (Map.Entry<Integer, Integer> entry : result.entrySet()) {
                    allRegionsAgeQuantityMap.put(entry.getKey(), allRegionsAgeQuantityMap.getOrDefault(entry.getKey(), 0) + entry.getValue());
                }
            } catch (Exception e) {
                // Handle any exceptions during processing
                System.out.println("Exception combining results");
            }
        }
        threadPool.shutdown();
        return retrieveTopAges(allRegionsAgeQuantityMap, 3);
    }

    /*
     * Given a map of ages and their quantities, return the top N ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    private String[] retrieveTopAges(Map<Integer, Integer> ageQuantityMap, int top) {
        PriorityQueue<Map.Entry<Integer, Integer>> minHeap = new PriorityQueue<>(Map.Entry.comparingByValue());
        // Use a min heap to find the top N ages
        for (Map.Entry<Integer, Integer> entry : ageQuantityMap.entrySet()) {
            minHeap.add(entry);
            if (minHeap.size() > top) {
                minHeap.poll();
            }
        }
        int minHeapSize = minHeap.size();
        String[] result = new String[minHeapSize];
        int index = minHeapSize;
        // Retrieve the top N ages from the min heap and format the output
        while (!minHeap.isEmpty()) {
            Map.Entry<Integer, Integer> entry = minHeap.poll();
            result[--index] = String.format(OUTPUT_FORMAT, index+1, entry.getKey(), entry.getValue());
        }
        return result;
    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}
