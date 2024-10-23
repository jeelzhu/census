import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Implement the two methods below. We expect this class to be stateless and thread safe.
 */
public class Census {
    private static final Logger LOGGER = Logger.getLogger(Census.class.getName());

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

    private static final ExecutorService THREAD_POOL = Executors.newFixedThreadPool(CORES);


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
        return retrieveTopAges(retrieveAgeQuantityMap(region), 3);
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {
        List<CompletableFuture<Map<Integer, Integer>>> futures = regionNames.stream()
            .map(region -> CompletableFuture.supplyAsync(() -> retrieveAgeQuantityMap(region), THREAD_POOL)
                .exceptionally(ex -> {
                    LOGGER.log(Level.SEVERE, "Exception processing region: " + region, ex);
                    return Collections.emptyMap();
                }))
        .collect(Collectors.toList());

        Map<Integer, Integer> allRegionsAgeQuantityMap = futures.stream()
            .map(CompletableFuture::join)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    Integer::sum
            ));
        return retrieveTopAges(allRegionsAgeQuantityMap, 3);
    }

    /**
     * Given a region name, retrieve the ages and their quantities from the source: iterator.
     */
    private Map<Integer, Integer>  retrieveAgeQuantityMap(String region) {
        Map<Integer, Integer> ageQuantityMap = new HashMap<>();
        try (AgeInputIterator iterator = iteratorFactory.apply(region)) {
            while (iterator.hasNext()) {
                int age = iterator.next();
                if (age >= 0) {
                    ageQuantityMap.put(age, ageQuantityMap.getOrDefault(age, 0) + 1);
                } else {
                    LOGGER.log(Level.INFO,"Invalid age: " + age);
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "IOException processing region: " + region, e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unexpected exception processing region: " + region, e);
        }
        return ageQuantityMap;
    }

    /**
     * Given a map of ages and their quantities, return the top N ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    private String[] retrieveTopAges(Map<Integer, Integer> ageQuantityMap, int top) {
        List<Map.Entry<Integer, Integer>> sortedEntries = ageQuantityMap.entrySet().stream()
                .sorted(Map.Entry.<Integer, Integer>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey())) // Sort by value descending, then by key ascending
                .limit(top)
                .collect(Collectors.toList());

        int size = Math.min(top, sortedEntries.size());
        String[] result = new String[size];
        for (int i = 0; i < size; i++) {
            Map.Entry<Integer, Integer> entry = sortedEntries.get(i);
            result[i] = String.format(OUTPUT_FORMAT, i + 1, entry.getKey(), entry.getValue());
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
