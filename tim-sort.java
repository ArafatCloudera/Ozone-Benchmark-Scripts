import java.util.Arrays;
import java.util.Random;

public class TimSortExample {

    public static void main(String[] args) {
        // Create an array with 1 million Integer elements
        Integer[] array = new Integer[1_000_000];

        // Fill the array with random values
        Random random = new Random();
        for (int i = 0; i < array.length; i++) {
            array[i] = random.nextInt();
        }

        // Record the start time
        long startTime = System.currentTimeMillis();

        // Sort the array using TimSort (implicitly used by Arrays.sort for objects)
        Arrays.sort(array);

        // Record the end time
        long endTime = System.currentTimeMillis();

        // Calculate and print the sorting time
        System.out.println("Sorting 1 million integers took " + (endTime - startTime) + " milliseconds.");
    }
}
