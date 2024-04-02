import random
import time

def main():
    # Create a list with 1 million random integers
    array = [random.randint(0, 1000000) for _ in range(1000000)]

    # Record the start time
    start_time = time.time()

    # Sort the list using TimSort (implicitly used by sort())
    array.sort()

    # Alternatively, you could use sorted() to achieve the same effect:
    # sorted_array = sorted(array)

    # Record the end time
    end_time = time.time()

    # Calculate and print the sorting time
    print(f"Sorting 1 million integers took {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
