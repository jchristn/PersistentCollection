namespace AutomatedTest.PersistentStack
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using PersistentCollection;

    class Program
    {
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.

        private static string _testDirectory = Path.Combine(Path.GetTempPath(), "PersistentStackTest");
        private static int _passedTests = 0;
        private static int _failedTests = 0;

        static async Task Main(string[] args)
        {
            Console.WriteLine("PersistentStack Test Program");
            Console.WriteLine($"Test directory: {_testDirectory}");
            Console.WriteLine("-----------------------------------------");

            CleanTestDirectory();
            await RunBasicTests();
            await RunStackTests();
            await RunConcurrentTests();
            await RunComplexScenarioTests();

            // Summary
            Console.WriteLine("-----------------------------------------");
            Console.WriteLine($"Tests completed: {_passedTests + _failedTests}");
            Console.WriteLine($"Passed: {_passedTests}");
            Console.WriteLine($"Failed: {_failedTests}");

            // Cleanup test directory at the end
            CleanTestDirectory();
        }

        #region Setup and Helper Methods

        private static void CleanTestDirectory()
        {
            if (Directory.Exists(_testDirectory))
            {
                try
                {
                    Directory.Delete(_testDirectory, true);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Could not delete test directory: {ex.Message}");
                }
            }
        }

        private static void AssertTrue(bool condition, string message)
        {
            if (condition)
            {
                _passedTests++;
                Console.WriteLine($"[PASS] {message}");
            }
            else
            {
                _failedTests++;
                Console.WriteLine($"[FAIL] {message}");
            }
        }

        private static void AssertEquals<T>(T expected, T actual, string message)
        {
            if (EqualityComparer<T>.Default.Equals(expected, actual))
            {
                _passedTests++;
                Console.WriteLine($"[PASS] {message}");
            }
            else
            {
                _failedTests++;
                Console.WriteLine($"[FAIL] {message} Expected: {expected}, Actual: {actual}");
            }
        }

        private static void AssertCollectionEquals<T>(IEnumerable<T> expected, IEnumerable<T> actual, string message)
        {
            bool areEqual = expected.Count() == actual.Count() &&
                            expected.Zip(actual, (e, a) => EqualityComparer<T>.Default.Equals(e, a)).All(x => x);

            if (areEqual)
            {
                _passedTests++;
                Console.WriteLine($"[PASS] {message}");
            }
            else
            {
                _failedTests++;
                Console.WriteLine($"[FAIL] {message}");
                Console.WriteLine($"  Expected: [{string.Join(", ", expected)}]");
                Console.WriteLine($"  Actual: [{string.Join(", ", actual)}]");
            }
        }

        #endregion

        #region Test Categories

        private static async Task RunBasicTests()
        {
            Console.WriteLine("\nRunning Basic Functionality Tests...");

            // Test constructor and initialization
            {
                var stackFile = Path.Combine(_testDirectory, "basic.idx");
                using var stack = new PersistentStack<string>(stackFile);
                AssertEquals(0, stack.Count, "New stack should be empty");
            }

            // Test Push and Count
            {
                var stackFile = Path.Combine(_testDirectory, "push_count.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                AssertEquals(3, stack.Count, "Stack should contain 3 items after pushing");
            }

            // Test Push and Pop
            {
                var stackFile = Path.Combine(_testDirectory, "push_pop.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                AssertEquals(3, stack.Count, "Stack should contain 3 items after pushing");

                string item = stack.Pop();
                AssertEquals("Item3", item, "Pop should return the last pushed item (LIFO)");
                AssertEquals(2, stack.Count, "Stack should contain 2 items after one pop");

                item = stack.Pop();
                AssertEquals("Item2", item, "Pop should return the previous pushed item");
                AssertEquals(1, stack.Count, "Stack should contain 1 item after two pops");

                item = stack.Pop();
                AssertEquals("Item1", item, "Pop should return the first pushed item");
                AssertEquals(0, stack.Count, "Stack should be empty after popping all items");
            }

            // Test Peek
            {
                var stackFile = Path.Combine(_testDirectory, "peek.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                AssertEquals("Item3", stack.Peek(), "Peek should return the top item without removing it");
                AssertEquals(3, stack.Count, "Stack count should remain unchanged after peek");

                string item = stack.Pop();
                AssertEquals("Item3", item, "Pop should return the same item as peek");
                AssertEquals("Item2", stack.Peek(), "Peek should now return the new top item");
            }

            // Test enumeration to access items
            {
                var stackFile = Path.Combine(_testDirectory, "enumeration.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                var items = stack.ToArray();
                AssertEquals("Item3", items[0], "First item in array should be 'Item3'");
                AssertEquals("Item2", items[1], "Second item in array should be 'Item2'");
                AssertEquals("Item1", items[2], "Third item in array should be 'Item1'");
            }

            // Test Clear
            {
                var stackFile = Path.Combine(_testDirectory, "clear.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                stack.Clear();

                AssertEquals(0, stack.Count, "Stack should be empty after Clear()");
            }

            // Test persistence across instances
            {
                var stackFile = Path.Combine(_testDirectory, "persistence.idx");

                // First instance adds items
                using (var stack1 = new PersistentStack<string>(stackFile))
                {
                    stack1.Push("Item1");
                    stack1.Push("Item2");
                    stack1.Push("Item3");

                    AssertEquals(3, stack1.Count, "First instance should have 3 items");
                }

                // Second instance reads the same items
                using (var stack2 = new PersistentStack<string>(stackFile))
                {
                    AssertEquals(3, stack2.Count, "Second instance should have 3 items");
                    var items = stack2.ToArray();
                    AssertEquals("Item3", items[0], "First item should persist");
                    AssertEquals("Item2", items[1], "Second item should persist");
                    AssertEquals("Item1", items[2], "Third item should persist");
                }
            }

            // Test for different data types
            {
                // Test with integers
                using (var intStack = new PersistentStack<int>(Path.Combine(_testDirectory, "data_types_int.idx")))
                {
                    intStack.Push(10);
                    intStack.Push(20);
                    intStack.Push(30);

                    AssertEquals(3, intStack.Count, "Integer stack should have 3 items");
                    var items = intStack.ToArray();
                    AssertEquals(30, items[0], "First int should be 30");
                    AssertEquals(20, items[1], "Second int should be 20");
                    AssertEquals(10, items[2], "Third int should be 10");
                }

                // Test with complex objects
                using (var personStack = new PersistentStack<Person>(Path.Combine(_testDirectory, "data_types_person.idx")))
                {
                    personStack.Push(new Person { Name = "Alice", Age = 25 });
                    personStack.Push(new Person { Name = "Bob", Age = 30 });

                    AssertEquals(2, personStack.Count, "Person stack should have 2 items");
                    var people = personStack.ToArray();
                    AssertEquals("Bob", people[0].Name, "First person should be Bob");
                    AssertEquals(25, people[1].Age, "Second person's age should be 25");
                }
            }
        }

        private static async Task RunStackTests()
        {
            Console.WriteLine("\nRunning Stack Implementation Tests...");

            // Test Peek
            {
                var stackFile = Path.Combine(_testDirectory, "stack_peek.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                // Test Peek (doesn't remove the item)
                string peekedItem = stack.Peek();
                AssertEquals("Item3", peekedItem, "Peek should return the first item without removing it");
                AssertEquals(3, stack.Count, "Stack should still have 3 items after Peek");

                // Peek again and verify the same item is returned
                peekedItem = stack.Peek();
                AssertEquals("Item3", peekedItem, "Repeated Peek should return the same first item");
            }

            // Test Contains
            {
                var stackFile = Path.Combine(_testDirectory, "contains.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                AssertTrue(stack.Contains("Item2"), "Contains should return true for existing item");
                AssertTrue(!stack.Contains("NonExistentItem"), "Contains should return false for non-existent item");
            }

            // Test CopyTo
            {
                var stackFile = Path.Combine(_testDirectory, "copyto.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                string[] array = new string[5];
                stack.CopyTo(array, 1);

                AssertEquals(null, array[0], "First element should be null");
                AssertEquals("Item3", array[1], "Second element should be 'Item3'");
                AssertEquals("Item2", array[2], "Third element should be 'Item2'");
                AssertEquals("Item1", array[3], "Fourth element should be 'Item1'");
                AssertEquals(null, array[4], "Fifth element should be null");
            }

            // Test ToArray
            {
                var stackFile = Path.Combine(_testDirectory, "to_array.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                string[] array = stack.ToArray();

                AssertEquals(3, array.Length, "ToArray should return array with correct length");
                AssertEquals("Item3", array[0], "First element should be 'Item3'");
                AssertEquals("Item2", array[1], "Second element should be 'Item2'");
                AssertEquals("Item1", array[2], "Third element should be 'Item1'");
            }

            // Test TryPop
            {
                var stackFile = Path.Combine(_testDirectory, "try_pop.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");

                // Test TryPop success
                bool success = stack.TryPop(out string result);
                AssertTrue(success, "TryPop should return true when stack is not empty");
                AssertEquals("Item2", result, "TryPop should set out parameter to the top item");
                AssertEquals(1, stack.Count, "Stack should have 1 item after successful TryPop");

                // Test TryPop on empty stack
                stack.Clear();
                success = stack.TryPop(out result);
                AssertTrue(!success, "TryPop should return false when stack is empty");
                AssertEquals(default(string), result, "TryPop should set out parameter to default value on failure");
            }

            // Test TryPeek
            {
                var stackFile = Path.Combine(_testDirectory, "try_peek.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");

                // Test TryPeek success
                bool success = stack.TryPeek(out string result);
                AssertTrue(success, "TryPeek should return true when stack is not empty");
                AssertEquals("Item1", result, "TryPeek should set out parameter to the top item");
                AssertEquals(1, stack.Count, "Stack should still have 1 item after TryPeek");

                // Test TryPeek on empty stack
                stack.Clear();
                success = stack.TryPeek(out result);
                AssertTrue(!success, "TryPeek should return false when stack is empty");
                AssertEquals(default(string), result, "TryPeek should set out parameter to default value on failure");
            }

            // Test Enumeration using foreach
            {
                var stackFile = Path.Combine(_testDirectory, "foreach.idx");
                using var stack = new PersistentStack<string>(stackFile);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                List<string> items = new List<string>();
                foreach (var item in stack)
                {
                    items.Add(item);
                }

                AssertCollectionEquals(new[] { "Item3", "Item2", "Item1" }, items, "Enumeration should visit all items in LIFO order");
            }

            // Test exceptions
            {
                var stackFile = Path.Combine(_testDirectory, "exceptions.idx");
                using var stack = new PersistentStack<string>(stackFile);

                // Test pop from empty stack
                try
                {
                    var result = stack.Pop();
                    AssertTrue(false, "Should throw InvalidOperationException for empty stack");
                }
                catch (InvalidOperationException)
                {
                    AssertTrue(true, "Correctly threw InvalidOperationException for empty stack");
                }
                catch (Exception ex)
                {
                    AssertTrue(false, $"Threw wrong exception type: {ex.GetType().Name}");
                }

                // Test null constructor
                try
                {
                    var testStack = new PersistentStack<string>(null);
                    AssertTrue(false, "Should throw ArgumentNullException for null path");
                }
                catch (ArgumentNullException)
                {
                    AssertTrue(true, "Correctly threw ArgumentNullException for null path");
                }
                catch (Exception ex)
                {
                    AssertTrue(false, $"Threw wrong exception type: {ex.GetType().Name}");
                }
            }
        }

        private static async Task RunConcurrentTests()
        {
            Console.WriteLine("\nRunning Concurrent Access Tests...");

            // Test concurrent reads
            {
                var stackFile = Path.Combine(_testDirectory, "concurrent_read.idx");
                using var stack = new PersistentStack<int>(stackFile);

                // Add some items
                for (int i = 0; i < 100; i++)
                {
                    stack.Push(i);
                }

                // Create multiple concurrent read tasks
                var tasks = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        var array = stack.ToArray();
                        // Just iterate through the array, no assertions needed
                        for (int j = 0; j < array.Length; j++)
                        {
                            var value = array[j];
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertTrue(true, "Concurrent reads completed without exceptions");
            }

            // Test concurrent writes
            {
                var stackFile = Path.Combine(_testDirectory, "concurrent_write.idx");
                using var stack = new PersistentStack<int>(stackFile);

                // Create multiple concurrent write tasks
                var tasks = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    int taskId = i;
                    tasks.Add(Task.Run(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            int value = taskId * 10 + j;
                            stack.Push(value);
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertEquals(100, stack.Count, "All concurrent pushes should be completed");
            }

            // Test concurrent mixed operations
            {
                var stackFile = Path.Combine(_testDirectory, "concurrent_mixed.idx");
                using var stack = new PersistentStack<string>(stackFile);

                // Add initial items
                for (int i = 0; i < 20; i++)
                {
                    stack.Push($"Item{i}");
                }

                // Perform various operations concurrently
                var tasks = new List<Task>();

                // Task 1: Read operations using ToArray to get a snapshot
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        try
                        {
                            var items = stack.ToArray();
                            if (items.Length > 0)
                            {
                                string value = items[0]; // Always use the first item
                            }
                        }
                        catch (Exception)
                        {
                            // Handle exceptions as needed
                        }
                        Thread.Sleep(5); // Small delay to reduce race conditions
                    }
                }));

                // Task 2: Write operations
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        stack.Push($"NewItem{i}");
                        await Task.Delay(5); // Small delay to reduce race conditions
                    }
                }));

                // Task 3: Pop operations
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 5; i++)
                    {
                        try
                        {
                            if (stack.Count > 0)
                            {
                                stack.TryPop(out string value);
                            }
                        }
                        catch (Exception)
                        {
                            // Handle exceptions as needed
                        }
                        Thread.Sleep(10); // Small delay to avoid racing with push
                    }
                }));

                await Task.WhenAll(tasks);

                AssertTrue(stack.Count >= 15, "Stack should have at least (initial + new - popped) items");
            }

            // Test concurrent push and pop
            {
                var stackFile = Path.Combine(_testDirectory, "concurrent_push_pop.idx");
                using var stack = new PersistentStack<int>(stackFile);

                // Task to continuously push items
                var pushTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 100; i++)
                    {
                        stack.Push(i);
                        await Task.Delay(5); // Small delay
                    }
                });

                // Task to continuously pop items
                var popTask = Task.Run(async () =>
                {
                    int popped = 0;
                    while (popped < 50) // Pop only half
                    {
                        if (stack.Count > 0)
                        {
                            stack.Pop();
                            popped++;
                        }
                        await Task.Delay(10); // Slightly slower than push
                    }
                });

                await Task.WhenAll(pushTask, popTask);

                AssertTrue(stack.Count > 0, "Stack should have items remaining after concurrent operations");
                AssertTrue(stack.Count < 100, "Stack should have had some items popped");
            }

            // Test multiple threads pushing items - maintain LIFO order
            {
                var stackFile = Path.Combine(_testDirectory, "concurrent_order.idx");
                using var stack = new PersistentStack<int>(stackFile);

                // Start with a clean stack
                stack.Clear();

                // Set up threads to push values
                var tasks = new List<Task>();
                int threadsCount = 4;
                int itemsPerThread = 25;

                for (int t = 0; t < threadsCount; t++)
                {
                    int threadId = t;
                    tasks.Add(Task.Run(() =>
                    {
                        for (int i = 0; i < itemsPerThread; i++)
                        {
                            // Each thread adds values in its own range
                            int value = threadId * 100 + i;
                            stack.Push(value);
                            Thread.Sleep(1); // Small delay for interleaving
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                // Verify count
                AssertEquals(threadsCount * itemsPerThread, stack.Count, "All items should be pushed");

                // Analyze the order - we can't guarantee perfect LIFO across threads
                // but we can check that the items from each thread maintain their relative order
                var items = stack.ToArray();

                // Group values by the thread that produced them
                var itemsByThread = new Dictionary<int, List<int>>();

                for (int t = 0; t < threadsCount; t++)
                {
                    itemsByThread[t] = new List<int>();
                }

                foreach (int item in items)
                {
                    int threadId = item / 100; // Determine which thread produced this item
                    if (threadId < threadsCount)
                    {
                        itemsByThread[threadId].Add(item);
                    }
                }

                // For each thread, check that its items are in descending order (LIFO)
                bool allThreadsInOrder = true;
                for (int t = 0; t < threadsCount; t++)
                {
                    var threadItems = itemsByThread[t];
                    for (int i = 0; i < threadItems.Count - 1; i++)
                    {
                        if (threadItems[i] % 100 < threadItems[i + 1] % 100)
                        {
                            allThreadsInOrder = false;
                            break;
                        }
                    }
                }

                AssertTrue(allThreadsInOrder, "Items from each thread should maintain their relative LIFO order");
            }
        }

        private static async Task RunComplexScenarioTests()
        {
            Console.WriteLine("\nRunning Complex Scenario Tests...");

            // Scenario 1: Stack as a processing stack with multiple workers
            {
                var stackFile = Path.Combine(_testDirectory, "scenario_processing_stack.idx");
                using var stack = new PersistentStack<StackItem>(stackFile);

                // Add work items
                int workItems = 20;
                for (int i = 0; i < workItems; i++)
                {
                    stack.Push(new StackItem { Id = i, Value = $"Task {i}" });
                }

                int processedItems = 0;
                int workerCount = 3;
                var workers = new List<Task>();

                for (int i = 0; i < workerCount; i++)
                {
                    workers.Add(Task.Run(() =>
                    {
                        while (true)
                        {
                            StackItem item = null;
                            lock (stack) // Lock to prevent race conditions on TryPop
                            {
                                if (!stack.TryPop(out item))
                                    break; // No more items to process
                            }

                            // Process the item (simulated by incrementing counter)
                            Interlocked.Increment(ref processedItems);
                            Thread.Sleep(10); // Simulate processing time
                        }
                    }));
                }

                await Task.WhenAll(workers);

                AssertEquals(workItems, processedItems, "All work items should be processed");
                AssertEquals(0, stack.Count, "Stack should be empty after processing");
            }

            // Scenario 2: Stack with multiple readers and writers
            {
                var stackFile = Path.Combine(_testDirectory, "scenario_multi_readwrite.idx");
                using var stack = new PersistentStack<string>(stackFile);

                int writerCount = 2;
                int readerCount = 2;
                int itemsPerWriter = 5;
                int readOperations = 10;

                var writers = new List<Task>();
                var readers = new List<Task>();

                // Start writers
                for (int w = 0; w < writerCount; w++)
                {
                    int writerId = w;
                    writers.Add(Task.Run(async () =>
                    {
                        for (int i = 0; i < itemsPerWriter; i++)
                        {
                            stack.Push($"Writer{writerId}_Item{i}");
                            await Task.Delay(5); // Small delay
                        }
                    }));
                }

                // Wait a bit to give writers a head start
                await Task.Delay(100);

                // Start readers (non-destructive reads using Peek)
                for (int r = 0; r < readerCount; r++)
                {
                    readers.Add(Task.Run(() =>
                    {
                        for (int i = 0; i < readOperations; i++)
                        {
                            string item = null;
                            stack.TryPeek(out item);
                            Thread.Sleep(5); // Small delay
                        }
                    }));
                }

                // Wait for all operations to complete
                await Task.WhenAll(writers);
                await Task.WhenAll(readers);

                AssertEquals(writerCount * itemsPerWriter, stack.Count,
                    "Stack should contain all pushed items since readers don't remove items");
            }

            // Scenario 3: Persistent stack with multiple instances
            {
                var stackFile = Path.Combine(_testDirectory, "scenario_multiple_instances.idx");

                // First instance: add initial data
                using (var stack1 = new PersistentStack<string>(stackFile))
                {
                    stack1.Clear(); // Ensure clean state
                    stack1.Push("First");
                    stack1.Push("Second");
                    stack1.Push("Third");
                }

                // Second instance: read and modify
                using (var stack2 = new PersistentStack<string>(stackFile))
                {
                    AssertEquals(3, stack2.Count, "Second instance should see all items");
                    AssertEquals("Third", stack2.Peek(), "Second instance should read correct top item");

                    // Pop top item
                    string item = stack2.Pop();
                    AssertEquals("Third", item, "Should pop items in LIFO order");

                    // Add new item
                    stack2.Push("Fourth");
                }

                // Third instance: verify changes
                using (var stack3 = new PersistentStack<string>(stackFile))
                {
                    AssertEquals(3, stack3.Count, "Third instance should see correct item count");

                    // Verify stack content
                    var items = new List<string>();
                    while (stack3.Count > 0)
                    {
                        items.Add(stack3.Pop());
                    }

                    AssertCollectionEquals(new[] { "Fourth", "Second", "First" }, items,
                        "Final stack should contain the correct items in the correct order");
                }
            }
        }

        #endregion

#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
    }

    // Helper class for testing
    [Serializable]
    public class Person
    {
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).

        public string Name { get; set; }
        public int Age { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is Person other)
            {
                return Name == other.Name && Age == other.Age;
            }
            return false;
        }

        public override int GetHashCode()
        {
            return (Name?.GetHashCode() ?? 0) ^ Age.GetHashCode();
        }

        public override string ToString()
        {
            return $"{Name} ({Age})";
        }

#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    }

    // Helper class for stack scenarios
    [Serializable]
    public class StackItem
    {
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
        public int Id { get; set; }
        public string Value { get; set; }

        public override string ToString()
        {
            return $"Item {Id}: {Value}";
        }

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    }
}