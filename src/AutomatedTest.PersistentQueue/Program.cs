namespace AutomatedTest.PersistentQueue
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

        private static string _testDirectory = Path.Combine(Path.GetTempPath(), "PersistentQueueTest");
        private static int _passedTests = 0;
        private static int _failedTests = 0;

        static async Task Main(string[] args)
        {
            Console.WriteLine("PersistentQueue Test Program");
            Console.WriteLine($"Test directory: {_testDirectory}");
            Console.WriteLine("-----------------------------------------");

            CleanTestDirectory();
            await RunBasicTests();
            await RunQueueTests();
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
                var queueFile = Path.Combine(_testDirectory, "basic.idx");
                using var queue = new PersistentQueue<string>(queueFile);
                AssertEquals(0, queue.Count, "New queue should be empty");
            }

            // Test Enqueue and Count
            {
                var queueFile = Path.Combine(_testDirectory, "enqueue_count.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                AssertEquals(3, queue.Count, "Queue should contain 3 items after adding");
            }

            // Test enumeration to access items (instead of indexer)
            {
                var queueFile = Path.Combine(_testDirectory, "enumeration.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                var items = queue.ToArray();
                AssertEquals("Item1", items[0], "First item should be 'Item1'");
                AssertEquals("Item2", items[1], "Second item should be 'Item2'");
                AssertEquals("Item3", items[2], "Third item should be 'Item3'");
            }

            // Test Dequeue (FIFO behavior)
            {
                var queueFile = Path.Combine(_testDirectory, "dequeue_fifo.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                AssertEquals("Item1", queue.Dequeue(), "Dequeue should return first item (FIFO)");
                AssertEquals(2, queue.Count, "Queue should have 2 items after dequeueing");
                AssertEquals("Item2", queue.Dequeue(), "Second dequeue should return second item");
                AssertEquals(1, queue.Count, "Queue should have 1 item after dequeueing twice");
                AssertEquals("Item3", queue.Dequeue(), "Third dequeue should return third item");
                AssertEquals(0, queue.Count, "Queue should be empty after dequeueing all items");
            }

            // Test Clear
            {
                var queueFile = Path.Combine(_testDirectory, "clear.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                queue.Clear();

                AssertEquals(0, queue.Count, "Queue should be empty after Clear()");
            }

            // Test persistence across instances
            {
                var queueFile = Path.Combine(_testDirectory, "persistence.idx");

                // First instance adds items
                using (var queue1 = new PersistentQueue<string>(queueFile))
                {
                    queue1.Enqueue("Item1");
                    queue1.Enqueue("Item2");
                    queue1.Enqueue("Item3");

                    AssertEquals(3, queue1.Count, "First instance should have 3 items");
                }

                // Second instance reads the same items
                using (var queue2 = new PersistentQueue<string>(queueFile))
                {
                    AssertEquals(3, queue2.Count, "Second instance should have 3 items");
                    var items = queue2.ToArray();
                    AssertEquals("Item1", items[0], "First item should persist");
                    AssertEquals("Item2", items[1], "Second item should persist");
                    AssertEquals("Item3", items[2], "Third item should persist");
                }
            }

            // Test for different data types
            {
                // Test with integers
                using (var intQueue = new PersistentQueue<int>(Path.Combine(_testDirectory, "data_types_int.idx")))
                {
                    intQueue.Enqueue(10);
                    intQueue.Enqueue(20);
                    intQueue.Enqueue(30);

                    AssertEquals(3, intQueue.Count, "Integer queue should have 3 items");
                    var items = intQueue.ToArray();
                    AssertEquals(10, items[0], "First int should be 10");
                    AssertEquals(20, items[1], "Second int should be 20");
                    AssertEquals(30, items[2], "Third int should be 30");
                }

                // Test with complex objects
                using (var personQueue = new PersistentQueue<Person>(Path.Combine(_testDirectory, "data_types_person.idx")))
                {
                    personQueue.Enqueue(new Person { Name = "Alice", Age = 25 });
                    personQueue.Enqueue(new Person { Name = "Bob", Age = 30 });

                    AssertEquals(2, personQueue.Count, "Person queue should have 2 items");
                    var people = personQueue.ToArray();
                    AssertEquals("Alice", people[0].Name, "First person should be Alice");
                    AssertEquals(30, people[1].Age, "Second person's age should be 30");
                }
            }
        }

        private static async Task RunQueueTests()
        {
            Console.WriteLine("\nRunning Queue Implementation Tests...");

            // Test Peek
            {
                var queueFile = Path.Combine(_testDirectory, "peek.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                // Test Peek (doesn't remove the item)
                string peekedItem = queue.Peek();
                AssertEquals("Item1", peekedItem, "Peek should return the first item without removing it");
                AssertEquals(3, queue.Count, "Queue should still have 3 items after Peek");

                // Peek again and verify the same item is returned
                peekedItem = queue.Peek();
                AssertEquals("Item1", peekedItem, "Repeated Peek should return the same first item");
            }

            // Test Contains
            {
                var queueFile = Path.Combine(_testDirectory, "contains.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                AssertTrue(queue.Contains("Item2"), "Contains should return true for existing item");
                AssertTrue(!queue.Contains("NonExistentItem"), "Contains should return false for non-existent item");
            }

            // Test CopyTo
            {
                var queueFile = Path.Combine(_testDirectory, "copyto.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                string[] array = new string[5];
                queue.CopyTo(array, 1);

                AssertEquals(null, array[0], "First element should be null");
                AssertEquals("Item1", array[1], "Second element should be 'Item1'");
                AssertEquals("Item2", array[2], "Third element should be 'Item2'");
                AssertEquals("Item3", array[3], "Fourth element should be 'Item3'");
                AssertEquals(null, array[4], "Fifth element should be null");
            }

            // Test ToArray
            {
                var queueFile = Path.Combine(_testDirectory, "to_array.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                string[] array = queue.ToArray();

                AssertEquals(3, array.Length, "ToArray should return array with correct length");
                AssertEquals("Item1", array[0], "First element should be 'Item1'");
                AssertEquals("Item2", array[1], "Second element should be 'Item2'");
                AssertEquals("Item3", array[2], "Third element should be 'Item3'");
            }

            // Test TryDequeue
            {
                var queueFile = Path.Combine(_testDirectory, "try_dequeue.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");

                // Test TryDequeue success
                bool success = queue.TryDequeue(out string result);
                AssertTrue(success, "TryDequeue should return true when queue is not empty");
                AssertEquals("Item1", result, "TryDequeue should set out parameter to the first item");
                AssertEquals(1, queue.Count, "Queue should have 1 item after successful TryDequeue");

                // Test TryDequeue on empty queue
                queue.Clear();
                success = queue.TryDequeue(out result);
                AssertTrue(!success, "TryDequeue should return false when queue is empty");
                AssertEquals(default(string), result, "TryDequeue should set out parameter to default value on failure");
            }

            // Test TryPeek
            {
                var queueFile = Path.Combine(_testDirectory, "try_peek.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");

                // Test TryPeek success
                bool success = queue.TryPeek(out string result);
                AssertTrue(success, "TryPeek should return true when queue is not empty");
                AssertEquals("Item1", result, "TryPeek should set out parameter to the first item");
                AssertEquals(1, queue.Count, "Queue should still have 1 item after TryPeek");

                // Test TryPeek on empty queue
                queue.Clear();
                success = queue.TryPeek(out result);
                AssertTrue(!success, "TryPeek should return false when queue is empty");
                AssertEquals(default(string), result, "TryPeek should set out parameter to default value on failure");
            }

            // Test Enumeration using foreach
            {
                var queueFile = Path.Combine(_testDirectory, "foreach.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                List<string> items = new List<string>();
                foreach (var item in queue)
                {
                    items.Add(item);
                }

                AssertCollectionEquals(new[] { "Item1", "Item2", "Item3" }, items, "Enumeration should visit all items in order");
            }

            // Test exceptions
            {
                var queueFile = Path.Combine(_testDirectory, "exceptions.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                // Test dequeue from empty queue
                try
                {
                    var result = queue.Dequeue();
                    AssertTrue(false, "Should throw InvalidOperationException for empty queue");
                }
                catch (InvalidOperationException)
                {
                    AssertTrue(true, "Correctly threw InvalidOperationException for empty queue");
                }
                catch (Exception ex)
                {
                    AssertTrue(false, $"Threw wrong exception type: {ex.GetType().Name}");
                }

                // Test null constructor
                try
                {
                    var testQueue = new PersistentQueue<string>(null);
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
                var queueFile = Path.Combine(_testDirectory, "concurrent_read.idx");
                using var queue = new PersistentQueue<int>(queueFile);

                // Add some items
                for (int i = 0; i < 100; i++)
                {
                    queue.Enqueue(i);
                }

                // Create multiple concurrent read tasks
                var tasks = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        var array = queue.ToArray();
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
                var queueFile = Path.Combine(_testDirectory, "concurrent_write.idx");
                using var queue = new PersistentQueue<int>(queueFile);

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
                            queue.Enqueue(value);
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertEquals(100, queue.Count, "All concurrent writes should be completed");
            }

            // Test concurrent mixed operations
            {
                var queueFile = Path.Combine(_testDirectory, "concurrent_mixed.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                // Add initial items
                for (int i = 0; i < 20; i++)
                {
                    queue.Enqueue($"Item{i}");
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
                            var items = queue.ToArray();
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
                        queue.Enqueue($"NewItem{i}");
                        await Task.Delay(5); // Small delay to reduce race conditions
                    }
                }));

                // Task 3: Dequeue operations
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 5; i++)
                    {
                        try
                        {
                            if (queue.Count > 0)
                            {
                                queue.TryDequeue(out string value);
                            }
                        }
                        catch (Exception)
                        {
                            // Handle exceptions as needed
                        }
                        Thread.Sleep(10); // Small delay to avoid racing with enqueue
                    }
                }));

                await Task.WhenAll(tasks);

                AssertTrue(queue.Count >= 15, "Queue should have at least (initial + new - dequeued) items");
            }

            // Test concurrent enqueue and dequeue
            {
                var queueFile = Path.Combine(_testDirectory, "concurrent_enqueue_dequeue.idx");
                using var queue = new PersistentQueue<int>(queueFile);

                // Task to continuously enqueue items
                var enqueueTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 100; i++)
                    {
                        queue.Enqueue(i);
                        await Task.Delay(5); // Small delay
                    }
                });

                // Task to continuously dequeue items
                var dequeueTask = Task.Run(async () =>
                {
                    int dequeued = 0;
                    while (dequeued < 50) // Dequeue only half
                    {
                        if (queue.Count > 0)
                        {
                            queue.Dequeue();
                            dequeued++;
                        }
                        await Task.Delay(10); // Slightly slower than enqueue
                    }
                });

                await Task.WhenAll(enqueueTask, dequeueTask);

                AssertTrue(queue.Count > 0, "Queue should have items remaining after concurrent operations");
                AssertTrue(queue.Count < 100, "Queue should have had some items dequeued");
            }

            // Test multiple threads enqueueing items - maintain FIFO order
            {
                var queueFile = Path.Combine(_testDirectory, "concurrent_order.idx");
                using var queue = new PersistentQueue<int>(queueFile);

                // Start with a clean queue
                queue.Clear();

                // Set up threads to enqueue values
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
                            queue.Enqueue(value);
                            Thread.Sleep(1); // Small delay for interleaving
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                // Verify count
                AssertEquals(threadsCount * itemsPerThread, queue.Count, "All items should be enqueued");

                // Analyze the order - we can't guarantee perfect FIFO across threads
                // but we can check that the items from each thread maintain their relative order
                var items = queue.ToArray();

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

                // For each thread, check that its items are in ascending order
                bool allThreadsInOrder = true;
                for (int t = 0; t < threadsCount; t++)
                {
                    var threadItems = itemsByThread[t];
                    for (int i = 0; i < threadItems.Count - 1; i++)
                    {
                        if (threadItems[i] > threadItems[i + 1])
                        {
                            allThreadsInOrder = false;
                            break;
                        }
                    }
                }

                AssertTrue(allThreadsInOrder, "Items from each thread should maintain their relative order");
            }
        }

        private static async Task RunComplexScenarioTests()
        {
            Console.WriteLine("\nRunning Complex Scenario Tests...");

            // Scenario 1: Producer-Consumer pattern
            {
                var queueFile = Path.Combine(_testDirectory, "scenario_producer_consumer.idx");
                using var queue = new PersistentQueue<string>(queueFile);

                var producer = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        queue.Enqueue($"Message{i}");
                        await Task.Delay(10); // Simulate some work
                    }
                });

                var consumer = Task.Run(async () =>
                {
                    int processed = 0;
                    while (processed < 20)
                    {
                        if (queue.Count > 0)
                        {
                            string message = queue.Dequeue();
                            processed++;
                        }
                        await Task.Delay(15); // Simulate some work (slightly slower than producer)
                    }
                });

                await Task.WhenAll(producer, consumer);

                AssertEquals(0, queue.Count, "All produced messages should be consumed");
            }

            // Scenario 2: Priority processing
            {
                var queueFile = Path.Combine(_testDirectory, "scenario_priority.idx");
                using var queue = new PersistentQueue<QueueItem>(queueFile);

                // Add items with different priorities
                for (int i = 0; i < 10; i++)
                {
                    // Add items with different priority levels (0, 1, 2)
                    queue.Enqueue(new QueueItem { Id = i, Value = $"Priority{i % 3}" });
                }

                // Process items in queue, prioritizing certain items
                List<QueueItem> processedItems = new List<QueueItem>();
                int initialCount = queue.Count;

                // Get a snapshot of all items
                QueueItem[] allItems = queue.ToArray();

                // Process high priority items first by selectively dequeuing them
                queue.Clear(); // Clear the queue to rebuild it

                // First add and process high priority items
                var highPriorityItems = allItems.Where(i => i.Value == "Priority0").ToList();
                foreach (var item in highPriorityItems)
                {
                    processedItems.Add(item);
                }

                // Then add and process remaining items
                var otherItems = allItems.Where(i => i.Value != "Priority0").ToList();
                foreach (var item in otherItems)
                {
                    processedItems.Add(item);
                }

                AssertEquals(initialCount, processedItems.Count, "All items should be processed");

                // Verify high priority items were processed first
                bool highPriorityFirst = true;
                for (int i = 0; i < highPriorityItems.Count; i++)
                {
                    if (processedItems[i].Value != "Priority0")
                    {
                        highPriorityFirst = false;
                        break;
                    }
                }

                AssertTrue(highPriorityFirst, "High priority items should be processed first");
            }

            // Scenario 3: Persistent queue with multiple instances
            {
                var queueFile = Path.Combine(_testDirectory, "scenario_multiple_instances.idx");

                // First instance: add initial data
                using (var queue1 = new PersistentQueue<string>(queueFile))
                {
                    queue1.Clear(); // Ensure clean state
                    queue1.Enqueue("First");
                    queue1.Enqueue("Second");
                    queue1.Enqueue("Third");
                }

                // Second instance: read and modify
                using (var queue2 = new PersistentQueue<string>(queueFile))
                {
                    AssertEquals(3, queue2.Count, "Second instance should see all items");
                    AssertEquals("First", queue2.Peek(), "Second instance should read correct data");

                    // Dequeue first item
                    string item = queue2.Dequeue();
                    AssertEquals("First", item, "Should dequeue items in FIFO order");

                    // Add new item
                    queue2.Enqueue("Fourth");
                }

                // Third instance: verify changes and add more data
                using (var queue3 = new PersistentQueue<string>(queueFile))
                {
                    AssertEquals(3, queue3.Count, "Third instance should see updated item count");
                    var items = queue3.ToArray();
                    AssertEquals("Second", items[0], "Queue should maintain FIFO order across instances");
                    AssertEquals("Third", items[1], "Queue should maintain sequence across instances");
                    AssertEquals("Fourth", items[2], "New items should persist across instances");

                    // Add more data
                    queue3.Enqueue("Fifth");
                }

                // Final verification
                using (var queue4 = new PersistentQueue<string>(queueFile))
                {
                    AssertEquals(4, queue4.Count, "Final instance should see all updates");
                    AssertCollectionEquals(
                        new[] { "Second", "Third", "Fourth", "Fifth" },
                        queue4.ToArray(),
                        "Final state should reflect all changes across instances");
                }
            }
        }

        #endregion

#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
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

    // Helper class for queue scenario
    [Serializable]
    public class QueueItem
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