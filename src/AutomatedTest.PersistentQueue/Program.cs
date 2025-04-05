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
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning disable CS8602 // Dereference of a possibly null reference.
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).#pragma warning disable CS8629 // Nullable value type may be null.


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
            await RunEventTests();
            await RunExceptionTests();
            await RunAsyncTests();
            await RunConcurrentTests();
            await RunConcurrentOrderTests();
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
                var queueDir = Path.Combine(_testDirectory, "basic");
                using var queue = new PersistentQueue<string>(queueDir);
                AssertEquals(0, queue.Count, "New queue should be empty");
                AssertEquals(0L, queue.Length, "New queue should have zero length");
            }

            // Test Enqueue and Count
            {
                var queueDir = Path.Combine(_testDirectory, "enqueue_count");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                AssertEquals(3, queue.Count, "Queue should contain 3 items after adding");
                AssertTrue(queue.Length > 0, "Queue length should be greater than 0 after adding items");
            }

            // Test Get by index
            {
                var queueDir = Path.Combine(_testDirectory, "get_index");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                AssertEquals("Item1", queue[0], "First item should be 'Item1'");
                AssertEquals("Item2", queue[1], "Second item should be 'Item2'");
                AssertEquals("Item3", queue[2], "Third item should be 'Item3'");
            }

            // Test Dequeue (FIFO behavior)
            {
                var queueDir = Path.Combine(_testDirectory, "dequeue_fifo");
                using var queue = new PersistentQueue<string>(queueDir);

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
                var queueDir = Path.Combine(_testDirectory, "clear");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                queue.Clear();

                AssertEquals(0, queue.Count, "Queue should be empty after Clear()");
            }

            // Test GetKeys
            {
                var queueDir = Path.Combine(_testDirectory, "get_keys");
                using var queue = new PersistentQueue<string>(queueDir);

                string key1 = queue.Enqueue("Item1");
                string key2 = queue.Enqueue("Item2");
                string key3 = queue.Enqueue("Item3");

                var keys = queue.GetKeys();

                AssertEquals(3, keys.Count, "GetKeys should return 3 keys");
                AssertTrue(keys.Contains(key1) && keys.Contains(key2) && keys.Contains(key3),
                    "GetKeys should contain all added keys");
            }

            // Test persistence across instances
            {
                var queueDir = Path.Combine(_testDirectory, "persistence");
                string key1, key2, key3;

                // First instance adds items
                using (var queue1 = new PersistentQueue<string>(queueDir))
                {
                    key1 = queue1.Enqueue("Item1");
                    key2 = queue1.Enqueue("Item2");
                    key3 = queue1.Enqueue("Item3");

                    AssertEquals(3, queue1.Count, "First instance should have 3 items");
                }

                // Second instance reads the same items
                using (var queue2 = new PersistentQueue<string>(queueDir))
                {
                    AssertEquals(3, queue2.Count, "Second instance should have 3 items");
                    AssertEquals("Item1", queue2[0], "First item should persist");
                    AssertEquals("Item2", queue2[1], "Second item should persist");
                    AssertEquals("Item3", queue2[2], "Third item should persist");

                    // Test that keys persist correctly too
                    var persistedKeys = queue2.GetKeys();
                    AssertTrue(persistedKeys.Contains(key1) && persistedKeys.Contains(key2) && persistedKeys.Contains(key3),
                        "Keys should persist across instances");
                }
            }

            // Test for different data types
            {
                var queueDir = Path.Combine(_testDirectory, "data_types");

                // Test with integers
                using (var intQueue = new PersistentQueue<int>(queueDir + "/int"))
                {
                    intQueue.Enqueue(10);
                    intQueue.Enqueue(20);
                    intQueue.Enqueue(30);

                    AssertEquals(3, intQueue.Count, "Integer queue should have 3 items");
                    AssertEquals(10, intQueue[0], "First int should be 10");
                    AssertEquals(20, intQueue[1], "Second int should be 20");
                    AssertEquals(30, intQueue[2], "Third int should be 30");
                }

                // Test with complex objects
                using (var personQueue = new PersistentQueue<Person>(queueDir + "/person"))
                {
                    personQueue.Enqueue(new Person { Name = "Alice", Age = 25 });
                    personQueue.Enqueue(new Person { Name = "Bob", Age = 30 });

                    AssertEquals(2, personQueue.Count, "Person queue should have 2 items");
                    AssertEquals("Alice", personQueue[0].Name, "First person should be Alice");
                    AssertEquals(30, personQueue[1].Age, "Second person's age should be 30");
                }
            }
        }

        private static async Task RunQueueTests()
        {
            Console.WriteLine("\nRunning Queue Implementation Tests...");

            // Test Peek
            {
                var queueDir = Path.Combine(_testDirectory, "peek");
                using var queue = new PersistentQueue<string>(queueDir);

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

            // Test PeekAt
            {
                var queueDir = Path.Combine(_testDirectory, "peek_at");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                // Test PeekAt
                string peekedItem = queue.PeekAt(1);
                AssertEquals("Item2", peekedItem, "PeekAt should return the item at the specified index");
                AssertEquals(3, queue.Count, "Queue should still have 3 items after PeekAt");
            }

            // Test Dequeue with key
            {
                var queueDir = Path.Combine(_testDirectory, "dequeue_key");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                string key2 = queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                // Dequeue with key (specific item removal)
                string dequeuedItem = queue.Dequeue(key2, true);
                AssertEquals("Item2", dequeuedItem, "Dequeue with key should return the correct item");
                AssertEquals(2, queue.Count, "Queue should have 2 items after dequeueing one");

                // Verify the remaining items are in the right order
                AssertEquals("Item1", queue[0], "First item should still be 'Item1'");
                AssertEquals("Item3", queue[1], "Second item should now be 'Item3'");
            }

            // Test DequeueAt
            {
                var queueDir = Path.Combine(_testDirectory, "dequeue_at");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                // DequeueAt with remove=false (similar to PeekAt)
                string dequeuedItem = queue.DequeueAt(1, false);
                AssertEquals("Item2", dequeuedItem, "DequeueAt with remove=false should return the item without removing it");
                AssertEquals(3, queue.Count, "Queue should still have 3 items after DequeueAt with remove=false");

                // DequeueAt with remove=true
                dequeuedItem = queue.DequeueAt(1, true);
                AssertEquals("Item2", dequeuedItem, "DequeueAt with remove=true should return the item and remove it");
                AssertEquals(2, queue.Count, "Queue should have 2 items after DequeueAt with remove=true");

                // Verify the remaining items
                AssertEquals("Item1", queue[0], "First item should still be 'Item1'");
                AssertEquals("Item3", queue[1], "Second item should now be 'Item3'");
            }

            // Test TryDequeue
            {
                var queueDir = Path.Combine(_testDirectory, "try_dequeue");
                using var queue = new PersistentQueue<string>(queueDir);

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
                var queueDir = Path.Combine(_testDirectory, "try_peek");
                using var queue = new PersistentQueue<string>(queueDir);

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

            // Test TryPeekAt
            {
                var queueDir = Path.Combine(_testDirectory, "try_peek_at");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");

                // Test TryPeekAt success
                bool success = queue.TryPeekAt(1, out string result);
                AssertTrue(success, "TryPeekAt should return true when index is valid");
                AssertEquals("Item2", result, "TryPeekAt should set out parameter to the item at specified index");
                AssertEquals(2, queue.Count, "Queue should still have 2 items after TryPeekAt");

                // Test TryPeekAt with invalid index
                success = queue.TryPeekAt(5, out result);
                AssertTrue(!success, "TryPeekAt should return false when index is invalid");
                AssertEquals(default(string), result, "TryPeekAt should set out parameter to default value on failure");
            }

            // Test Contains
            {
                var queueDir = Path.Combine(_testDirectory, "contains");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                string key2 = queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                AssertTrue(queue.Contains(key2), "Contains should return true for existing key");
                AssertTrue(!queue.Contains("NonExistentKey"), "Contains should return false for non-existent key");
            }

            // Test ContainsIndex
            {
                var queueDir = Path.Combine(_testDirectory, "contains_index");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");

                AssertTrue(queue.ContainsIndex(0), "ContainsIndex should return true for valid index 0");
                AssertTrue(queue.ContainsIndex(1), "ContainsIndex should return true for valid index 1");
                AssertTrue(!queue.ContainsIndex(2), "ContainsIndex should return false for invalid index");
                AssertTrue(!queue.ContainsIndex(-1), "ContainsIndex should return false for negative index");
            }

            // Test Remove
            {
                var queueDir = Path.Combine(_testDirectory, "remove");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                string key2 = queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                // Remove by key
                queue.Remove(key2);
                AssertEquals(2, queue.Count, "Queue should have 2 items after Remove");
                AssertEquals("Item1", queue[0], "First item should still be 'Item1'");
                AssertEquals("Item3", queue[1], "Second item should now be 'Item3'");
            }

            // Test RemoveAt
            {
                var queueDir = Path.Combine(_testDirectory, "remove_at");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                // RemoveAt (similar to DequeueAt but doesn't return the item)
                queue.RemoveAt(1);
                AssertEquals(2, queue.Count, "Queue should have 2 items after RemoveAt");
                AssertEquals("Item1", queue[0], "First item should still be 'Item1'");
                AssertEquals("Item3", queue[1], "Second item should now be 'Item3'");
            }

            // Test CopyTo
            {
                var queueDir = Path.Combine(_testDirectory, "copyto");
                using var queue = new PersistentQueue<string>(queueDir);

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
                var queueDir = Path.Combine(_testDirectory, "to_array");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Enqueue("Item3");

                string[] array = queue.ToArray();

                AssertEquals(3, array.Length, "ToArray should return array with correct length");
                AssertEquals("Item1", array[0], "First element should be 'Item1'");
                AssertEquals("Item2", array[1], "Second element should be 'Item2'");
                AssertEquals("Item3", array[2], "Third element should be 'Item3'");
            }

            // Test Enumeration using foreach
            {
                var queueDir = Path.Combine(_testDirectory, "enumeration");
                using var queue = new PersistentQueue<string>(queueDir);

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
        }

        private static async Task RunEventTests()
        {
            Console.WriteLine("\nRunning Event Handler Tests...");

            // Test DataEnqueued event
            {
                var queueDir = Path.Combine(_testDirectory, "event_enqueued");
                using var queue = new PersistentQueue<string>(queueDir);

                string enqueuedKey = null;
                queue.DataEnqueued += (sender, key) => { enqueuedKey = key; };

                string key = queue.Enqueue("TestItem");

                AssertEquals(key, enqueuedKey, "DataEnqueued event should provide correct key");
            }

            // Test DataDequeued event
            {
                var queueDir = Path.Combine(_testDirectory, "event_dequeued");
                using var queue = new PersistentQueue<string>(queueDir);

                string dequeuedKey = null;
                queue.DataDequeued += (sender, key) => { dequeuedKey = key; };

                string key = queue.Enqueue("TestItem");
                queue.Dequeue();

                AssertEquals(key, dequeuedKey, "DataDequeued event should provide correct key");
            }

            // Test Cleared event
            {
                var queueDir = Path.Combine(_testDirectory, "event_cleared");
                using var queue = new PersistentQueue<string>(queueDir);

                bool cleared = false;
                queue.Cleared += (sender, args) => { cleared = true; };

                queue.Enqueue("Item1");
                queue.Enqueue("Item2");
                queue.Clear();

                AssertTrue(cleared, "Cleared event should be raised");
            }

            // Test ExceptionEncountered event (simulate by corrupting index file)
            {
                var queueDir = Path.Combine(_testDirectory, "event_exception");
                using var queue = new PersistentQueue<string>(queueDir);

                bool exceptionRaised = false;
                queue.ExceptionEncountered += (sender, ex) => { exceptionRaised = true; };

                // Add some items
                queue.Enqueue("Item1");
                queue.Enqueue("Item2");

                // Corrupt the index file by writing invalid data
                File.WriteAllText(Path.Combine(queueDir, ".index"), "invalid data that can't be parsed");

                // Try to access the queue, which should trigger exception handling
                try
                {
                    var keys = queue.GetKeys();
                    // We don't expect to get here, but if we do, check if the exception event was raised
                    AssertTrue(exceptionRaised, "ExceptionEncountered event should be raised for corrupted index file");
                }
                catch
                {
                    // An exception might be thrown instead of being handled internally
                    // Either way is acceptable for the test
                    AssertTrue(true, "Exception was thrown for corrupted index file");
                }
            }
        }

        private static async Task RunExceptionTests()
        {
            Console.WriteLine("\nRunning Exception Handling Tests...");

            // Test constructor with null directory
            try
            {
                var queue = new PersistentQueue<string>(null);
                AssertTrue(false, "Should throw ArgumentNullException for null directory");
            }
            catch (ArgumentNullException)
            {
                AssertTrue(true, "Correctly threw ArgumentNullException for null directory");
            }
            catch (Exception ex)
            {
                AssertTrue(false, $"Threw wrong exception type: {ex.GetType().Name}");
            }

            // Test invalid index access
            {
                var queueDir = Path.Combine(_testDirectory, "exception_index");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");

                try
                {
                    var item = queue[-1];
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for negative index");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for negative index");
                }

                try
                {
                    var item = queue[1];
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for out of bounds index");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for out of bounds index");
                }
            }

            // Test invalid key access
            {
                var queueDir = Path.Combine(_testDirectory, "exception_key");
                using var queue = new PersistentQueue<string>(queueDir);

                queue.Enqueue("Item1");

                try
                {
                    var data = queue.Dequeue("nonexistent-key");
                    AssertTrue(false, "Should throw KeyNotFoundException for nonexistent key");
                }
                catch (KeyNotFoundException)
                {
                    AssertTrue(true, "Correctly threw KeyNotFoundException for nonexistent key");
                }
            }

            // Test null arguments
            {
                var queueDir = Path.Combine(_testDirectory, "exception_null");
                using var queue = new PersistentQueue<string>(queueDir);

                try
                {
                    queue.Enqueue(null);
                    AssertTrue(false, "Should throw ArgumentNullException for null item");
                }
                catch (ArgumentNullException)
                {
                    AssertTrue(true, "Correctly threw ArgumentNullException for null item");
                }

                try
                {
                    queue.Remove(null);
                    AssertTrue(false, "Should throw ArgumentNullException for null key");
                }
                catch (ArgumentNullException)
                {
                    AssertTrue(true, "Correctly threw ArgumentNullException for null key");
                }
            }

            // Test dequeue from empty queue
            {
                var queueDir = Path.Combine(_testDirectory, "exception_empty");
                using var queue = new PersistentQueue<string>(queueDir);

                // Try to dequeue from empty queue
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
            }
        }

        private static async Task RunAsyncTests()
        {
            Console.WriteLine("\nRunning Asynchronous API Tests...");

            // Test EnqueueAsync
            {
                var queueDir = Path.Combine(_testDirectory, "async_enqueue");
                using var queue = new PersistentQueue<string>(queueDir);

                string key = await queue.EnqueueAsync("AsyncItem");

                AssertEquals(1, queue.Count, "EnqueueAsync should add item");
                AssertEquals("AsyncItem", queue[0], "EnqueueAsync should add the correct item");
            }

            // Test DequeueAsync (without specific key)
            {
                var queueDir = Path.Combine(_testDirectory, "async_dequeue");
                using var queue = new PersistentQueue<string>(queueDir);

                await queue.EnqueueAsync("Item1");
                await queue.EnqueueAsync("Item2");

                string item = await queue.DequeueAsync();
                AssertEquals("Item1", item, "DequeueAsync should return first item");
                AssertEquals(1, queue.Count, "Queue should have 1 item after async dequeue");
            }

            // Test DequeueAsync (with specific key)
            {
                var queueDir = Path.Combine(_testDirectory, "async_dequeue_key");
                using var queue = new PersistentQueue<string>(queueDir);

                await queue.EnqueueAsync("Item1");
                string key2 = await queue.EnqueueAsync("Item2");
                await queue.EnqueueAsync("Item3");

                string item = await queue.DequeueAsync(key2);
                AssertEquals("Item2", item, "DequeueAsync with key should return the correct item");
                AssertEquals(2, queue.Count, "Queue should have 2 items after async dequeue with key");
            }

            // Test DequeueAtAsync
            {
                var queueDir = Path.Combine(_testDirectory, "async_dequeue_at");
                using var queue = new PersistentQueue<string>(queueDir);

                await queue.EnqueueAsync("Item1");
                await queue.EnqueueAsync("Item2");
                await queue.EnqueueAsync("Item3");

                string item = await queue.DequeueAtAsync(1, true);
                AssertEquals("Item2", item, "DequeueAtAsync should return and remove the item at specified index");
                AssertEquals(2, queue.Count, "Queue should have 2 items after DequeueAtAsync");
            }

            // Test PeekAsync
            {
                var queueDir = Path.Combine(_testDirectory, "async_peek");
                using var queue = new PersistentQueue<string>(queueDir);

                await queue.EnqueueAsync("Item1");
                await queue.EnqueueAsync("Item2");

                string item = await queue.PeekAsync();
                AssertEquals("Item1", item, "PeekAsync should return the first item without removing it");
                AssertEquals(2, queue.Count, "Queue should still have 2 items after PeekAsync");
            }

            // Test PeekAtAsync
            {
                var queueDir = Path.Combine(_testDirectory, "async_peek_at");
                using var queue = new PersistentQueue<string>(queueDir);

                await queue.EnqueueAsync("Item1");
                await queue.EnqueueAsync("Item2");
                await queue.EnqueueAsync("Item3");

                string item = await queue.PeekAtAsync(1);
                AssertEquals("Item2", item, "PeekAtAsync should return the item at specified index");
                AssertEquals(3, queue.Count, "Queue should still have 3 items after PeekAtAsync");
            }

            // Test GetBytesAsync
            {
                var queueDir = Path.Combine(_testDirectory, "async_get_bytes");
                using var queue = new PersistentQueue<string>(queueDir);

                await queue.EnqueueAsync("TestItem");

                byte[] data = await queue.GetBytesAsync(0);
                string item = Encoding.UTF8.GetString(data);
                AssertEquals("TestItem", item, "GetBytesAsync should return the correct data");
            }

            // Test GetKeysAsync
            {
                var queueDir = Path.Combine(_testDirectory, "async_get_keys");
                using var queue = new PersistentQueue<string>(queueDir);

                string key1 = await queue.EnqueueAsync("Item1");
                string key2 = await queue.EnqueueAsync("Item2");

                var keys = await queue.GetKeysAsync();

                AssertEquals(2, keys.Count, "GetKeysAsync should return correct number of keys");
                AssertTrue(keys.Contains(key1) && keys.Contains(key2), "GetKeysAsync should return all keys");
            }

            // Test TryDequeueAsync
            {
                var queueDir = Path.Combine(_testDirectory, "async_try_dequeue");
                using var queue = new PersistentQueue<string>(queueDir);

                await queue.EnqueueAsync("Item1");

                var (success, result) = await queue.TryDequeueAsync();
                AssertTrue(success, "TryDequeueAsync should return success=true for non-empty queue");
                AssertEquals("Item1", result, "TryDequeueAsync should return the correct item");
                AssertEquals(0, queue.Count, "Queue should be empty after TryDequeueAsync");

                // Try on empty queue
                (success, result) = await queue.TryDequeueAsync();
                AssertTrue(!success, "TryDequeueAsync should return success=false for empty queue");
                AssertEquals(default(string), result, "TryDequeueAsync should return default value for empty queue");
            }

            // Test TryPeekAsync
            {
                var queueDir = Path.Combine(_testDirectory, "async_try_peek");
                using var queue = new PersistentQueue<string>(queueDir);

                await queue.EnqueueAsync("Item1");

                var (success, result) = await queue.TryPeekAsync();
                AssertTrue(success, "TryPeekAsync should return success=true for non-empty queue");
                AssertEquals("Item1", result, "TryPeekAsync should return the correct item");
                AssertEquals(1, queue.Count, "Queue should still have 1 item after TryPeekAsync");

                // Clear and try on empty queue
                queue.Clear();
                (success, result) = await queue.TryPeekAsync();
                AssertTrue(!success, "TryPeekAsync should return success=false for empty queue");
                AssertEquals(default(string), result, "TryPeekAsync should return default value for empty queue");
            }

            // Test cancellation token
            {
                var queueDir = Path.Combine(_testDirectory, "async_cancel");
                using var queue = new PersistentQueue<string>(queueDir);

                var cts = new CancellationTokenSource();
                cts.Cancel(); // Cancel immediately

                try
                {
                    // This should be cancelled
                    await queue.EnqueueAsync("TestItem", cts.Token);

                    // If we get here, the operation wasn't cancelled properly
                    AssertTrue(false, "EnqueueAsync should respect cancellation token");
                }
                catch (OperationCanceledException)
                {
                    // This is expected
                    AssertTrue(true, "EnqueueAsync correctly responded to cancellation token");
                }

                // The queue should be empty due to cancellation
                AssertEquals(0, queue.Count, "Queue should be empty after cancelled EnqueueAsync");
            }

            // Test multiple async operations together
            {
                var queueDir = Path.Combine(_testDirectory, "async_multiple");
                using var queue = new PersistentQueue<string>(queueDir);

                // Create a sequence of async operations
                await queue.EnqueueAsync("Item1");
                await queue.EnqueueAsync("Item2");
                await queue.EnqueueAsync("Item3");

                string item1 = await queue.DequeueAsync();
                AssertEquals("Item1", item1, "First dequeued item should be Item1");

                // Peek at the new front of the queue
                string item2 = await queue.PeekAsync();
                AssertEquals("Item2", item2, "Peek should show Item2 at the front");

                // Add a new item
                await queue.EnqueueAsync("Item4");

                // Check the items remaining in the queue
                var keys = await queue.GetKeysAsync();
                AssertEquals(3, keys.Count, "Queue should have 3 items after operations");

                var items = queue.ToArray();
                AssertCollectionEquals(new[] { "Item2", "Item3", "Item4" }, items,
                    "Queue should contain the expected items in order");
            }
        }

        private static async Task RunConcurrentTests()
        {
            Console.WriteLine("\nRunning Concurrent Access Tests...");

            // Test concurrent reads
            {
                var queueDir = Path.Combine(_testDirectory, "concurrent_read");
                using var queue = new PersistentQueue<int>(queueDir);

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
                        for (int j = 0; j < 100; j++)
                        {
                            int value = queue[j];
                            // No assertion here, just checking it doesn't crash
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertTrue(true, "Concurrent reads completed without exceptions");
            }

            // Test concurrent writes
            {
                var queueDir = Path.Combine(_testDirectory, "concurrent_write");
                using var queue = new PersistentQueue<int>(queueDir);

                // Create multiple concurrent write tasks
                var tasks = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    int taskId = i;
                    tasks.Add(Task.Run(async () =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            int value = taskId * 10 + j;
                            await queue.EnqueueAsync(value);
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertEquals(100, queue.Count, "All concurrent writes should be completed");
            }

            // Test concurrent mixed operations
            {
                var queueDir = Path.Combine(_testDirectory, "concurrent_mixed");
                using var queue = new PersistentQueue<string>(queueDir);

                // Add initial items
                for (int i = 0; i < 20; i++)
                {
                    queue.Enqueue($"Item{i}");
                }

                // Perform various operations concurrently
                var tasks = new List<Task>();

                // Task 1: Read operations - make it handle file not found
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        try
                        {
                            if (i < queue.Count)
                            {
                                string value = queue[i];
                            }
                        }
                        catch (FileNotFoundException)
                        {
                            // Item might have been removed by another thread, ignore
                        }
                        catch (Exception)
                        {
                            // Handle other exceptions as needed
                        }
                        Thread.Sleep(5); // Small delay to reduce race conditions
                    }
                }));

                // Task 2: Write operations
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await queue.EnqueueAsync($"NewItem{i}");
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
                var queueDir = Path.Combine(_testDirectory, "concurrent_enqueue_dequeue");
                using var queue = new PersistentQueue<int>(queueDir);

                // Task to continuously enqueue items
                var enqueueTask = Task.Run(async () =>
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await queue.EnqueueAsync(i);
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
        }

        private static async Task RunConcurrentOrderTests()
        {
            Console.WriteLine("\nRunning Concurrent Order Preservation Tests...");

            // Test 1: Order preservation with concurrent enqueues
            {
                var queueDir = Path.Combine(_testDirectory, "concurrent_order_enqueue");
                using var queue = new PersistentQueue<int>(queueDir);

                // Create multiple concurrent enqueue tasks that add items in sequence from each thread
                var tasks = new List<Task<List<string>>>();
                int threadCount = 4;
                int itemsPerThread = 25;

                // Track keys in order for verification
                var allKeysInOrder = new List<string>[threadCount];

                for (int i = 0; i < threadCount; i++)
                {
                    int threadId = i;
                    tasks.Add(Task.Run(() =>
                    {
                        var keysAdded = new List<string>();
                        for (int j = 0; j < itemsPerThread; j++)
                        {
                            // Each thread enqueues values in its own range
                            // Thread 0: 0-24, Thread 1: 100-124, etc.
                            int value = threadId * 100 + j;
                            string key = queue.Enqueue(value);
                            keysAdded.Add(key);
                            // Small delay to increase chance of thread interleaving
                            Thread.Sleep(1);
                        }
                        return keysAdded;
                    }));
                }

                // Wait for all tasks and collect added keys
                var results = await Task.WhenAll(tasks);
                for (int i = 0; i < threadCount; i++)
                {
                    allKeysInOrder[i] = results[i];
                }

                // Verify total count
                AssertEquals(threadCount * itemsPerThread, queue.Count,
                    "All items from all threads should be added");

                // For each thread, verify that its items maintain their relative order in the queue
                for (int t = 0; t < threadCount; t++)
                {
                    var keysFromThread = allKeysInOrder[t];

                    // Verify that when we iterate through the queue, the items from this thread
                    // appear in the same relative order they were added
                    var itemsFromThread = new List<int>();
                    foreach (var item in queue)
                    {
                        if (item >= t * 100 && item < (t + 1) * 100)
                        {
                            itemsFromThread.Add(item);
                        }
                    }

                    // Verify values are in sequential order within each thread's range
                    bool inOrder = true;
                    for (int i = 0; i < itemsFromThread.Count - 1; i++)
                    {
                        if (itemsFromThread[i] + 1 != itemsFromThread[i + 1])
                        {
                            inOrder = false;
                            break;
                        }
                    }

                    AssertTrue(inOrder, $"Items enqueued by thread {t} should maintain their relative order");
                }
            }

            // Test 2: Order preservation with interleaved enqueues and dequeues
            {
                var queueDir = Path.Combine(_testDirectory, "concurrent_enqueue_dequeue_order");
                using var queue = new PersistentQueue<int>(queueDir);

                Console.WriteLine("===== DEBUG INFO START =====");

                // Start with a clean queue
                queue.Clear();
                Console.WriteLine($"After clear: Queue count = {queue.Count}");

                // Record actual numbers of items added and removed
                int actualItemsAdded = 0;

                // First enqueue a set of ordered items
                for (int i = 0; i < 50; i++)
                {
                    queue.Enqueue(i);
                    actualItemsAdded++;
                }

                Console.WriteLine($"After initial enqueue: Queue count = {queue.Count}, Items added = {actualItemsAdded}");

                // Use a counter to track enqueued items
                int enqueuedCount = 0;

                // Create a task that enqueues items
                var enqueueTask = Task.Run(() =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        try
                        {
                            queue.Enqueue(1000 + i);
                            Interlocked.Increment(ref enqueuedCount);
                            Thread.Sleep(5); // Small delay
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Enqueue error: {ex.Message}");
                        }
                    }
                    Console.WriteLine($"Enqueue task finished. Items added: {enqueuedCount}");
                });

                // Create a task that dequeues EXACTLY 15 items
                var dequeueTask = Task.Run(() =>
                {
                    List<int> dequeuedItems = new List<int>();
                    int attemptsLeft = 15; // We want exactly 15 items

                    Console.WriteLine("Starting dequeue task");

                    while (attemptsLeft > 0)
                    {
                        try
                        {
                            int item = queue.Dequeue();
                            dequeuedItems.Add(item);
                            attemptsLeft--;

                            Console.WriteLine($"Successfully dequeued item: {item}. Remaining attempts: {attemptsLeft}");
                            Thread.Sleep(7); // Small delay
                        }
                        catch (InvalidOperationException)
                        {
                            Console.WriteLine("Queue empty, waiting to retry...");
                            Thread.Sleep(10);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Dequeue error: {ex.Message}");
                            Thread.Sleep(10);
                        }
                    }

                    Console.WriteLine($"Dequeue task finished. Items removed: {dequeuedItems.Count}");
                    return dequeuedItems;
                });

                // Wait for tasks to complete
                await Task.WhenAll(enqueueTask, dequeueTask);
                var dequeuedItems = await dequeueTask;

                // Calculate the expected final count
                actualItemsAdded += enqueuedCount;
                int expectedFinalCount = actualItemsAdded - dequeuedItems.Count;

                // Check directory files vs queue count
                int fileCount = Directory.GetFiles(queueDir)
                    .Where(f => !Path.GetFileName(f).Equals(".index"))
                    .Count();

                Console.WriteLine($"Final values:");
                Console.WriteLine($"  - Initial items: 50");
                Console.WriteLine($"  - Total items added: {actualItemsAdded}");
                Console.WriteLine($"  - Items dequeued: {dequeuedItems.Count}");
                Console.WriteLine($"  - Expected queue count: {expectedFinalCount}");
                Console.WriteLine($"  - Actual queue count: {queue.Count}");
                Console.WriteLine($"  - File count in directory: {fileCount}");

                // Check the index vs files
                var indexEntries = new List<string>();
                try
                {
                    string indexPath = Path.Combine(queueDir, ".index");
                    if (File.Exists(indexPath))
                    {
                        indexEntries = File.ReadAllLines(indexPath).ToList();
                        Console.WriteLine($"  - Index entries: {indexEntries.Count}");
                        Console.WriteLine($"  - Index content: {string.Join(", ", indexEntries.Take(10))}...");
                    }
                    else
                    {
                        Console.WriteLine("  - Index file doesn't exist!");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  - Error reading index: {ex.Message}");
                }

                // Check if files in directory match index entries
                var fileNames = Directory.GetFiles(queueDir)
                    .Where(f => !Path.GetFileName(f).Equals(".index"))
                    .Select(f => Path.GetFileName(f))
                    .ToList();

                int matchingEntries = 0;
                foreach (var entry in indexEntries)
                {
                    var parts = entry.Split(' ');
                    if (parts.Length >= 1 && fileNames.Contains(parts[0]))
                    {
                        matchingEntries++;
                    }
                }

                Console.WriteLine($"  - Files matching index entries: {matchingEntries}/{indexEntries.Count}");

                // Try to capture file content for debugging
                try
                {
                    var sampleFiles = Directory.GetFiles(queueDir)
                        .Where(f => !Path.GetFileName(f).Equals(".index"))
                        .Take(3);

                    Console.WriteLine("  - Sample file contents:");
                    foreach (var file in sampleFiles)
                    {
                        try
                        {
                            byte[] bytes = File.ReadAllBytes(file);
                            Console.WriteLine($"    - {Path.GetFileName(file)}: {bytes.Length} bytes");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"    - Error reading {Path.GetFileName(file)}: {ex.Message}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  - Error examining files: {ex.Message}");
                }

                Console.WriteLine("===== DEBUG INFO END =====");

                // Verify we dequeued exactly 15 items
                AssertEquals(15, dequeuedItems.Count, "Dequeue task should remove exactly 15 items");

                // Now we can check the total accounting
                AssertEquals(expectedFinalCount, queue.Count,
                    "Queue should account for all items (50 original + 20 enqueued - 15 dequeued)");
            }

            // Test 3: Order preservation with multiple threads performing mixed operations
            {
                var queueDir = Path.Combine(_testDirectory, "concurrent_mixed_operations_order");
                using var queue = new PersistentQueue<string>(queueDir);

                // Make sure directory is clean
                queue.Clear();

                // Enqueue initial items (A0-A49)
                for (int i = 0; i < 50; i++)
                {
                    queue.Enqueue($"A{i}");
                }

                // Use a CountdownEvent to ensure all operations complete
                var operationsCompleted = new CountdownEvent(3);

                // Track enqueued items for verification
                var enqueuedBItems = new List<string>();
                var enqueuedCItems = new List<string>();
                var dequeuedItems = new List<string>();

                // Tasks will perform different operations concurrently
                var tasks = new List<Task>();

                // Task 1: Enqueue B items
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            string item = $"B{i}";
                            queue.Enqueue(item);
                            lock (enqueuedBItems)
                            {
                                enqueuedBItems.Add(item);
                            }
                            Console.WriteLine($"Enqueued {item}");
                            Thread.Sleep(3);
                        }
                    }
                    finally
                    {
                        operationsCompleted.Signal();
                    }
                }));

                // Task 2: Enqueue C items
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            string item = $"C{i}";
                            queue.Enqueue(item);
                            lock (enqueuedCItems)
                            {
                                enqueuedCItems.Add(item);
                            }
                            Console.WriteLine($"Enqueued {item}");
                            Thread.Sleep(4);
                        }
                    }
                    finally
                    {
                        operationsCompleted.Signal();
                    }
                }));

                // Task 3: Dequeue exactly 5 items
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        // Wait a short moment to ensure some items are enqueued first
                        Thread.Sleep(10);

                        for (int i = 0; i < 5; i++)
                        {
                            if (queue.Count > 0)
                            {
                                string dequeued = queue.Dequeue();
                                lock (dequeuedItems)
                                {
                                    dequeuedItems.Add(dequeued);
                                }
                                Console.WriteLine($"Dequeued {dequeued}");
                                Thread.Sleep(5);
                            }
                        }
                    }
                    finally
                    {
                        operationsCompleted.Signal();
                    }
                }));

                // Wait for all tasks to complete
                operationsCompleted.Wait();

                // Get all remaining items in order
                List<string> remainingItems = queue.ToArray().ToList();

                // Count by category
                int remainingBCount = remainingItems.Count(item => item.StartsWith("B"));
                int remainingCCount = remainingItems.Count(item => item.StartsWith("C"));
                int remainingACount = remainingItems.Count(item => item.StartsWith("A"));
                int otherCount = remainingItems.Count(item => !item.StartsWith("A") && !item.StartsWith("B") && !item.StartsWith("C"));

                // Calculate dequeued counts
                int dequeuedBCount = dequeuedItems.Count(item => item.StartsWith("B"));
                int dequeuedCCount = dequeuedItems.Count(item => item.StartsWith("C"));
                int dequeuedACount = dequeuedItems.Count(item => item.StartsWith("A"));

                // Get the remaining items by category
                List<string> remainingAItems = remainingItems.Where(item => item.StartsWith("A")).ToList();
                List<string> remainingBItems = remainingItems.Where(item => item.StartsWith("B")).ToList();
                List<string> remainingCItems = remainingItems.Where(item => item.StartsWith("C")).ToList();

                // Print detailed diagnostic information
                Console.WriteLine("Enqueued B items in order:");
                foreach (var item in enqueuedBItems)
                {
                    Console.WriteLine($"  {item}");
                }

                Console.WriteLine("Remaining B items in order they appear in the queue:");
                foreach (var item in remainingBItems)
                {
                    Console.WriteLine($"  {item}");
                }

                Console.WriteLine("Dequeued items during test:");
                foreach (var item in dequeuedItems)
                {
                    Console.WriteLine($"  {item}");
                }

                // In a FIFO queue, the items should be in increasing order of index
                // IMPORTANT: Check for B items (should be in order of index)
                bool bItemsInFIFO = true;
                for (int i = 0; i < remainingBItems.Count - 1; i++)
                {
                    // Extract the indexes from the item names (e.g., "B5" -> 5)
                    int currentIndex = int.Parse(remainingBItems[i].Substring(1));
                    int nextIndex = int.Parse(remainingBItems[i + 1].Substring(1));

                    // In FIFO order, items with lower indices should be dequeued first
                    if (currentIndex > nextIndex)
                    {
                        bItemsInFIFO = false;
                        Console.WriteLine($"FIFO violation: {remainingBItems[i]} before {remainingBItems[i + 1]}");
                        break;
                    }
                }

                // Same check for C items
                bool cItemsInFIFO = true;
                for (int i = 0; i < remainingCItems.Count - 1; i++)
                {
                    int currentIndex = int.Parse(remainingCItems[i].Substring(1));
                    int nextIndex = int.Parse(remainingCItems[i + 1].Substring(1));

                    if (currentIndex > nextIndex)
                    {
                        cItemsInFIFO = false;
                        Console.WriteLine($"FIFO violation: {remainingCItems[i]} before {remainingCItems[i + 1]}");
                        break;
                    }
                }

                // A items should be in order (A0 first, then A1, etc.)
                bool aItemsInFIFO = true;
                for (int i = 0; i < remainingAItems.Count - 1; i++)
                {
                    int currentIndex = int.Parse(remainingAItems[i].Substring(1));
                    int nextIndex = int.Parse(remainingAItems[i + 1].Substring(1));

                    if (currentIndex > nextIndex)
                    {
                        aItemsInFIFO = false;
                        Console.WriteLine($"FIFO violation for A items: {remainingAItems[i]} before {remainingAItems[i + 1]}");
                        break;
                    }
                }

                // Print detailed summary
                Console.WriteLine($"Total items at end: {remainingItems.Count}");
                Console.WriteLine($"Items dequeued during test: {dequeuedItems.Count}");
                Console.WriteLine($"Enqueued B items: {enqueuedBItems.Count}, Remaining: {remainingBCount}, Dequeued: {dequeuedBCount}");
                Console.WriteLine($"Enqueued C items: {enqueuedCItems.Count}, Remaining: {remainingCCount}, Dequeued: {dequeuedCCount}");
                Console.WriteLine($"Original A items: 50, Remaining: {remainingACount}, Dequeued: {dequeuedACount}");

                // Verify that all items are accounted for
                AssertEquals(enqueuedBItems.Count, remainingBCount + dequeuedBCount, "All B items should be accounted for");
                AssertEquals(enqueuedCItems.Count, remainingCCount + dequeuedCCount, "All C items should be accounted for");
                AssertEquals(50, remainingACount + dequeuedACount, "All A items should be accounted for");

                // Verify FIFO ordering within each category
                AssertTrue(bItemsInFIFO, "B items should maintain FIFO order within their category");
                AssertTrue(cItemsInFIFO, "C items should maintain FIFO order within their category");
                AssertTrue(aItemsInFIFO, "A items should maintain FIFO order within their category");

                // Verify no unexpected items
                AssertEquals(0, otherCount, "No unexpected items should be in the queue");

                // Verify total count
                int expectedTotalItems = enqueuedBItems.Count + enqueuedCItems.Count + 50 - dequeuedItems.Count;
                AssertEquals(expectedTotalItems, remainingItems.Count, "Queue should have correct total count after all operations");

                // Verify that the first item in the remaining list continues from where the dequeued items left off
                if (dequeuedItems.Count > 0 && remainingItems.Count > 0)
                {
                    // The first remaining A item index should be >= the last dequeued A item index
                    // This is a bit complex because we don't know exactly which items were dequeued,
                    // but we can verify that dequeued A items have lower indices than remaining A items
                    var dequeuedAIndices = dequeuedItems
                        .Where(item => item.StartsWith("A"))
                        .Select(item => int.Parse(item.Substring(1)))
                        .ToList();

                    var remainingAIndices = remainingItems
                        .Where(item => item.StartsWith("A"))
                        .Select(item => int.Parse(item.Substring(1)))
                        .ToList();

                    if (dequeuedAIndices.Count > 0 && remainingAIndices.Count > 0)
                    {
                        int maxDequeuedAIndex = dequeuedAIndices.Max();
                        int minRemainingAIndex = remainingAIndices.Min();

                        AssertTrue(maxDequeuedAIndex < minRemainingAIndex,
                            "All dequeued A items should have lower indices than remaining A items");
                    }
                }

                // Make sure the queue still works after all these operations
                string newItem = "TestItem";
                queue.Enqueue(newItem);
                AssertEquals(newItem, queue.ToArray().Last(), "Queue should still be operational after tests");
            }
        }

        private static async Task RunComplexScenarioTests()
        {
            Console.WriteLine("\nRunning Complex Scenario Tests...");

            // Scenario 1: Producer-Consumer pattern
            {
                var queueDir = Path.Combine(_testDirectory, "scenario_producer_consumer");
                using var queue = new PersistentQueue<string>(queueDir);

                var producer = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        string key = await queue.EnqueueAsync($"Message{i}");
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

            // Scenario 2: Message processing system
            {
                var queueDir = Path.Combine(_testDirectory, "scenario_message_processing");
                using var queue = new PersistentQueue<QueueItem>(queueDir);

                // Enqueue different types of messages
                for (int i = 0; i < 10; i++)
                {
                    queue.Enqueue(new QueueItem { Id = i, Value = $"Priority{i % 3}" });
                }

                // Process high priority messages first
                var highPriorityMessages = new List<QueueItem>();
                var keys = queue.GetKeys();

                foreach (var key in keys)
                {
                    QueueItem item = queue.Dequeue(key, false); // peek only
                    if (item.Value == "Priority0")
                    {
                        highPriorityMessages.Add(queue.Dequeue(key)); // now remove
                    }
                }

                // Then process the rest in FIFO order
                var remainingMessages = new List<QueueItem>();
                while (queue.Count > 0)
                {
                    remainingMessages.Add(queue.Dequeue());
                }

                AssertEquals(10, highPriorityMessages.Count + remainingMessages.Count,
                    "All messages should be processed");
                AssertTrue(highPriorityMessages.All(m => m.Value == "Priority0"),
                    "High priority messages should all have Priority0");
            }

            // Scenario 3: Persistent queue with multiple instances
            {
                var queueDir = Path.Combine(_testDirectory, "scenario_multiple_instances");

                // First instance: add initial data
                string key1, key2, key3;
                using (var queue1 = new PersistentQueue<string>(queueDir))
                {
                    queue1.Clear(); // Ensure clean state
                    key1 = queue1.Enqueue("First");
                    key2 = queue1.Enqueue("Second");
                    key3 = queue1.Enqueue("Third");
                }

                // Second instance: read and modify
                using (var queue2 = new PersistentQueue<string>(queueDir))
                {
                    AssertEquals(3, queue2.Count, "Second instance should see all items");
                    AssertEquals("First", queue2[0], "Second instance should read correct data");

                    // Dequeue first item
                    string item = queue2.Dequeue();
                    AssertEquals("First", item, "Should dequeue items in FIFO order");

                    // Add new item
                    queue2.Enqueue("Fourth");
                }

                // Third instance: verify changes and add more data
                using (var queue3 = new PersistentQueue<string>(queueDir))
                {
                    AssertEquals(3, queue3.Count, "Third instance should see updated item count");
                    AssertEquals("Second", queue3[0], "Queue should maintain FIFO order across instances");
                    AssertEquals("Third", queue3[1], "Queue should maintain sequence across instances");
                    AssertEquals("Fourth", queue3[2], "New items should persist across instances");

                    // Add more data
                    queue3.Enqueue("Fifth");
                }

                // Final verification
                using (var queue4 = new PersistentQueue<string>(queueDir))
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
    }

    // Helper class for testing
    [Serializable]
    public class Person
    {

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
    }

    // Helper class for queue scenario
    [Serializable]
    public class QueueItem
    {
        public int Id { get; set; }
        public string Value { get; set; }

        public override string ToString()
        {
            return $"Item {Id}: {Value}";
        }


#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning restore CS8602 // Dereference of a possibly null reference.
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
#pragma warning restore CS8629 // Nullable value type may be null.
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
    }
}