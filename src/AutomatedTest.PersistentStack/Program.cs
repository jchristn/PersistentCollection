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
    using SerializationHelper;

    class Program
    {
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning disable CS8629 // Nullable value type may be null.
#pragma warning disable CS8602 // Dereference of a possibly null reference.

        private static string _testDirectory = Path.Combine(Path.GetTempPath(), "PersistentStackTest");
        private static int _passedTests = 0;
        private static int _failedTests = 0;
        private static Serializer _Serializer = new Serializer();

        static async Task Main(string[] args)
        {
            Console.WriteLine("PersistentStack Test Program");
            Console.WriteLine($"Test directory: {_testDirectory}");
            Console.WriteLine("-----------------------------------------");

            // Ensure clean test directory
            CleanTestDirectory();

            // Run basic functionality tests
            await RunBasicTests();

            // Run IStack implementation tests
            await RunIStackTests();

            // Run event handler tests
            await RunEventTests();

            // Run exception handling tests
            await RunExceptionTests();

            // Run asynchronous API tests
            await RunAsyncTests();

            // Run concurrent access tests
            await RunConcurrentTests();

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
                var listDir = Path.Combine(_testDirectory, "basic");
                using var list = new PersistentStack<string>(listDir);
                AssertEquals(0, list.Count, "New list should be empty");
                AssertEquals(0L, list.Length, "New list should have zero length");
            }

            // Test Push and Pop
            {
                var listDir = Path.Combine(_testDirectory, "basic");
                using var list = new PersistentStack<string>(listDir);

                list.Push("Item1");
                list.Push("Item2");
                list.Push("Item3");

                AssertEquals(3, list.Count, "Stack should contain 3 items after adding");

                string val = list.Pop();
                AssertEquals(val, "Item3", "Popped item should be Item3");
                val = list.Pop();
                AssertEquals(val, "Item2", "Popped item should be Item2");
                val = list.Pop();
                AssertEquals(val, "Item1", "Popped item should be Item1");
            }

            // Test Add and Count
            {
                var listDir = Path.Combine(_testDirectory, "add_count");
                using var list = new PersistentStack<string>(listDir);

                list.Push("Item1");
                list.Push("Item2");
                list.Push("Item3");

                AssertEquals(3, list.Count, "Stack should contain 3 items after adding");
                AssertTrue(list.Length > 0, "Stack length should be greater than 0 after adding items");
            }

            // Test Get by index
            {
                var listDir = Path.Combine(_testDirectory, "get_index");
                using var list = new PersistentStack<string>(listDir);

                list.Push("Item1");
                list.Push("Item2");
                list.Push("Item3");

                AssertEquals("Item3", list[0], "First item should be 'Item3'");
                AssertEquals("Item2", list[1], "Second item should be 'Item2'");
                AssertEquals("Item1", list[2], "Third item should be 'Item1'");
            }

            // Test Clear
            {
                var listDir = Path.Combine(_testDirectory, "clear");
                using var list = new PersistentStack<string>(listDir);

                list.Push("Item1");
                list.Push("Item2");
                list.Push("Item3");

                list.Clear();

                AssertEquals(0, list.Count, "Stack should be empty after Clear()");
            }

            // Test GetKeys
            {
                var listDir = Path.Combine(_testDirectory, "get_keys");
                using var list = new PersistentStack<string>(listDir);

                string key1 = list.Push("Item1"); // Explicitly use the overload that returns a key
                string key2 = list.Push("Item2"); // Explicitly use the overload that returns a key
                string key3 = list.Push("Item3"); // Explicitly use the overload that returns a key

                var keys = list.GetKeys();

                AssertEquals(3, keys.Count, "GetKeys should return 3 keys");
                AssertTrue(keys.Contains(key1) && keys.Contains(key2) && keys.Contains(key3),
                    "GetKeys should contain all added keys");
            }

            // Test persistence across instances
            {
                var listDir = Path.Combine(_testDirectory, "persistence");
                string key1, key2, key3;

                // First instance adds items
                using (var list1 = new PersistentStack<string>(listDir))
                {
                    key1 = list1.Push("Item1");
                    key2 = list1.Push("Item2");
                    key3 = list1.Push("Item3");

                    AssertEquals(3, list1.Count, "First instance should have 3 items");
                }

                // Second instance reads the same items
                using (var list2 = new PersistentStack<string>(listDir))
                {
                    AssertEquals(3, list2.Count, "Second instance should have 3 items");
                    AssertEquals("Item3", list2[0], "First item should persist");
                    AssertEquals("Item2", list2[1], "Second item should persist");
                    AssertEquals("Item1", list2[2], "Third item should persist");

                    // Test that keys persist correctly too
                    var persistedKeys = list2.GetKeys();
                    AssertTrue(persistedKeys.Contains(key1) && persistedKeys.Contains(key2) && persistedKeys.Contains(key3),
                        "Keys should persist across instances");
                }
            }

            // Test for different data types
            {
                var listDir = Path.Combine(_testDirectory, "data_types");

                // Test with integers
                using (var intStack = new PersistentStack<int>(listDir + "/int"))
                {
                    intStack.Push(10);
                    intStack.Push(20);
                    intStack.Push(30);

                    AssertEquals(3, intStack.Count, "Integer list should have 3 items");
                    AssertEquals(30, intStack[0], "First int should be 30");
                    AssertEquals(20, intStack[1], "Second int should be 20");
                    AssertEquals(10, intStack[2], "Third int should be 10");
                }

                // Test with complex objects
                using (var personStack = new PersistentStack<Person>(listDir + "/person"))
                {
                    personStack.Push(new Person { Name = "Alice", Age = 25 });
                    personStack.Push(new Person { Name = "Bob", Age = 30 });

                    AssertEquals(2, personStack.Count, "Person list should have 2 items");
                    AssertEquals("Bob", personStack[0].Name, "First person should be Bob");
                    AssertEquals(25, personStack[1].Age, "Second person's age should be 25");
                }
            }
        }

        private static async Task RunIStackTests()
        {
            Console.WriteLine("\nRunning IStack Implementation Tests...");

            // Test indexer get/set
            {
                var listDir = Path.Combine(_testDirectory, "indexer");
                using var list = new PersistentStack<string>(listDir);

                list.Push("Item1");
                list.Push("Item2");
                list.Push("Item3");

                // Test indexer get
                AssertEquals("Item2", list[1], "Indexer get should retrieve correct item");
            }

            // Test Contains
            {
                var listDir = Path.Combine(_testDirectory, "contains");
                using var list = new PersistentStack<string>(listDir);

                list.Push("Item1");
                list.Push("Item2");
                list.Push("Item3");

                AssertTrue(list.Contains("Item2"), "Contains should return true for existing Item2");
                AssertTrue(!list.Contains("NonExistentItem"), "Contains should return false for non-existent item");
            }

            // Test CopyTo
            {
                var listDir = Path.Combine(_testDirectory, "copyto");
                using var list = new PersistentStack<string>(listDir);

                list.Push("Item1");
                list.Push("Item2");
                list.Push("Item3");

                string[] array = new string[5];
                list.CopyTo(array, 1);

                AssertEquals(null, array[0], "First element should be null");
                AssertEquals("Item3", array[1], "Second element should be 'Item3'");
                AssertEquals("Item2", array[2], "Third element should be 'Item2'");
                AssertEquals("Item1", array[3], "Fourth element should be 'Item1'");
                AssertEquals(null, array[4], "Fifth element should be null");
            }

            // Test Enumeration using foreach
            {
                var listDir = Path.Combine(_testDirectory, "enumeration");
                using var list = new PersistentStack<string>(listDir);

                list.Push("Item1");
                list.Push("Item2");
                list.Push("Item3");

                Stack<string> items = new Stack<string>();
                foreach (var item in list)
                {
                    items.Push(item);
                }

                AssertCollectionEquals(new[] { "Item1", "Item2", "Item3" }, items, "Enumeration should visit all items in order");
            }
        }

        private static async Task RunEventTests()
        {
            Console.WriteLine("\nRunning Event Handler Tests...");

            // Test DataAdded event
            {
                var listDir = Path.Combine(_testDirectory, "event_added");
                using var list = new PersistentStack<string>(listDir);

                string addedKey = null;
                list.DataAdded += (sender, key) => { addedKey = key; };

                string key = list.Push("TestItem"); // Explicitly use the overload that returns a key

                AssertEquals(key, addedKey, "DataAdded event should provide correct key");
            }

            // Test Cleared event
            {
                var listDir = Path.Combine(_testDirectory, "event_cleared");
                using var list = new PersistentStack<string>(listDir);

                bool cleared = false;
                list.Cleared += (sender, args) => { cleared = true; };

                list.Push("Item1");
                list.Push("Item2");
                list.Clear();

                AssertTrue(cleared, "Cleared event should be raised");
            }

            // Test ExceptionEncountered event
            // This is harder to test directly since it requires creating corrupt data
            // We'll skip this for now
        }

        private static async Task RunExceptionTests()
        {
            Console.WriteLine("\nRunning Exception Handling Tests...");

            // Test constructor with null directory
            try
            {
                var list = new PersistentStack<string>(null);
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
                var listDir = Path.Combine(_testDirectory, "exception_index");
                using var list = new PersistentStack<string>(listDir);

                list.Push("Item1");

                try
                {
                    var item = list[-1];
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for negative index");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for negative index");
                }

                try
                {
                    var item = list[1];
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for out of bounds index");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for out of bounds index");
                }
            }
        }

        private static async Task RunAsyncTests()
        {
            Console.WriteLine("\nRunning Asynchronous API Tests...");

            // Test GetKeysAsync
            {
                var listDir = Path.Combine(_testDirectory, "async_get_keys");
                using var list = new PersistentStack<string>(listDir);

                string key1 = await list.PushAsync("Item1");
                string key2 = await list.PushAsync("Item2");

                var keys = await list.GetKeysAsync();

                AssertEquals(2, keys.Count, "GetKeysAsync should return correct number of keys");
                AssertTrue(keys.Contains(key1) && keys.Contains(key2), "GetKeysAsync should return all keys");
            }
        }

        private static async Task RunConcurrentTests()
        {
            Console.WriteLine("\nRunning Concurrent Access Tests...");

            // Test concurrent reads
            {
                var listDir = Path.Combine(_testDirectory, "concurrent_read");
                using var list = new PersistentStack<int>(listDir);

                // Add some items
                for (int i = 0; i < 10; i++)
                {
                    list.Push(i);
                }

                // Create multiple concurrent read tasks
                var tasks = new Stack<Task>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Push(Task.Run(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            int value = list[j];
                            // No assertion here, just checking it doesn't crash
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertTrue(true, "Concurrent reads completed without exceptions");
            }

            // Test concurrent writes
            {
                var listDir = Path.Combine(_testDirectory, "concurrent_write");
                using var list = new PersistentStack<int>(listDir);

                // Create multiple concurrent write tasks
                var tasks = new Stack<Task>();

                int numTasks = 5;
                int numInsertions = 5;

                for (int i = 0; i < numTasks; i++)
                {
                    int taskId = i;
                    tasks.Push(Task.Run(async () =>
                    {
                        for (int j = 0; j < numInsertions; j++)
                        {
                            int value = taskId * 10 + j;
                            await list.PushAsync(value);
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertEquals((numTasks * numInsertions), list.Count, "All concurrent writes should be completed");
            }

            // Test concurrent mixed operations
            {
                var listDir = Path.Combine(_testDirectory, "concurrent_mixed");
                using var list = new PersistentStack<string>(listDir);

                // Add initial items
                for (int i = 0; i < 20; i++)
                {
                    list.Push($"Item{i}");
                }

                // Perform various operations concurrently
                var tasks = new Stack<Task>();

                // Task 1: Read operations
                tasks.Push(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        if (i < list.Count)
                        {
                            string value = list[i];
                        }
                    }
                }));

                // Task 2: Write operations
                tasks.Push(Task.Run(async () =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await list.PushAsync($"NewItem{i}");
                    }
                }));

                await Task.WhenAll(tasks);

                AssertTrue(list.Count >= 30, "Stack should have at least initial + new items");
            }
        }

        #endregion

#pragma warning restore CS8602 // Dereference of a possibly null reference.
#pragma warning restore CS8629 // Nullable value type may be null.
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
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