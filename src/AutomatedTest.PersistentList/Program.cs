namespace AutomatedTest.PersistentList
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
#pragma warning disable CS8629 // Nullable value type may be null.
#pragma warning disable CS8602 // Dereference of a possibly null reference.

        private static string _testDirectory = Path.Combine(Path.GetTempPath(), "PersistentListTest");
        private static int _passedTests = 0;
        private static int _failedTests = 0;

        static async Task Main(string[] args)
        {
            Console.WriteLine("PersistentList Test Program");
            Console.WriteLine($"Test directory: {_testDirectory}");
            Console.WriteLine("-----------------------------------------");

            CleanTestDirectory();
            await RunBasicTests();
            await RunIListTests();
            await RunEventTests();
            await RunSortingSearchingTests();
            await RunAdditionalFeatureTests();
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
                var listDir = Path.Combine(_testDirectory, "basic");
                using var list = new PersistentList<string>(listDir);
                AssertEquals(0, list.Count, "New list should be empty");
                AssertEquals(0L, list.Length, "New list should have zero length");
            }

            // Test Add and Count
            {
                var listDir = Path.Combine(_testDirectory, "add_count");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                AssertEquals(3, list.Count, "List should contain 3 items after adding");
                AssertTrue(list.Length > 0, "List length should be greater than 0 after adding items");
            }

            // Test Get by index
            {
                var listDir = Path.Combine(_testDirectory, "get_index");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                AssertEquals("Item1", list[0], "First item should be 'Item1'");
                AssertEquals("Item2", list[1], "Second item should be 'Item2'");
                AssertEquals("Item3", list[2], "Third item should be 'Item3'");
            }

            // Test Update
            {
                var listDir = Path.Combine(_testDirectory, "update");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                string key = list.Add("Item2", null); // Explicitly use the overload that returns a key
                list.Add("Item3");

                list.Update(key, "UpdatedItem2");

                AssertEquals("UpdatedItem2", list[1], "Updated item should have new value");
            }

            // Test Remove
            {
                var listDir = Path.Combine(_testDirectory, "remove");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                string key = list.Add("Item2", null); // Explicitly use the overload that returns a key
                list.Add("Item3");

                list.Remove(key);

                AssertEquals(2, list.Count, "List should contain 2 items after removal");
                AssertEquals("Item1", list[0], "First item should still be 'Item1'");
                AssertEquals("Item3", list[1], "Second item should now be 'Item3'");
            }

            // Test Clear
            {
                var listDir = Path.Combine(_testDirectory, "clear");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                list.Clear();

                AssertEquals(0, list.Count, "List should be empty after Clear()");
            }

            // Test GetKeys
            {
                var listDir = Path.Combine(_testDirectory, "get_keys");
                using var list = new PersistentList<string>(listDir);

                string key1 = list.Add("Item1", null); // Explicitly use the overload that returns a key
                string key2 = list.Add("Item2", null); // Explicitly use the overload that returns a key
                string key3 = list.Add("Item3", null); // Explicitly use the overload that returns a key

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
                using (var list1 = new PersistentList<string>(listDir))
                {
                    key1 = list1.Add("Item1");
                    key2 = list1.Add("Item2");
                    key3 = list1.Add("Item3");

                    AssertEquals(3, list1.Count, "First instance should have 3 items");
                }

                // Second instance reads the same items
                using (var list2 = new PersistentList<string>(listDir))
                {
                    AssertEquals(3, list2.Count, "Second instance should have 3 items");
                    AssertEquals("Item1", list2[0], "First item should persist");
                    AssertEquals("Item2", list2[1], "Second item should persist");
                    AssertEquals("Item3", list2[2], "Third item should persist");

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
                using (var intList = new PersistentList<int>(listDir + "/int"))
                {
                    intList.Add(10);
                    intList.Add(20);
                    intList.Add(30);

                    AssertEquals(3, intList.Count, "Integer list should have 3 items");
                    AssertEquals(10, intList[0], "First int should be 10");
                    AssertEquals(20, intList[1], "Second int should be 20");
                    AssertEquals(30, intList[2], "Third int should be 30");
                }

                // Test with complex objects
                using (var personList = new PersistentList<Person>(listDir + "/person"))
                {
                    personList.Add(new Person { Name = "Alice", Age = 25 });
                    personList.Add(new Person { Name = "Bob", Age = 30 });

                    AssertEquals(2, personList.Count, "Person list should have 2 items");
                    AssertEquals("Alice", personList[0].Name, "First person should be Alice");
                    AssertEquals(30, personList[1].Age, "Second person's age should be 30");
                }
            }
        }

        private static async Task RunIListTests()
        {
            Console.WriteLine("\nRunning IList Implementation Tests...");

            // Test indexer get/set
            {
                var listDir = Path.Combine(_testDirectory, "indexer");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                // Test indexer get
                AssertEquals("Item2", list[1], "Indexer get should retrieve correct item");

                // Test indexer set
                list[1] = "UpdatedItem2";
                AssertEquals("UpdatedItem2", list[1], "Indexer set should update item");
            }

            // Test Contains
            {
                var listDir = Path.Combine(_testDirectory, "contains");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                AssertTrue(list.Contains("Item2"), "Contains should return true for existing item");
                AssertTrue(!list.Contains("NonExistentItem"), "Contains should return false for non-existent item");
            }

            // Test IndexOf
            {
                var listDir = Path.Combine(_testDirectory, "indexof");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");
                list.Add("Item2"); // Duplicate item

                AssertEquals(1, list.IndexOf("Item2"), "IndexOf should return index of first occurrence");
                AssertEquals(-1, list.IndexOf("NonExistentItem"), "IndexOf should return -1 for non-existent item");
            }

            // Test Insert
            {
                var listDir = Path.Combine(_testDirectory, "insert");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item3");

                list.Insert(1, "Item2");

                AssertEquals(3, list.Count, "List should have 3 items after insert");
                AssertEquals("Item1", list[0], "First item should still be 'Item1'");
                AssertEquals("Item2", list[1], "Second item should be inserted 'Item2'");
                AssertEquals("Item3", list[2], "Third item should be shifted 'Item3'");
            }

            // Test Insert at beginning
            {
                var listDir = Path.Combine(_testDirectory, "insert_beginning");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item2");
                list.Add("Item3");

                list.Insert(0, "Item1");

                AssertEquals(3, list.Count, "List should have 3 items after insert at beginning");
                AssertEquals("Item1", list[0], "First item should be inserted 'Item1'");
                AssertEquals("Item2", list[1], "Second item should be shifted 'Item2'");
                AssertEquals("Item3", list[2], "Third item should be shifted 'Item3'");
            }

            // Test Insert at end
            {
                var listDir = Path.Combine(_testDirectory, "insert_end");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");

                list.Insert(2, "Item3");

                AssertEquals(3, list.Count, "List should have 3 items after insert at end");
                AssertEquals("Item1", list[0], "First item should still be 'Item1'");
                AssertEquals("Item2", list[1], "Second item should still be 'Item2'");
                AssertEquals("Item3", list[2], "Third item should be inserted 'Item3'");
            }

            // Test RemoveAt
            {
                var listDir = Path.Combine(_testDirectory, "removeat");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                list.RemoveAt(1);

                AssertEquals(2, list.Count, "List should have 2 items after RemoveAt");
                AssertEquals("Item1", list[0], "First item should still be 'Item1'");
                AssertEquals("Item3", list[1], "Second item should now be 'Item3'");
            }

            // Test Remove by value
            {
                var listDir = Path.Combine(_testDirectory, "remove_value");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");
                list.Add("Item2"); // Duplicate

                // Test Remove by value (using IList implementation)
                bool removed = ((IList<string>)list).Remove("Item2");

                AssertTrue(removed, "Remove should return true when item is found and removed");
                AssertEquals(3, list.Count, "List should have 3 items after removing one");
                AssertEquals("Item1", list[0], "First item should still be 'Item1'");
                AssertEquals("Item3", list[1], "Second item should now be 'Item3'");
                AssertEquals("Item2", list[2], "Third item should be the remaining 'Item2'");
            }

            // Test CopyTo
            {
                var listDir = Path.Combine(_testDirectory, "copyto");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                string[] array = new string[5];
                list.CopyTo(array, 1);

                AssertEquals(null, array[0], "First element should be null");
                AssertEquals("Item1", array[1], "Second element should be 'Item1'");
                AssertEquals("Item2", array[2], "Third element should be 'Item2'");
                AssertEquals("Item3", array[3], "Fourth element should be 'Item3'");
                AssertEquals(null, array[4], "Fifth element should be null");
            }

            // Test IsReadOnly property
            {
                var listDir = Path.Combine(_testDirectory, "readonly");
                using var list = new PersistentList<string>(listDir);

                AssertTrue(!list.IsReadOnly, "IsReadOnly should be false");
            }

            // Test Enumeration using foreach
            {
                var listDir = Path.Combine(_testDirectory, "enumeration");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                List<string> items = new List<string>();
                foreach (var item in list)
                {
                    items.Add(item);
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
                using var list = new PersistentList<string>(listDir);

                string addedKey = null;
                list.DataAdded += (sender, key) => { addedKey = key; };

                string key = list.Add("TestItem", null); // Explicitly use the overload that returns a key

                AssertEquals(key, addedKey, "DataAdded event should provide correct key");
            }

            // Test DataRemoved event
            {
                var listDir = Path.Combine(_testDirectory, "event_removed");
                using var list = new PersistentList<string>(listDir);

                string removedKey = null;
                list.DataRemoved += (sender, key) => { removedKey = key; };

                string key = list.Add("TestItem", null); // Explicitly use the overload that returns a key
                list.Remove(key);

                AssertEquals(key, removedKey, "DataRemoved event should provide correct key");
            }

            // Test DataUpdated event
            {
                var listDir = Path.Combine(_testDirectory, "event_updated");
                using var list = new PersistentList<string>(listDir);

                string updatedKey = null;
                list.DataUpdated += (sender, key) => { updatedKey = key; };

                string key = list.Add("TestItem", null); // Explicitly use the overload that returns a key
                list.Update(key, "UpdatedItem");

                AssertEquals(key, updatedKey, "DataUpdated event should provide correct key");
            }

            // Test Cleared event
            {
                var listDir = Path.Combine(_testDirectory, "event_cleared");
                using var list = new PersistentList<string>(listDir);

                bool cleared = false;
                list.Cleared += (sender, args) => { cleared = true; };

                list.Add("Item1");
                list.Add("Item2");
                list.Clear();

                AssertTrue(cleared, "Cleared event should be raised");
            }

            // Test ExceptionEncountered event
            // This is harder to test directly since it requires creating corrupt data
            // We'll skip this for now
        }

        private static async Task RunSortingSearchingTests()
        {
            Console.WriteLine("\nRunning Sorting and Searching Tests...");

            // Test Sort
            {
                var listDir = Path.Combine(_testDirectory, "sort");
                using var list = new PersistentList<int>(listDir);

                list.Add(3);
                list.Add(1);
                list.Add(4);
                list.Add(2);

                list.Sort();

                AssertEquals(1, list[0], "First item after sort should be 1");
                AssertEquals(2, list[1], "Second item after sort should be 2");
                AssertEquals(3, list[2], "Third item after sort should be 3");
                AssertEquals(4, list[3], "Fourth item after sort should be 4");
            }

            // Test Sort with custom comparer
            {
                var listDir = Path.Combine(_testDirectory, "sort_comparer");
                using var list = new PersistentList<string>(listDir);

                list.Add("aaa");
                list.Add("b");
                list.Add("cc");

                // Sort by string length
                list.Sort((x, y) => x.Length.CompareTo(y.Length));

                AssertEquals("b", list[0], "First item after sort should be the shortest");
                AssertEquals("cc", list[1], "Second item after sort should be medium length");
                AssertEquals("aaa", list[2], "Third item after sort should be the longest");
            }

            // Test BinarySearch
            {
                var listDir = Path.Combine(_testDirectory, "binary_search");
                using var list = new PersistentList<int>(listDir);

                list.Add(10);
                list.Add(20);
                list.Add(30);
                list.Add(40);
                list.Add(50);

                int index = list.BinarySearch(30);

                AssertEquals(2, index, "BinarySearch should find element at correct index");
                AssertTrue(list.BinarySearch(35) < 0, "BinarySearch should return negative for missing element");
            }

            // Test Find
            {
                var listDir = Path.Combine(_testDirectory, "find");
                using var list = new PersistentList<Person>(listDir);

                list.Add(new Person { Name = "Alice", Age = 25 });
                list.Add(new Person { Name = "Bob", Age = 30 });
                list.Add(new Person { Name = "Charlie", Age = 35 });

                var found = list.Find(p => p.Age > 30);

                AssertTrue(found != null, "Find should locate an element");
                AssertEquals("Charlie", found.Name, "Find should return correct element");

                var notFound = list.Find(p => p.Age > 40);
                AssertTrue(notFound == null, "Find should return null when no element matches");
            }

            // Test FindIndex
            {
                var listDir = Path.Combine(_testDirectory, "find_index");
                using var list = new PersistentList<int>(listDir);

                list.Add(10);
                list.Add(20);
                list.Add(30);
                list.Add(40);

                int index = list.FindIndex(x => x > 25);

                AssertEquals(2, index, "FindIndex should return correct index");
                AssertEquals(-1, list.FindIndex(x => x > 50), "FindIndex should return -1 when no element matches");
            }

            // Test FindAll
            {
                var listDir = Path.Combine(_testDirectory, "find_all");
                using var list = new PersistentList<int>(listDir);

                list.Add(10);
                list.Add(20);
                list.Add(30);
                list.Add(15);
                list.Add(25);

                var found = list.FindAll(x => x > 15);

                AssertEquals(3, found.Count, "FindAll should return correct number of matches");
                AssertCollectionEquals(new[] { 20, 30, 25 }, found, "FindAll should return all matching elements");
            }

            // Test Exists
            {
                var listDir = Path.Combine(_testDirectory, "exists");
                using var list = new PersistentList<string>(listDir);

                list.Add("apple");
                list.Add("banana");
                list.Add("cherry");

                AssertTrue(list.Exists(s => s.StartsWith("b")), "Exists should return true when element matches");
                AssertTrue(!list.Exists(s => s.StartsWith("d")), "Exists should return false when no element matches");
            }

            // Test Reverse
            {
                var listDir = Path.Combine(_testDirectory, "reverse");
                using var list = new PersistentList<int>(listDir);

                list.Add(1);
                list.Add(2);
                list.Add(3);
                list.Add(4);

                list.Reverse();

                AssertCollectionEquals(new[] { 4, 3, 2, 1 }, list.ToList(), "Reverse should reverse the order of elements");
            }
        }

        private static async Task RunAdditionalFeatureTests()
        {
            Console.WriteLine("\nRunning Additional Feature Tests...");

            // Test AddRange
            {
                var listDir = Path.Combine(_testDirectory, "add_range");
                using var list = new PersistentList<int>(listDir);

                list.AddRange(new[] { 1, 2, 3, 4, 5 });

                AssertEquals(5, list.Count, "AddRange should add all items");
                AssertCollectionEquals(new[] { 1, 2, 3, 4, 5 }, list.ToList(), "AddRange should maintain order");
            }

            // Test InsertRange
            {
                var listDir = Path.Combine(_testDirectory, "insert_range");
                using var list = new PersistentList<int>(listDir);

                list.Add(1);
                list.Add(5);

                list.InsertRange(1, new[] { 2, 3, 4 });

                AssertEquals(5, list.Count, "InsertRange should insert all items");
                AssertCollectionEquals(new[] { 1, 2, 3, 4, 5 }, list.ToList(), "InsertRange should insert at correct position");
            }

            // Test RemoveRange
            {
                var listDir = Path.Combine(_testDirectory, "remove_range");
                using var list = new PersistentList<int>(listDir);

                list.AddRange(new[] { 1, 2, 3, 4, 5 });

                list.RemoveRange(1, 3);

                AssertEquals(2, list.Count, "RemoveRange should remove correct number of items");
                AssertCollectionEquals(new[] { 1, 5 }, list.ToList(), "RemoveRange should remove correct items");
            }

            // Test GetRange
            {
                var listDir = Path.Combine(_testDirectory, "get_range");
                using var list = new PersistentList<int>(listDir);

                list.AddRange(new[] { 10, 20, 30, 40, 50 });

                var subrange = list.GetRange(1, 3);

                AssertEquals(3, subrange.Count, "GetRange should return correct number of items");
                AssertCollectionEquals(new[] { 20, 30, 40 }, subrange, "GetRange should return correct items");
            }

            // Test RemoveAll
            {
                var listDir = Path.Combine(_testDirectory, "remove_all");
                using var list = new PersistentList<int>(listDir);

                list.AddRange(new[] { 1, 2, 3, 4, 5, 6 });

                int removed = list.RemoveAll(x => x % 2 == 0); // Remove even numbers

                AssertEquals(3, removed, "RemoveAll should return correct count of removed items");
                AssertEquals(3, list.Count, "List should have remaining items after RemoveAll");
                AssertCollectionEquals(new[] { 1, 3, 5 }, list.ToList(), "RemoveAll should remove only matching items");
            }

            // Test ForEach
            {
                var listDir = Path.Combine(_testDirectory, "foreach");
                using var list = new PersistentList<int>(listDir);

                list.AddRange(new[] { 1, 2, 3, 4, 5 });

                int sum = 0;
                list.ForEach(x => sum += x);

                AssertEquals(15, sum, "ForEach should execute action on each element");
            }

            // Test ConvertAll
            {
                var listDir = Path.Combine(_testDirectory, "convert_all");
                using var list = new PersistentList<int>(listDir);

                list.AddRange(new[] { 1, 2, 3, 4, 5 });

                var squared = list.ConvertAll(x => x * x);

                AssertCollectionEquals(new[] { 1, 4, 9, 16, 25 }, squared, "ConvertAll should transform all elements");
            }

            // Test TrueForAll
            {
                var listDir = Path.Combine(_testDirectory, "true_for_all");
                using var list = new PersistentList<int>(listDir);

                list.AddRange(new[] { 2, 4, 6, 8 });

                bool allEven = list.TrueForAll(x => x % 2 == 0);
                bool allGreaterThan5 = list.TrueForAll(x => x > 5);

                AssertTrue(allEven, "TrueForAll should return true when all elements match");
                AssertTrue(!allGreaterThan5, "TrueForAll should return false when any element doesn't match");
            }

            // Test AsReadOnly
            {
                var listDir = Path.Combine(_testDirectory, "as_readonly");
                using var list = new PersistentList<string>(listDir);

                list.AddRange(new[] { "one", "two", "three" });

                var readOnly = list.AsReadOnly();

                AssertEquals(3, readOnly.Count, "ReadOnly collection should have same count");
                AssertCollectionEquals(new[] { "one", "two", "three" }, readOnly, "ReadOnly collection should have same elements");

                // Add to original list
                list.Add("four");

                // ReadOnly is a snapshot and doesn't change
                AssertEquals(3, readOnly.Count, "ReadOnly collection should not be affected by changes to original list");
            }

            // Test ToList
            {
                var listDir = Path.Combine(_testDirectory, "to_list");
                using var list = new PersistentList<string>(listDir);

                list.AddRange(new[] { "apple", "banana", "cherry" });

                var standardList = list.ToList();

                AssertCollectionEquals(new[] { "apple", "banana", "cherry" }, standardList, "ToList should create exact copy");

                // Modify the standard list
                standardList.Add("date");

                // Original list should be unchanged
                AssertEquals(3, list.Count, "Original list should be unchanged by modifications to the copy");
            }
        }

        private static async Task RunExceptionTests()
        {
            Console.WriteLine("\nRunning Exception Handling Tests...");

            // Test constructor with null directory
            try
            {
                var list = new PersistentList<string>(null);
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
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");

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

            // Test invalid key access
            {
                var listDir = Path.Combine(_testDirectory, "exception_key");
                using var list = new PersistentList<string>(listDir);

                list.Add("Item1");

                try
                {
                    var data = list.Get("nonexistent-key");
                    AssertTrue(false, "Should throw KeyNotFoundException for nonexistent key");
                }
                catch (KeyNotFoundException)
                {
                    AssertTrue(true, "Correctly threw KeyNotFoundException for nonexistent key");
                }
            }

            // Test null arguments
            {
                var listDir = Path.Combine(_testDirectory, "exception_null");
                using var list = new PersistentList<string>(listDir);

                try
                {
                    list.Update(null, "Value");
                    AssertTrue(false, "Should throw ArgumentNullException for null key");
                }
                catch (ArgumentNullException)
                {
                    AssertTrue(true, "Correctly threw ArgumentNullException for null key");
                }

                try
                {
                    list.Add("Item1");
                    list.Update("nonexistent-key", "Value");
                    AssertTrue(false, "Should throw KeyNotFoundException for nonexistent key in Update");
                }
                catch (KeyNotFoundException)
                {
                    AssertTrue(true, "Correctly threw KeyNotFoundException for nonexistent key in Update");
                }
            }

            // Test invalid range operations
            {
                var listDir = Path.Combine(_testDirectory, "exception_range");
                using var list = new PersistentList<int>(listDir);

                list.AddRange(new[] { 1, 2, 3 });

                try
                {
                    list.GetRange(-1, 2);
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for negative index in GetRange");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for negative index in GetRange");
                }

                try
                {
                    list.GetRange(0, 4);
                    AssertTrue(false, "Should throw ArgumentException for too large count in GetRange");
                }
                catch (ArgumentException)
                {
                    AssertTrue(true, "Correctly threw ArgumentException for too large count in GetRange");
                }
            }
        }

        private static async Task RunAsyncTests()
        {
            Console.WriteLine("\nRunning Asynchronous API Tests...");

            // Test AddAsync
            {
                var listDir = Path.Combine(_testDirectory, "async_add");
                using var list = new PersistentList<string>(listDir);

                string key = await list.AddAsync("AsyncItem");

                AssertEquals(1, list.Count, "AddAsync should add item");
                byte[] data = await list.GetAsync(key);
                string value = Encoding.UTF8.GetString(data);
                AssertTrue(value.Contains("AsyncItem"), "GetByKeyAsync should retrieve correct item");
            }

            // Test UpdateAsync
            {
                var listDir = Path.Combine(_testDirectory, "async_update");
                using var list = new PersistentList<string>(listDir);

                string key = await list.AddAsync("OriginalItem");
                await list.UpdateAsync(key, "UpdatedItem");

                AssertEquals("UpdatedItem", await list.GetByKeyAsync(key), "UpdateAsync should update item");
            }

            // Test GetKeysAsync
            {
                var listDir = Path.Combine(_testDirectory, "async_get_keys");
                using var list = new PersistentList<string>(listDir);

                string key1 = await list.AddAsync("Item1");
                string key2 = await list.AddAsync("Item2");

                var keys = await list.GetKeysAsync();

                AssertEquals(2, keys.Count, "GetKeysAsync should return correct number of keys");
                AssertTrue(keys.Contains(key1) && keys.Contains(key2), "GetKeysAsync should return all keys");
            }

            // Test AddRangeAsync
            {
                var listDir = Path.Combine(_testDirectory, "async_add_range");
                using var list = new PersistentList<int>(listDir);

                await list.AddRangeAsync(new[] { 1, 2, 3, 4, 5 });

                AssertEquals(5, list.Count, "AddRangeAsync should add all items");
                for (int i = 0; i < 5; i++)
                {
                    AssertEquals(i + 1, list[i], $"AddRangeAsync should maintain order for item {i}");
                }
            }

            // Test cancellation token
            {
                var listDir = Path.Combine(_testDirectory, "async_cancel");
                using var list = new PersistentList<int>(listDir);

                var cts = new CancellationTokenSource();
                cts.Cancel(); // Cancel immediately

                try
                {
                    // This should be cancelled
                    await list.AddRangeAsync(Enumerable.Range(1, 20), cts.Token);

                    // If we get here, the operation wasn't cancelled properly
                    AssertTrue(false, "AddRangeAsync should respect cancellation token");
                }
                catch (OperationCanceledException)
                {
                    // This is expected
                    AssertTrue(true, "AddRangeAsync correctly responded to cancellation token");
                }

                // The list should be empty due to cancellation
                AssertTrue(list.Count < 5, "List should have few or no items after cancelled AddRangeAsync");
            }
        }

        private static async Task RunConcurrentTests()
        {
            Console.WriteLine("\nRunning Concurrent Access Tests...");

            // Test concurrent reads
            {
                var listDir = Path.Combine(_testDirectory, "concurrent_read");
                using var list = new PersistentList<int>(listDir);

                // Add some items
                for (int i = 0; i < 100; i++)
                {
                    list.Add(i);
                }

                // Create multiple concurrent read tasks
                var tasks = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        for (int j = 0; j < 100; j++)
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
                using var list = new PersistentList<int>(listDir);

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
                            await list.AddAsync(value);
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertEquals(100, list.Count, "All concurrent writes should be completed");
            }

            // Test concurrent mixed operations
            {
                var listDir = Path.Combine(_testDirectory, "concurrent_mixed");
                using var list = new PersistentList<string>(listDir);

                // Add initial items
                for (int i = 0; i < 20; i++)
                {
                    list.Add($"Item{i}");
                }

                // Perform various operations concurrently
                var tasks = new List<Task>();

                // Task 1: Read operations
                tasks.Add(Task.Run(() =>
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
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await list.AddAsync($"NewItem{i}");
                    }
                }));

                // Task 3: Update operations
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        if (i < list.Count)
                        {
                            list[i] = $"UpdatedItem{i}";
                        }
                    }
                }));

                await Task.WhenAll(tasks);

                AssertTrue(list.Count >= 30, "List should have at least initial + new items");
            }
        }

        private static async Task RunConcurrentOrderTests()
        {
            Console.WriteLine("\nRunning Concurrent Order Preservation Tests...");

            // Test 1: Order preservation with concurrent adds
            {
                var listDir = Path.Combine(_testDirectory, "concurrent_order_add");
                using var list = new PersistentList<int>(listDir);

                // Create multiple concurrent add tasks that add items in sequence from each thread
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
                            // Each thread adds values in its own range
                            // Thread 0: 0-24, Thread 1: 100-124, etc.
                            int value = threadId * 100 + j;
                            string key = list.Add(value, null);
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
                AssertEquals(threadCount * itemsPerThread, list.Count,
                    "All items from all threads should be added");

                // Verify that items from each thread maintain their relative order
                for (int t = 0; t < threadCount; t++)
                {
                    var keysFromThread = allKeysInOrder[t];
                    var valuesInList = new List<int>();

                    // Get values by keys in the order they were added
                    foreach (var key in keysFromThread)
                    {
                        valuesInList.Add(list.GetByKey(key));
                    }

                    // Verify values are in ascending order within each thread's range
                    bool inOrder = true;
                    for (int i = 1; i < valuesInList.Count; i++)
                    {
                        if (valuesInList[i] != valuesInList[i - 1] + 1)
                        {
                            inOrder = false;
                            break;
                        }
                    }

                    AssertTrue(inOrder, $"Items added by thread {t} should maintain their relative order");
                }
            }

            // Test 2: Order preservation with interleaved adds and removes
            {
                var listDir = Path.Combine(_testDirectory, "concurrent_add_remove_order");
                using var list = new PersistentList<int>(listDir);

                // First add a set of ordered items
                for (int i = 0; i < 50; i++)
                {
                    list.Add(i);
                }

                // Create a task that adds items
                var addTask = Task.Run(() =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        list.Add(1000 + i);  // Add items in 1000s
                        Thread.Sleep(5);     // Small delay to interleave operations
                    }
                });

                // Create a task that removes items
                var removeTask = Task.Run(() =>
                {
                    for (int i = 0; i < 15; i++)
                    {
                        // Remove items from the middle
                        if (list.Count > 20)
                        {
                            list.RemoveAt(list.Count / 2);
                            Thread.Sleep(7);  // Different delay to create more interleaving
                        }
                    }
                });

                await Task.WhenAll(addTask, removeTask);

                // Verify the list is still in order
                bool isOrdered = true;
                int previousValue = -1;

                for (int i = 0; i < list.Count; i++)
                {
                    int currentValue = list[i];

                    // The first value doesn't need comparison
                    if (i > 0 && previousValue > currentValue)
                    {
                        isOrdered = false;
                        break;
                    }

                    previousValue = currentValue;
                }

                AssertTrue(isOrdered, "List should maintain order after concurrent adds and removes");
                AssertEquals(55, list.Count, "List should have 50 original items + 20 adds - 15 removes = 55 items");
            }

            // Test 3: Order preservation with multiple threads performing mixed operations
            {
                var listDir = Path.Combine(_testDirectory, "concurrent_mixed_operations_order");
                using var list = new PersistentList<string>(listDir);

                // Add initial items (A0-A49)
                for (int i = 0; i < 50; i++)
                {
                    list.Add($"A{i}");
                }

                // Tasks will perform different operations concurrently
                var tasks = new List<Task>();

                // Task 1: Add B items at the beginning (insert)
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        list.Insert(0, $"B{i}");
                        Thread.Sleep(3);
                    }
                }));

                // Task 2: Add C items at the end
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        list.Add($"C{i}");
                        Thread.Sleep(4);
                    }
                }));

                // Task 3: Remove items from the middle
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 5; i++)
                    {
                        if (list.Count > 20)
                        {
                            int middleIndex = list.Count / 2;
                            list.RemoveAt(middleIndex);
                            Thread.Sleep(5);
                        }
                    }
                }));

                await Task.WhenAll(tasks);

                // Verify category ordering (B's at beginning, A's in middle, C's at end)
                // This verifies that operations from different threads maintain expected ordering

                // Count number of items in each category
                int bCount = 0;
                int aCount = 0;
                int cCount = 0;
                int otherCount = 0;

                // Track if ordering is correct (all B's, then A's, then C's)
                bool seenB = false;
                bool seenA = false;
                bool seenC = false;
                bool orderViolated = false;

                for (int i = 0; i < list.Count; i++)
                {
                    string item = list[i];

                    // Count occurrences
                    if (item.StartsWith("B")) bCount++;
                    else if (item.StartsWith("A")) aCount++;
                    else if (item.StartsWith("C")) cCount++;
                    else otherCount++;

                    // Check order violations
                    if (item.StartsWith("B"))
                    {
                        seenB = true;
                        if (seenA || seenC) orderViolated = true;
                    }
                    else if (item.StartsWith("A"))
                    {
                        seenA = true;
                        if (seenC) orderViolated = true;
                    }
                    else if (item.StartsWith("C"))
                    {
                        seenC = true;
                    }
                }

                AssertTrue(!orderViolated, "B items should precede A items, which should precede C items");
                AssertEquals(10, bCount, "All 10 B items should be in the list");
                AssertEquals(10, cCount, "All 10 C items should be in the list");
                AssertEquals(45, aCount, "45 A items should remain (50 original - 5 removed)");
                AssertEquals(0, otherCount, "No unexpected items should be in the list");
                AssertEquals(65, list.Count, "List should have correct total count after all operations");
            }
        }

        private static async Task RunComplexScenarioTests()
        {
            Console.WriteLine("\nRunning Complex Scenario Tests...");

            // Scenario 1: Producer-Consumer pattern
            {
                var listDir = Path.Combine(_testDirectory, "scenario_producer_consumer");
                using var list = new PersistentList<string>(listDir);

                var producer = Task.Run(async () =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        string key = await list.AddAsync($"Message{i}");
                        await Task.Delay(10); // Simulate some work
                    }
                });

                var consumer = Task.Run(async () =>
                {
                    int processed = 0;
                    while (processed < 20)
                    {
                        if (list.Count > 0)
                        {
                            string message = list[0];
                            list.RemoveAt(0);
                            processed++;
                        }
                        await Task.Delay(15); // Simulate some work (slightly slower than producer)
                    }
                });

                await Task.WhenAll(producer, consumer);

                AssertEquals(0, list.Count, "All produced messages should be consumed");
            }

            // Scenario 2: Persistent object database with CRUD operations
            {
                var listDir = Path.Combine(_testDirectory, "scenario_object_db");
                using var list = new PersistentList<Person>(listDir);

                // Create
                string aliceKey = await list.AddAsync(new Person { Name = "Alice", Age = 25 });
                string bobKey = await list.AddAsync(new Person { Name = "Bob", Age = 30 });
                string charlieKey = await list.AddAsync(new Person { Name = "Charlie", Age = 35 });

                // Read
                var alice = list.GetByKey(aliceKey);
                AssertEquals("Alice", alice.Name, "Retrieved person should have correct name");
                AssertEquals(25, alice.Age, "Retrieved person should have correct age");

                // Update
                alice.Age = 26;
                await list.UpdateAsync(aliceKey, alice);
                var updatedAlice = await list.GetByKeyAsync(aliceKey);
                AssertEquals(26, updatedAlice.Age, "Updated person should have new age");

                // Delete
                list.Remove(bobKey);
                AssertEquals(2, list.Count, "List should have 2 items after removal");

                // Query
                var olderThan30 = list.FindAll(p => p.Age > 30);
                AssertEquals(1, olderThan30.Count, "Query should find 1 person older than 30");
                AssertEquals("Charlie", olderThan30[0].Name, "Query should find Charlie");
            }

            // Scenario 4: Persistent list with multiple instances
            {
                var listDir = Path.Combine(_testDirectory, "scenario_multiple_instances");

                // First instance: add initial data
                string key1, key2, key3;
                using (var list1 = new PersistentList<string>(listDir))
                {
                    list1.Clear(); // Ensure clean state
                    key1 = list1.Add("First", null); // Explicitly use the overload that returns a key
                    key2 = list1.Add("Second", null); // Explicitly use the overload that returns a key
                    key3 = list1.Add("Third", null); // Explicitly use the overload that returns a key
                }

                // Second instance: read and modify
                using (var list2 = new PersistentList<string>(listDir))
                {
                    AssertEquals(3, list2.Count, "Second instance should see all items");
                    AssertEquals("Second", list2[1], "Second instance should read correct data");

                    // Modify data
                    list2.Update(key2, "Modified");
                    list2.Remove(key3);
                    list2.Add("Fourth");
                }

                // Third instance: verify changes and add more data
                using (var list3 = new PersistentList<string>(listDir))
                {
                    AssertEquals(3, list3.Count, "Third instance should see updated item count");
                    AssertEquals("First", list3[0], "Unchanged items should persist");
                    AssertEquals("Modified", list3[1], "Modified items should persist");
                    AssertEquals("Fourth", list3[2], "New items should persist");

                    // Add more data
                    list3.Add("Fifth");
                }

                // Final verification
                using (var list4 = new PersistentList<string>(listDir))
                {
                    AssertEquals(4, list4.Count, "Final instance should see all updates");
                    AssertCollectionEquals(
                        new[] { "First", "Modified", "Fourth", "Fifth" },
                        list4.ToList(),
                        "Final state should reflect all changes across instances");
                }
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
    public class ListItem
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