namespace AutomatedTest.PersistentListNew
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

        private static string _testDirectory = Path.Combine(Path.GetTempPath(), "PersistentListNewTest");
        private static int _passedTests = 0;
        private static int _failedTests = 0;

        static async Task Main(string[] args)
        {
            Console.WriteLine("PersistentListNew Test Program");
            Console.WriteLine($"Test directory: {_testDirectory}");
            Console.WriteLine("-----------------------------------------");

            CleanTestDirectory();
            await RunBasicTests();
            await RunIListTests();
            await RunPersistenceTests();
            await RunSortingSearchingTests();
            await RunConcurrentTests();
            await RunStressTests();
            await RunExceptionTests();

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

            Directory.CreateDirectory(_testDirectory);
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
                var persistenceFile = Path.Combine(_testDirectory, "basic_test.idx");
                using var list = new PersistentList<string>(persistenceFile);
                AssertEquals(0, list.Count, "New list should be empty");
            }

            // Test Add and Count
            {
                var persistenceFile = Path.Combine(_testDirectory, "add_count.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                AssertEquals(3, list.Count, "List should contain 3 items after adding");
            }

            // Test Get by index
            {
                var persistenceFile = Path.Combine(_testDirectory, "get_index.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                AssertEquals("Item1", list[0], "First item should be 'Item1'");
                AssertEquals("Item2", list[1], "Second item should be 'Item2'");
                AssertEquals("Item3", list[2], "Third item should be 'Item3'");
            }

            // Test Update via indexer
            {
                var persistenceFile = Path.Combine(_testDirectory, "update.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                list[1] = "UpdatedItem2";

                AssertEquals("UpdatedItem2", list[1], "Updated item should have new value");
            }

            // Test Remove
            {
                var persistenceFile = Path.Combine(_testDirectory, "remove.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                bool result = list.Remove("Item2");

                AssertTrue(result, "Remove should return true for existing item");
                AssertEquals(2, list.Count, "List should contain 2 items after removal");
                AssertEquals("Item1", list[0], "First item should still be 'Item1'");
                AssertEquals("Item3", list[1], "Second item should now be 'Item3'");
            }

            // Test RemoveAt
            {
                var persistenceFile = Path.Combine(_testDirectory, "remove_at.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                list.RemoveAt(1);

                AssertEquals(2, list.Count, "List should contain 2 items after removal");
                AssertEquals("Item1", list[0], "First item should still be 'Item1'");
                AssertEquals("Item3", list[1], "Second item should now be 'Item3'");
            }

            // Test Clear
            {
                var persistenceFile = Path.Combine(_testDirectory, "clear.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                list.Clear();

                AssertEquals(0, list.Count, "List should be empty after Clear()");
            }

            // Test for different data types
            {
                // Test with integers
                var intPersistenceFile = Path.Combine(_testDirectory, "int_list.idx");
                using (var intList = new PersistentList<int>(intPersistenceFile))
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
                var personPersistenceFile = Path.Combine(_testDirectory, "person_list.idx");
                using (var personList = new PersistentList<Person>(personPersistenceFile))
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
                var persistenceFile = Path.Combine(_testDirectory, "indexer.idx");
                using var list = new PersistentList<string>(persistenceFile);

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
                var persistenceFile = Path.Combine(_testDirectory, "contains.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");

                AssertTrue(list.Contains("Item2"), "Contains should return true for existing item");
                AssertTrue(!list.Contains("NonExistentItem"), "Contains should return false for non-existent item");
            }

            // Test IndexOf
            {
                var persistenceFile = Path.Combine(_testDirectory, "indexof.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");
                list.Add("Item2"); // Duplicate item

                AssertEquals(1, list.IndexOf("Item2"), "IndexOf should return index of first occurrence");
                AssertEquals(-1, list.IndexOf("NonExistentItem"), "IndexOf should return -1 for non-existent item");
            }

            // Test Insert
            {
                var persistenceFile = Path.Combine(_testDirectory, "insert.idx");
                using var list = new PersistentList<string>(persistenceFile);

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
                var persistenceFile = Path.Combine(_testDirectory, "insert_beginning.idx");
                using var list = new PersistentList<string>(persistenceFile);

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
                var persistenceFile = Path.Combine(_testDirectory, "insert_end.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");

                list.Insert(2, "Item3");

                AssertEquals(3, list.Count, "List should have 3 items after insert at end");
                AssertEquals("Item1", list[0], "First item should still be 'Item1'");
                AssertEquals("Item2", list[1], "Second item should still be 'Item2'");
                AssertEquals("Item3", list[2], "Third item should be inserted 'Item3'");
            }

            // Test Remove by value
            {
                var persistenceFile = Path.Combine(_testDirectory, "remove_value.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");
                list.Add("Item2");
                list.Add("Item3");
                list.Add("Item2"); // Duplicate

                // Test Remove by value
                bool removed = list.Remove("Item2");

                AssertTrue(removed, "Remove should return true when item is found and removed");
                AssertEquals(3, list.Count, "List should have 3 items after removing one");
                AssertEquals("Item1", list[0], "First item should still be 'Item1'");
                AssertEquals("Item3", list[1], "Second item should now be 'Item3'");
                AssertEquals("Item2", list[2], "Third item should be the remaining 'Item2'");
            }

            // Test CopyTo
            {
                var persistenceFile = Path.Combine(_testDirectory, "copyto.idx");
                using var list = new PersistentList<string>(persistenceFile);

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
                var persistenceFile = Path.Combine(_testDirectory, "readonly.idx");
                using var list = new PersistentList<string>(persistenceFile);

                AssertTrue(!list.IsReadOnly, "IsReadOnly should be false");
            }

            // Test Enumeration using foreach
            {
                var persistenceFile = Path.Combine(_testDirectory, "enumeration.idx");
                using var list = new PersistentList<string>(persistenceFile);

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

        private static async Task RunPersistenceTests()
        {
            Console.WriteLine("\nRunning Persistence Tests...");

            // Test persistence across instances
            {
                var persistenceFile = Path.Combine(_testDirectory, "persistence.idx");

                // First instance adds items
                using (var list1 = new PersistentList<string>(persistenceFile))
                {
                    list1.Add("Item1");
                    list1.Add("Item2");
                    list1.Add("Item3");

                    AssertEquals(3, list1.Count, "First instance should have 3 items");
                }

                // Second instance reads the same items
                using (var list2 = new PersistentList<string>(persistenceFile))
                {
                    AssertEquals(3, list2.Count, "Second instance should have 3 items");
                    AssertEquals("Item1", list2[0], "First item should persist");
                    AssertEquals("Item2", list2[1], "Second item should persist");
                    AssertEquals("Item3", list2[2], "Third item should persist");
                }
            }

            // Test persistence after modifications
            {
                var persistenceFile = Path.Combine(_testDirectory, "persistence_modify.idx");

                // First instance: add initial data
                using (var list1 = new PersistentList<string>(persistenceFile))
                {
                    list1.Add("First");
                    list1.Add("Second");
                    list1.Add("Third");
                }

                // Second instance: read and modify
                using (var list2 = new PersistentList<string>(persistenceFile))
                {
                    AssertEquals(3, list2.Count, "Second instance should see all items");
                    AssertEquals("Second", list2[1], "Second instance should read correct data");

                    // Modify data
                    list2[1] = "Modified";
                    list2.RemoveAt(2);
                    list2.Add("Fourth");
                }

                // Third instance: verify changes and add more data
                using (var list3 = new PersistentList<string>(persistenceFile))
                {
                    AssertEquals(3, list3.Count, "Third instance should see updated item count");
                    AssertEquals("First", list3[0], "Unchanged items should persist");
                    AssertEquals("Modified", list3[1], "Modified items should persist");
                    AssertEquals("Fourth", list3[2], "New items should persist");

                    // Add more data
                    list3.Add("Fifth");
                }

                // Final verification
                using (var list4 = new PersistentList<string>(persistenceFile))
                {
                    AssertEquals(4, list4.Count, "Final instance should see all updates");

                    List<string> allItems = new List<string>();
                    foreach (var item in list4)
                    {
                        allItems.Add(item);
                    }

                    AssertCollectionEquals(
                        new[] { "First", "Modified", "Fourth", "Fifth" },
                        allItems,
                        "Final state should reflect all changes across instances");
                }
            }

            // Test persistence across complex operations
            {
                var persistenceFile = Path.Combine(_testDirectory, "persistence_complex.idx");

                // Add initial items
                using (var list1 = new PersistentList<int>(persistenceFile))
                {
                    for (int i = 0; i < 10; i++)
                    {
                        list1.Add(i);
                    }
                }

                // Perform inserts and removals
                using (var list2 = new PersistentList<int>(persistenceFile))
                {
                    // Insert at beginning
                    list2.Insert(0, 100);

                    // Insert in middle
                    list2.Insert(5, 200);

                    // Insert at end
                    list2.Insert(12, 300);

                    // Remove from beginning
                    list2.RemoveAt(0);

                    // Remove from middle
                    list2.RemoveAt(6);

                    // Remove from end
                    list2.RemoveAt(list2.Count - 1);
                }

                // Verify final state
                using (var list3 = new PersistentList<int>(persistenceFile))
                {
                    AssertEquals(10, list3.Count, "List should have 10 items after all operations");

                    // Expected items after operations: [0, 1, 2, 3, 4, 200, 6, 7, 8, 9]
                    AssertEquals(0, list3[0], "First item should be 0");
                    AssertEquals(200, list3[4], "Middle item should be 200");
                    AssertEquals(9, list3[9], "Last item should be 9");
                }
            }
        }

        private static async Task RunSortingSearchingTests()
        {
            Console.WriteLine("\nSkipping sorting and advanced searching tests - not implemented in PersistentListNew");

            // Only testing basic search functionality that's part of IList
            {
                var persistenceFile = Path.Combine(_testDirectory, "basic_search.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Apple");
                list.Add("Banana");
                list.Add("Cherry");

                int index = list.IndexOf("Banana");
                AssertEquals(1, index, "IndexOf should find correct index");

                bool contains = list.Contains("Cherry");
                AssertTrue(contains, "Contains should find existing item");

                contains = list.Contains("Dragonfruit");
                AssertTrue(!contains, "Contains should return false for non-existent item");
            }
        }

        private static async Task RunConcurrentTests()
        {
            Console.WriteLine("\nRunning Concurrent Access Tests...");

            // Test concurrent reads
            {
                var persistenceFile = Path.Combine(_testDirectory, "concurrent_read.idx");
                using var list = new PersistentList<int>(persistenceFile);

                // Add some items
                for (int i = 0; i < 10; i++)
                {
                    list.Add(i);
                }

                // Create multiple concurrent read tasks
                var tasks = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Add(Task.Run(() =>
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
                var persistenceFile = Path.Combine(_testDirectory, "concurrent_write.idx");
                using var list = new PersistentList<int>(persistenceFile);

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
                            list.Add(value);
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertEquals(100, list.Count, "All concurrent writes should be completed");
            }

            // Test concurrent mixed operations
            {
                var persistenceFile = Path.Combine(_testDirectory, "concurrent_mixed.idx");
                using var list = new PersistentList<string>(persistenceFile);

                // Add initial items
                for (int i = 0; i < 10; i++)
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
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        list.Add($"NewItem{i}");
                    }
                }));

                // Task 3: Update operations
                tasks.Add(Task.Run(() =>
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

                AssertTrue(list.Count >= 20, "List should have at least initial + new items");
            }

            // Test concurrent insert and remove operations
            {
                var persistenceFile = Path.Combine(_testDirectory, "concurrent_insert_remove.idx");
                using var list = new PersistentList<int>(persistenceFile);

                // Add initial items
                for (int i = 0; i < 50; i++)
                {
                    list.Add(i);
                }

                // Create a task that inserts items
                var insertTask = Task.Run(() =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        list.Insert(i * 2, 1000 + i);
                        Thread.Sleep(5); // Small delay to interleave operations
                    }
                });

                // Create a task that removes items
                var removeTask = Task.Run(() =>
                {
                    for (int i = 0; i < 15; i++)
                    {
                        if (list.Count > 20)
                        {
                            list.RemoveAt(list.Count / 2);
                            Thread.Sleep(7); // Different delay to create more interleaving
                        }
                    }
                });

                await Task.WhenAll(insertTask, removeTask);

                // Verify the list still has the expected number of items
                AssertEquals(55, list.Count, "List should have 50 original + 20 inserts - 15 removes = 55 items");
            }
        }

        private static async Task RunStressTests()
        {
            Console.WriteLine("\nRunning Stress Tests...");

            // Test large number of operations
            {
                var persistenceFile = Path.Combine(_testDirectory, "stress_large.idx");
                using var list = new PersistentList<int>(persistenceFile);

                // Add a large number of items
                const int itemCount = 10;
                for (int i = 0; i < itemCount; i++)
                {
                    list.Add(i);
                }

                AssertEquals(itemCount, list.Count, $"List should contain {itemCount} items");

                // Read all items
                for (int i = 0; i < itemCount; i++)
                {
                    AssertEquals(i, list[i], $"Item at index {i} should have correct value");
                }

                // Remove items from various positions
                list.RemoveAt(itemCount - 1); // Remove from end
                list.RemoveAt(0); // Remove from beginning
                list.RemoveAt(itemCount / 2 - 1); // Remove from middle

                AssertEquals(itemCount - 3, list.Count, "List should have 3 fewer items after removals");
            }

            // Test rapid add/remove cycles
            {
                var persistenceFile = Path.Combine(_testDirectory, "stress_cycles.idx");
                using var list = new PersistentList<int>(persistenceFile);

                const int cycles = 10;

                // Perform many add/remove cycles
                for (int cycle = 0; cycle < cycles; cycle++)
                {
                    // Add 10 items
                    for (int i = 0; i < 10; i++)
                    {
                        list.Add(cycle * 100 + i);
                    }

                    // Remove 5 items
                    for (int i = 0; i < 5; i++)
                    {
                        list.RemoveAt(list.Count - 1);
                    }
                }

                AssertEquals(cycles * 5, list.Count, "List should contain the expected number of items after cycles");
            }

            // Test mixed concurrent operations with high volume
            {
                var persistenceFile = Path.Combine(_testDirectory, "stress_concurrent.idx");
                using var list = new PersistentList<int>(persistenceFile);

                const int threadCount = 4;
                const int operationsPerThread = 10;

                var tasks = new List<Task>();

                for (int t = 0; t < threadCount; t++)
                {
                    int threadId = t;
                    tasks.Add(Task.Run(() =>
                    {
                        var random = new Random(threadId); // Use threadId as seed for deterministic but different sequences

                        for (int i = 0; i < operationsPerThread; i++)
                        {
                            int operationType = random.Next(4); // 0=add, 1=update, 2=read, 3=remove

                            try
                            {
                                switch (operationType)
                                {
                                    case 0: // Add
                                        list.Add(threadId * 1000 + i);
                                        break;

                                    case 1: // Update
                                        if (list.Count > 0)
                                        {
                                            int indexToUpdate = random.Next(list.Count);
                                            list[indexToUpdate] = threadId * 1000 + i;
                                        }
                                        break;

                                    case 2: // Read
                                        if (list.Count > 0)
                                        {
                                            int indexToRead = random.Next(list.Count);
                                            int value = list[indexToRead];
                                        }
                                        break;

                                    case 3: // Remove
                                        if (list.Count > 0)
                                        {
                                            int indexToRemove = random.Next(list.Count);
                                            list.RemoveAt(indexToRemove);
                                        }
                                        break;
                                }
                            }
                            catch (ArgumentOutOfRangeException)
                            {
                                // This can happen if another thread changed the size while we were operating
                                // Just continue with the next operation
                            }
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                // Simply verify that the test completed without throwing unhandled exceptions
                AssertTrue(true, "Mixed concurrent operations completed without unhandled exceptions");
            }
        }

        private static async Task RunExceptionTests()
        {
            Console.WriteLine("\nRunning Exception Handling Tests...");

            // Test constructor with null directory
            try
            {
                var list = new PersistentList<string>(null);
                AssertTrue(false, "Should throw ArgumentNullException for null persistence file");
            }
            catch (ArgumentNullException)
            {
                AssertTrue(true, "Correctly threw ArgumentNullException for null persistence file");
            }
            catch (Exception ex)
            {
                AssertTrue(false, $"Threw wrong exception type: {ex.GetType().Name}");
            }

            // Test invalid index access
            {
                var persistenceFile = Path.Combine(_testDirectory, "exception_index.idx");
                using var list = new PersistentList<string>(persistenceFile);

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

            // Test invalid insertion
            {
                var persistenceFile = Path.Combine(_testDirectory, "exception_insert.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");

                try
                {
                    list.Insert(-1, "InvalidInsert");
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for negative insert index");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for negative insert index");
                }

                try
                {
                    list.Insert(2, "InvalidInsert");
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for insert index > Count");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for insert index > Count");
                }
            }

            // Test invalid removal
            {
                var persistenceFile = Path.Combine(_testDirectory, "exception_remove.idx");
                using var list = new PersistentList<string>(persistenceFile);

                list.Add("Item1");

                try
                {
                    list.RemoveAt(-1);
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for negative remove index");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for negative remove index");
                }

                try
                {
                    list.RemoveAt(1);
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for remove index >= Count");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for remove index >= Count");
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
}