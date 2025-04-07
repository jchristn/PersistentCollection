namespace AutomatedTest.PersistentDictionary
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

        private static string _testDirectory = Path.Combine(Path.GetTempPath(), "PersistentDictionaryTest");
        private static int _passedTests = 0;
        private static int _failedTests = 0;

        static async Task Main(string[] args)
        {
            Console.WriteLine("PersistentDictionary Test Program");
            Console.WriteLine($"Test directory: {_testDirectory}");
            Console.WriteLine("-----------------------------------------");

            CleanTestDirectory();
            await RunBasicTests();
            await RunIDictionaryTests();
            await RunPersistenceTests();
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

        private static void AssertCollectionEquals<TKey, TValue>(
            IDictionary<TKey, TValue> expected,
            IDictionary<TKey, TValue> actual,
            string message)
        {
            bool keysEqual = expected.Keys.Count == actual.Keys.Count &&
                             expected.Keys.All(k => actual.ContainsKey(k));

            bool valuesEqual = keysEqual &&
                               expected.Keys.All(k =>
                                   EqualityComparer<TValue>.Default.Equals(expected[k], actual[k]));

            if (keysEqual && valuesEqual)
            {
                _passedTests++;
                Console.WriteLine($"[PASS] {message}");
            }
            else
            {
                _failedTests++;
                Console.WriteLine($"[FAIL] {message}");

                if (!keysEqual)
                {
                    Console.WriteLine($"  Expected keys: [{string.Join(", ", expected.Keys)}]");
                    Console.WriteLine($"  Actual keys: [{string.Join(", ", actual.Keys)}]");
                }
                else
                {
                    var differentValues = expected.Keys.Where(k => !EqualityComparer<TValue>.Default.Equals(expected[k], actual[k]))
                                                 .Select(k => $"{k}={expected[k]}/{actual[k]}");
                    Console.WriteLine($"  Different values: [{string.Join(", ", differentValues)}]");
                }
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
                using var dict = new PersistentDictionary<string, string>(persistenceFile);
                AssertEquals(0, dict.Count, "New dictionary should be empty");
            }

            // Test Add and Count
            {
                var persistenceFile = Path.Combine(_testDirectory, "add_count.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                AssertEquals(3, dict.Count, "Dictionary should contain 3 items after adding");
            }

            // Test Get by key
            {
                var persistenceFile = Path.Combine(_testDirectory, "get_key.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                AssertEquals("Value1", dict["Key1"], "Value for Key1 should be 'Value1'");
                AssertEquals("Value2", dict["Key2"], "Value for Key2 should be 'Value2'");
                AssertEquals("Value3", dict["Key3"], "Value for Key3 should be 'Value3'");
            }

            // Test Update via indexer
            {
                var persistenceFile = Path.Combine(_testDirectory, "update.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                dict["Key2"] = "UpdatedValue2";

                AssertEquals("UpdatedValue2", dict["Key2"], "Updated value should have new value");
            }

            // Test Remove
            {
                var persistenceFile = Path.Combine(_testDirectory, "remove.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                bool result = dict.Remove("Key2");

                AssertTrue(result, "Remove should return true for existing key");
                AssertEquals(2, dict.Count, "Dictionary should contain 2 items after removal");
                AssertTrue(dict.ContainsKey("Key1"), "Key1 should still exist");
                AssertTrue(!dict.ContainsKey("Key2"), "Key2 should be removed");
                AssertTrue(dict.ContainsKey("Key3"), "Key3 should still exist");
            }

            // Test Clear
            {
                var persistenceFile = Path.Combine(_testDirectory, "clear.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                dict.Clear();

                AssertEquals(0, dict.Count, "Dictionary should be empty after Clear()");
            }

            // Test TryGetValue
            {
                var persistenceFile = Path.Combine(_testDirectory, "try_get.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");

                bool result1 = dict.TryGetValue("Key1", out var value1);
                bool result2 = dict.TryGetValue("NonExistent", out var value2);

                AssertTrue(result1, "TryGetValue should return true for existing key");
                AssertTrue(!result2, "TryGetValue should return false for non-existent key");
                AssertEquals("Value1", value1, "TryGetValue should return correct value");
                AssertEquals(null, value2, "TryGetValue should return default value for non-existent key");
            }

            // Test Keys and Values collections
            {
                var persistenceFile = Path.Combine(_testDirectory, "keys_values.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                var keys = dict.Keys.ToList();
                var values = dict.Values.ToList();

                AssertEquals(3, keys.Count, "Keys collection should have 3 items");
                AssertEquals(3, values.Count, "Values collection should have 3 items");

                AssertTrue(keys.Contains("Key1"), "Keys should contain 'Key1'");
                AssertTrue(keys.Contains("Key2"), "Keys should contain 'Key2'");
                AssertTrue(keys.Contains("Key3"), "Keys should contain 'Key3'");

                AssertTrue(values.Contains("Value1"), "Values should contain 'Value1'");
                AssertTrue(values.Contains("Value2"), "Values should contain 'Value2'");
                AssertTrue(values.Contains("Value3"), "Values should contain 'Value3'");
            }
        }

        private static async Task RunIDictionaryTests()
        {
            Console.WriteLine("\nRunning IDictionary Implementation Tests...");

            // Test indexer get/set
            {
                var persistenceFile = Path.Combine(_testDirectory, "indexer.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                // Test indexer get
                AssertEquals("Value2", dict["Key2"], "Indexer get should retrieve correct value");

                // Test indexer set
                dict["Key2"] = "UpdatedValue2";
                AssertEquals("UpdatedValue2", dict["Key2"], "Indexer set should update value");

                // Test indexer add
                dict["Key4"] = "Value4";
                AssertEquals("Value4", dict["Key4"], "Indexer set should add new key-value pair");
                AssertEquals(4, dict.Count, "Count should increment after adding via indexer");
            }

            // Test ContainsKey
            {
                var persistenceFile = Path.Combine(_testDirectory, "contains_key.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                AssertTrue(dict.ContainsKey("Key2"), "ContainsKey should return true for existing key");
                AssertTrue(!dict.ContainsKey("NonExistentKey"), "ContainsKey should return false for non-existent key");
            }

            // Test Contains (KeyValuePair)
            {
                var persistenceFile = Path.Combine(_testDirectory, "contains_kvp.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                var existingPair = new KeyValuePair<string, string>("Key2", "Value2");
                var wrongValuePair = new KeyValuePair<string, string>("Key2", "WrongValue");
                var nonExistentPair = new KeyValuePair<string, string>("NonExistentKey", "Value");

                AssertTrue(dict.Contains(existingPair), "Contains should return true for existing key-value pair");
                AssertTrue(!dict.Contains(wrongValuePair), "Contains should return false for existing key with wrong value");
                AssertTrue(!dict.Contains(nonExistentPair), "Contains should return false for non-existent key");
            }

            // Test Add (KeyValuePair)
            {
                var persistenceFile = Path.Combine(_testDirectory, "add_kvp.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                var pair1 = new KeyValuePair<string, string>("Key1", "Value1");
                var pair2 = new KeyValuePair<string, string>("Key2", "Value2");

                ((ICollection<KeyValuePair<string, string>>)dict).Add(pair1);
                ((ICollection<KeyValuePair<string, string>>)dict).Add(pair2);

                AssertEquals(2, dict.Count, "Dictionary should have 2 items after adding KeyValuePairs");
                AssertEquals("Value1", dict["Key1"], "Value for Key1 should be correct");
                AssertEquals("Value2", dict["Key2"], "Value for Key2 should be correct");
            }

            // Test Remove (KeyValuePair)
            {
                var persistenceFile = Path.Combine(_testDirectory, "remove_kvp.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                var existingPair = new KeyValuePair<string, string>("Key2", "Value2");
                var wrongValuePair = new KeyValuePair<string, string>("Key3", "WrongValue");

                bool result1 = ((ICollection<KeyValuePair<string, string>>)dict).Remove(existingPair);
                bool result2 = ((ICollection<KeyValuePair<string, string>>)dict).Remove(wrongValuePair);

                AssertTrue(result1, "Remove should return true for existing key-value pair");
                AssertTrue(!result2, "Remove should return false for wrong value");
                AssertEquals(2, dict.Count, "Dictionary should have 2 items after removing one key-value pair");
                AssertTrue(!dict.ContainsKey("Key2"), "Key2 should be removed");
                AssertTrue(dict.ContainsKey("Key3"), "Key3 should not be removed due to value mismatch");
            }

            // Test CopyTo
            {
                var persistenceFile = Path.Combine(_testDirectory, "copyto.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                KeyValuePair<string, string>[] array = new KeyValuePair<string, string>[5];
                ((ICollection<KeyValuePair<string, string>>)dict).CopyTo(array, 1);

                AssertTrue(array[0].Equals(default(KeyValuePair<string, string>)), "First element should be default");
                AssertTrue(array[1].Key != null, "Second element should have a key");
                AssertTrue(array[2].Key != null, "Third element should have a key");
                AssertTrue(array[3].Key != null, "Fourth element should have a key");
                AssertTrue(array[4].Equals(default(KeyValuePair<string, string>)), "Fifth element should be default");

                // Verify that all keys and values were copied
                var extractedDict = new Dictionary<string, string>();
                for (int i = 1; i < 4; i++)
                {
                    if (array[i].Key != null)
                    {
                        extractedDict[array[i].Key] = array[i].Value;
                    }
                }

                AssertEquals(3, extractedDict.Count, "Extracted dictionary should have 3 items");
                AssertTrue(extractedDict.ContainsKey("Key1"), "Extracted dictionary should contain Key1");
                AssertTrue(extractedDict.ContainsKey("Key2"), "Extracted dictionary should contain Key2");
                AssertTrue(extractedDict.ContainsKey("Key3"), "Extracted dictionary should contain Key3");
            }

            // Test IsReadOnly property
            {
                var persistenceFile = Path.Combine(_testDirectory, "readonly.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                AssertTrue(!dict.IsReadOnly, "IsReadOnly should be false");
            }

            // Test Enumeration using foreach
            {
                var persistenceFile = Path.Combine(_testDirectory, "enumeration.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("Key1", "Value1");
                dict.Add("Key2", "Value2");
                dict.Add("Key3", "Value3");

                var extractedDict = new Dictionary<string, string>();
                foreach (var kvp in dict)
                {
                    extractedDict[kvp.Key] = kvp.Value;
                }

                AssertEquals(3, extractedDict.Count, "Enumeration should visit all 3 items");
                AssertEquals("Value1", extractedDict["Key1"], "Enumeration should get correct value for Key1");
                AssertEquals("Value2", extractedDict["Key2"], "Enumeration should get correct value for Key2");
                AssertEquals("Value3", extractedDict["Key3"], "Enumeration should get correct value for Key3");
            }
        }

        private static async Task RunPersistenceTests()
        {
            Console.WriteLine("\nRunning Persistence Tests...");

            // Test persistence across instances
            {
                var persistenceFile = Path.Combine(_testDirectory, "persistence.idx");

                // First instance adds items
                using (var dict1 = new PersistentDictionary<string, string>(persistenceFile))
                {
                    dict1.Add("Key1", "Value1");
                    dict1.Add("Key2", "Value2");
                    dict1.Add("Key3", "Value3");

                    AssertEquals(3, dict1.Count, "First instance should have 3 items");
                }

                // Second instance reads the same items
                using (var dict2 = new PersistentDictionary<string, string>(persistenceFile))
                {
                    AssertEquals(3, dict2.Count, "Second instance should have 3 items");
                    AssertEquals("Value1", dict2["Key1"], "First key's value should persist");
                    AssertEquals("Value2", dict2["Key2"], "Second key's value should persist");
                    AssertEquals("Value3", dict2["Key3"], "Third key's value should persist");
                }
            }

            // Test persistence after modifications
            {
                var persistenceFile = Path.Combine(_testDirectory, "persistence_modify.idx");

                // First instance: add initial data
                using (var dict1 = new PersistentDictionary<string, string>(persistenceFile))
                {
                    dict1.Add("Key1", "FirstValue");
                    dict1.Add("Key2", "SecondValue");
                    dict1.Add("Key3", "ThirdValue");
                }

                // Second instance: read and modify
                using (var dict2 = new PersistentDictionary<string, string>(persistenceFile))
                {
                    AssertEquals(3, dict2.Count, "Second instance should see all items");
                    AssertEquals("SecondValue", dict2["Key2"], "Second instance should read correct data");

                    // Modify data
                    dict2["Key2"] = "ModifiedValue";
                    dict2.Remove("Key3");
                    dict2.Add("Key4", "FourthValue");
                }

                // Third instance: verify changes and add more data
                using (var dict3 = new PersistentDictionary<string, string>(persistenceFile))
                {
                    AssertEquals(3, dict3.Count, "Third instance should see updated item count");
                    AssertEquals("FirstValue", dict3["Key1"], "Unchanged items should persist");
                    AssertEquals("ModifiedValue", dict3["Key2"], "Modified items should persist");
                    AssertTrue(!dict3.ContainsKey("Key3"), "Removed items should stay removed");
                    AssertEquals("FourthValue", dict3["Key4"], "New items should persist");

                    // Add more data
                    dict3.Add("Key5", "FifthValue");
                }

                // Final verification
                using (var dict4 = new PersistentDictionary<string, string>(persistenceFile))
                {
                    AssertEquals(4, dict4.Count, "Final instance should see all updates");
                    AssertTrue(dict4.ContainsKey("Key1"), "Key1 should exist");
                    AssertTrue(dict4.ContainsKey("Key2"), "Key2 should exist");
                    AssertTrue(!dict4.ContainsKey("Key3"), "Key3 should not exist");
                    AssertTrue(dict4.ContainsKey("Key4"), "Key4 should exist");
                    AssertTrue(dict4.ContainsKey("Key5"), "Key5 should exist");

                    AssertEquals("FirstValue", dict4["Key1"], "Value for Key1 should be correct");
                    AssertEquals("ModifiedValue", dict4["Key2"], "Value for Key2 should be correct");
                    AssertEquals("FourthValue", dict4["Key4"], "Value for Key4 should be correct");
                    AssertEquals("FifthValue", dict4["Key5"], "Value for Key5 should be correct");
                }
            }

            // Test persistence with both addition and removal
            {
                var persistenceFile = Path.Combine(_testDirectory, "persistence_add_remove.idx");

                // Add initial items
                using (var dict1 = new PersistentDictionary<string, string>(persistenceFile))
                {
                    for (int i = 0; i < 10; i++)
                    {
                        dict1.Add($"Key{i}", $"Value{i}");
                    }
                }

                // Check initial state and modify
                using (var dict2 = new PersistentDictionary<string, string>(persistenceFile))
                {
                    AssertEquals(10, dict2.Count, "Should have 10 initial items");

                    // Remove some items
                    dict2.Remove("Key1");
                    dict2.Remove("Key3");
                    dict2.Remove("Key5");
                    dict2.Remove("Key7");
                    dict2.Remove("Key9");

                    // Add some new items
                    dict2.Add("KeyA", "ValueA");
                    dict2.Add("KeyB", "ValueB");
                    dict2.Add("KeyC", "ValueC");

                    AssertEquals(8, dict2.Count, "Should have 8 items after modifications");
                }

                // Verify final state
                using (var dict3 = new PersistentDictionary<string, string>(persistenceFile))
                {
                    AssertEquals(8, dict3.Count, "Final instance should see correct item count");

                    // Check removed keys
                    AssertTrue(!dict3.ContainsKey("Key1"), "Key1 should be removed");
                    AssertTrue(!dict3.ContainsKey("Key3"), "Key3 should be removed");
                    AssertTrue(!dict3.ContainsKey("Key5"), "Key5 should be removed");
                    AssertTrue(!dict3.ContainsKey("Key7"), "Key7 should be removed");
                    AssertTrue(!dict3.ContainsKey("Key9"), "Key9 should be removed");

                    // Check remaining original keys
                    AssertTrue(dict3.ContainsKey("Key0"), "Key0 should exist");
                    AssertTrue(dict3.ContainsKey("Key2"), "Key2 should exist");
                    AssertTrue(dict3.ContainsKey("Key4"), "Key4 should exist");
                    AssertTrue(dict3.ContainsKey("Key6"), "Key6 should exist");
                    AssertTrue(dict3.ContainsKey("Key8"), "Key8 should exist");

                    // Check new keys
                    AssertTrue(dict3.ContainsKey("KeyA"), "KeyA should exist");
                    AssertTrue(dict3.ContainsKey("KeyB"), "KeyB should exist");
                    AssertTrue(dict3.ContainsKey("KeyC"), "KeyC should exist");
                }
            }
        }

        private static async Task RunConcurrentTests()
        {
            Console.WriteLine("\nRunning Concurrent Access Tests...");

            // Test concurrent reads
            {
                var persistenceFile = Path.Combine(_testDirectory, "concurrent_read.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                // Add some items
                for (int i = 0; i < 10; i++)
                {
                    dict.Add($"Key{i}", $"Value{i}");
                }

                // Create multiple concurrent read tasks
                var tasks = new List<Task>();
                for (int i = 0; i < 5; i++)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            string key = $"Key{j}";
                            if (dict.ContainsKey(key))
                            {
                                string value = dict[key];
                                // No assertion here, just checking it doesn't crash
                            }
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertTrue(true, "Concurrent reads completed without exceptions");
            }

            // Test concurrent writes
            {
                var persistenceFile = Path.Combine(_testDirectory, "concurrent_write.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                // Create multiple concurrent write tasks
                var tasks = new List<Task>();
                for (int i = 0; i < 5; i++)
                {
                    int taskId = i;
                    tasks.Add(Task.Run(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            string key = $"Key_{taskId}_{j}";
                            string value = $"Value_{taskId}_{j}";
                            dict.Add(key, value);
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertEquals(50, dict.Count, "All concurrent writes should be completed");

                // Verify each key-value pair was added correctly
                for (int i = 0; i < 5; i++)
                {
                    for (int j = 0; j < 10; j++)
                    {
                        string key = $"Key_{i}_{j}";
                        string expectedValue = $"Value_{i}_{j}";

                        AssertTrue(dict.ContainsKey(key), $"Dictionary should contain {key}");
                        AssertEquals(expectedValue, dict[key], $"Value for {key} should be {expectedValue}");
                    }
                }
            }

            // Test concurrent mixed operations
            {
                var persistenceFile = Path.Combine(_testDirectory, "concurrent_mixed.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                // Add initial items
                for (int i = 0; i < 10; i++)
                {
                    dict.Add($"InitialKey{i}", $"InitialValue{i}");
                }

                // Perform various operations concurrently
                var tasks = new List<Task>();

                // Task 1: Read operations
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        string key = $"InitialKey{i}";
                        if (dict.ContainsKey(key))
                        {
                            string value = dict[key];
                        }
                    }
                }));

                // Task 2: Write operations
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        string key = $"NewKey{i}";
                        string value = $"NewValue{i}";
                        dict.Add(key, value);
                    }
                }));

                // Task 3: Update operations
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        string key = $"InitialKey{i}";
                        if (dict.ContainsKey(key))
                        {
                            dict[key] = $"UpdatedValue{i}";
                        }
                    }
                }));

                await Task.WhenAll(tasks);

                AssertEquals(20, dict.Count, "Dictionary should have 20 items after all operations");

                // Verify updates
                for (int i = 0; i < 10; i++)
                {
                    string initialKey = $"InitialKey{i}";
                    string newKey = $"NewKey{i}";

                    AssertTrue(dict.ContainsKey(initialKey), $"Dictionary should contain {initialKey}");
                    AssertTrue(dict.ContainsKey(newKey), $"Dictionary should contain {newKey}");

                    // Initial keys should have updated values
                    AssertEquals($"UpdatedValue{i}", dict[initialKey],
                        $"Value for {initialKey} should be updated");

                    // New keys should have their original values
                    AssertEquals($"NewValue{i}", dict[newKey],
                        $"Value for {newKey} should be original");
                }
            }

            // Test concurrent add and remove operations
            {
                var persistenceFile = Path.Combine(_testDirectory, "concurrent_add_remove.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                // Add initial items
                for (int i = 0; i < 20; i++)
                {
                    dict.Add($"Key{i}", $"Value{i}");
                }

                // Create a task that adds new items
                var addTask = Task.Run(() =>
                {
                    for (int i = 0; i < 20; i++)
                    {
                        dict.Add($"NewKey{i}", $"NewValue{i}");
                        Thread.Sleep(5); // Small delay to interleave operations
                    }
                });

                // Create a task that removes items
                var removeTask = Task.Run(() =>
                {
                    for (int i = 0; i < 15; i++)
                    {
                        if (dict.ContainsKey($"Key{i}"))
                        {
                            dict.Remove($"Key{i}");
                            Thread.Sleep(7); // Different delay to create more interleaving
                        }
                    }
                });

                await Task.WhenAll(addTask, removeTask);

                // Verify the dictionary has the expected number of items
                AssertEquals(25, dict.Count, "Dictionary should have 20 original - 15 removes + 20 adds = 25 items");

                // Verify which keys are present
                for (int i = 0; i < 15; i++)
                {
                    AssertTrue(!dict.ContainsKey($"Key{i}"), $"Key{i} should have been removed");
                }

                for (int i = 15; i < 20; i++)
                {
                    AssertTrue(dict.ContainsKey($"Key{i}"), $"Key{i} should still exist");
                }

                for (int i = 0; i < 20; i++)
                {
                    AssertTrue(dict.ContainsKey($"NewKey{i}"), $"NewKey{i} should exist");
                    AssertEquals($"NewValue{i}", dict[$"NewKey{i}"], $"Value for NewKey{i} should be correct");
                }
            }
        }

        private static async Task RunStressTests()
        {
            Console.WriteLine("\nRunning Stress Tests...");

            // Test large number of operations
            {
                var persistenceFile = Path.Combine(_testDirectory, "stress_large.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                // Add a large number of items
                const int itemCount = 100;
                for (int i = 0; i < itemCount; i++)
                {
                    dict.Add($"Key{i}", $"Value{i}");
                }

                AssertEquals(itemCount, dict.Count, $"Dictionary should contain {itemCount} items");

                // Read all items
                for (int i = 0; i < itemCount; i++)
                {
                    AssertEquals($"Value{i}", dict[$"Key{i}"], $"Value at Key{i} should be correct");
                }

                // Remove items from various positions
                dict.Remove($"Key{itemCount - 1}"); // Remove last added
                dict.Remove($"Key0"); // Remove first added
                dict.Remove($"Key{itemCount / 2}"); // Remove from middle

                AssertEquals(itemCount - 3, dict.Count, "Dictionary should have 3 fewer items after removals");
            }

            // Test rapid add/remove cycles
            {
                var persistenceFile = Path.Combine(_testDirectory, "stress_cycles.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                const int cycles = 10;

                // Perform many add/remove cycles
                for (int cycle = 0; cycle < cycles; cycle++)
                {
                    // Add 10 items
                    for (int i = 0; i < 10; i++)
                    {
                        dict.Add($"Cycle{cycle}_Key{i}", $"Cycle{cycle}_Value{i}");
                    }

                    // Remove 5 items
                    for (int i = 0; i < 5; i++)
                    {
                        dict.Remove($"Cycle{cycle}_Key{i}");
                    }
                }

                AssertEquals(cycles * 5, dict.Count, "Dictionary should contain the expected number of items after cycles");

                // Verify correct items remain
                for (int cycle = 0; cycle < cycles; cycle++)
                {
                    for (int i = 0; i < 5; i++)
                    {
                        AssertTrue(!dict.ContainsKey($"Cycle{cycle}_Key{i}"),
                            $"Cycle{cycle}_Key{i} should have been removed");
                    }

                    for (int i = 5; i < 10; i++)
                    {
                        AssertTrue(dict.ContainsKey($"Cycle{cycle}_Key{i}"),
                            $"Cycle{cycle}_Key{i} should still exist");
                        AssertEquals($"Cycle{cycle}_Value{i}", dict[$"Cycle{cycle}_Key{i}"],
                            $"Value for Cycle{cycle}_Key{i} should be correct");
                    }
                }
            }

            // Test mixed concurrent operations with high volume
            {
                var persistenceFile = Path.Combine(_testDirectory, "stress_concurrent.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                const int threadCount = 4;
                const int operationsPerThread = 25;

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
                            string key = $"Thread{threadId}_Key{i}";

                            try
                            {
                                switch (operationType)
                                {
                                    case 0: // Add
                                        if (!dict.ContainsKey(key))
                                        {
                                            dict.Add(key, $"Thread{threadId}_Value{i}");
                                        }
                                        break;

                                    case 1: // Update
                                        if (dict.ContainsKey(key))
                                        {
                                            dict[key] = $"Thread{threadId}_UpdatedValue{i}";
                                        }
                                        else if (random.Next(2) == 0) // Sometimes add via indexer
                                        {
                                            dict[key] = $"Thread{threadId}_Value{i}";
                                        }
                                        break;

                                    case 2: // Read
                                        if (dict.ContainsKey(key))
                                        {
                                            string value = dict[key];
                                        }
                                        break;

                                    case 3: // Remove
                                        if (dict.ContainsKey(key))
                                        {
                                            dict.Remove(key);
                                        }
                                        break;
                                }
                            }
                            catch (Exception ex)
                            {
                                // This is typically not expected, but we'll log it if it happens
                                Console.WriteLine($"Exception in thread {threadId}: {ex.Message}");
                            }
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                // Simply verify that the test completed without throwing unhandled exceptions
                AssertTrue(true, "Mixed concurrent operations completed without unhandled exceptions");

                // Verify that all the items in the dictionary have valid keys and values
                foreach (var pair in dict)
                {
                    string key = pair.Key;
                    string value = pair.Value;

                    // Keys should follow our naming pattern
                    AssertTrue(key.StartsWith("Thread"), "Key should start with 'Thread'");

                    // Values should follow our naming pattern
                    AssertTrue(value.StartsWith("Thread"), "Value should start with 'Thread'");

                    // Extract thread ID from key and check it matches the thread ID in value
                    string[] keyParts = key.Split('_');
                    string[] valueParts = value.Split('_');

                    AssertTrue(keyParts.Length >= 1, "Key should have at least one part");
                    AssertTrue(valueParts.Length >= 1, "Value should have at least one part");

                    AssertEquals(keyParts[0], valueParts[0], "Thread identifier should match between key and value");
                }
            }
        }

        private static async Task RunExceptionTests()
        {
            Console.WriteLine("\nRunning Exception Handling Tests...");

            // Test constructor with null directory
            try
            {
                var dict = new PersistentDictionary<string, string>(null);
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

            // Test key not found exception
            {
                var persistenceFile = Path.Combine(_testDirectory, "exception_key_not_found.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("ExistingKey", "Value");

                try
                {
                    var value = dict["NonExistentKey"];
                    AssertTrue(false, "Should throw KeyNotFoundException for non-existent key");
                }
                catch (KeyNotFoundException)
                {
                    AssertTrue(true, "Correctly threw KeyNotFoundException for non-existent key");
                }
            }

            // Test adding duplicate key
            {
                var persistenceFile = Path.Combine(_testDirectory, "exception_duplicate.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                dict.Add("DuplicateKey", "Value1");

                try
                {
                    dict.Add("DuplicateKey", "Value2");
                    AssertTrue(false, "Should throw ArgumentException for duplicate key");
                }
                catch (ArgumentException)
                {
                    AssertTrue(true, "Correctly threw ArgumentException for duplicate key");
                }
            }

            // Test null key
            {
                var persistenceFile = Path.Combine(_testDirectory, "exception_null_key.idx");
                using var dict = new PersistentDictionary<string, string>(persistenceFile);

                try
                {
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
                    dict.Add(null, "Value");
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
                    AssertTrue(false, "Should throw ArgumentNullException for null key");
                }
                catch (ArgumentNullException)
                {
                    AssertTrue(true, "Correctly threw ArgumentNullException for null key");
                }
            }
        }

        #endregion

#pragma warning restore CS8602 // Dereference of a possibly null reference.
#pragma warning restore CS8629 // Nullable value type may be null.
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
    }
}