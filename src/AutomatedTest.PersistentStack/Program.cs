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
#pragma warning disable CS8602 // Dereference of a possibly null reference.
#pragma warning disable CS8604 // Possible null reference argument.
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
#pragma warning disable CS8629 // Nullable value type may be null.
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).

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

            // Run sorting and searching tests
            await RunStackSpecificTests();

            // Run additional feature tests
            await RunAdditionalFeatureTests();

            // Run exception handling tests
            await RunExceptionTests();

            // Run asynchronous API tests
            await RunAsyncTests();

            // Run concurrent access tests
            await RunConcurrentTests();

            // Run combined tests with complex scenarios
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
                var stackDir = Path.Combine(_testDirectory, "basic");
                using var stack = new PersistentStack<string>(stackDir);
                AssertEquals(0, stack.Count, "New stack should be empty");
                AssertEquals(0L, stack.Length, "New stack should have zero length");
            }

            // Test Push and Count
            {
                var stackDir = Path.Combine(_testDirectory, "push_count");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                AssertEquals(3, stack.Count, "Stack should contain 3 items after pushing");
                AssertTrue(stack.Length > 0, "Stack length should be greater than 0 after pushing items");
            }

            // Test Push and Pop
            {
                var stackDir = Path.Combine(_testDirectory, "push_pop");
                using var stack = new PersistentStack<string>(stackDir);

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
                var stackDir = Path.Combine(_testDirectory, "peek");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                AssertEquals("Item3", stack.Peek(), "Peek should return the top item without removing it");
                AssertEquals(3, stack.Count, "Stack count should remain unchanged after peek");

                string item = stack.Pop();
                AssertEquals("Item3", item, "Pop should return the same item as peek");
                AssertEquals("Item2", stack.Peek(), "Peek should now return the new top item");
            }

            // Test PeekAt
            {
                var stackDir = Path.Combine(_testDirectory, "peekat");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                AssertEquals("Item3", stack.PeekAt(0), "PeekAt(0) should return the top item");
                AssertEquals("Item2", stack.PeekAt(1), "PeekAt(1) should return the second item");
                AssertEquals("Item1", stack.PeekAt(2), "PeekAt(2) should return the third item");
                AssertEquals(3, stack.Count, "Stack count should remain unchanged after PeekAt");
            }

            // Test Get by index
            {
                var stackDir = Path.Combine(_testDirectory, "get_index");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                AssertEquals("Item3", stack[0], "First item (index 0) should be 'Item3'");
                AssertEquals("Item2", stack[1], "Second item (index 1) should be 'Item2'");
                AssertEquals("Item1", stack[2], "Third item (index 2) should be 'Item1'");
            }

            // Test Clear
            {
                var stackDir = Path.Combine(_testDirectory, "clear");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                stack.Clear();

                AssertEquals(0, stack.Count, "Stack should be empty after Clear()");
            }

            // Test GetKeys
            {
                var stackDir = Path.Combine(_testDirectory, "get_keys");
                using var stack = new PersistentStack<string>(stackDir);

                string key1 = stack.Push("Item1");
                string key2 = stack.Push("Item2");
                string key3 = stack.Push("Item3");

                var keys = stack.GetKeys();

                AssertEquals(3, keys.Count, "GetKeys should return 3 keys");
                AssertTrue(keys.Contains(key1) && keys.Contains(key2) && keys.Contains(key3),
                    "GetKeys should contain all added keys");
            }

            // Test persistence across instances
            {
                var stackDir = Path.Combine(_testDirectory, "persistence");
                string key1, key2, key3;

                // First instance adds items
                using (var stack1 = new PersistentStack<string>(stackDir))
                {
                    key1 = stack1.Push("Item1");
                    key2 = stack1.Push("Item2");
                    key3 = stack1.Push("Item3");

                    AssertEquals(3, stack1.Count, "First instance should have 3 items");
                }

                // Second instance reads the same items
                using (var stack2 = new PersistentStack<string>(stackDir))
                {
                    AssertEquals(3, stack2.Count, "Second instance should have 3 items");
                    AssertEquals("Item3", stack2[0], "First item should persist");
                    AssertEquals("Item2", stack2[1], "Second item should persist");
                    AssertEquals("Item1", stack2[2], "Third item should persist");

                    // Test that keys persist correctly too
                    var persistedKeys = stack2.GetKeys();
                    AssertTrue(persistedKeys.Contains(key1) && persistedKeys.Contains(key2) && persistedKeys.Contains(key3),
                        "Keys should persist across instances");
                }
            }

            // Test for different data types
            {
                var stackDir = Path.Combine(_testDirectory, "data_types");

                // Test with integers
                using (var intStack = new PersistentStack<int>(stackDir + "/int"))
                {
                    intStack.Push(10);
                    intStack.Push(20);
                    intStack.Push(30);

                    AssertEquals(3, intStack.Count, "Integer stack should have 3 items");
                    AssertEquals(30, intStack[0], "First int should be 30");
                    AssertEquals(20, intStack[1], "Second int should be 20");
                    AssertEquals(10, intStack[2], "Third int should be 10");
                }

                // Test with complex objects
                using (var personStack = new PersistentStack<Person>(stackDir + "/person"))
                {
                    personStack.Push(new Person { Name = "Alice", Age = 25 });
                    personStack.Push(new Person { Name = "Bob", Age = 30 });

                    AssertEquals(2, personStack.Count, "Person stack should have 2 items");
                    AssertEquals("Bob", personStack[0].Name, "First person should be Bob");
                    AssertEquals(25, personStack[1].Age, "Second person's age should be 25");
                }
            }
        }

        private static async Task RunIStackTests()
        {
            Console.WriteLine("\nRunning IStack Implementation Tests...");

            // Test indexer get
            {
                var stackDir = Path.Combine(_testDirectory, "indexer");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                // Test indexer get
                AssertEquals("Item2", stack[1], "Indexer get should retrieve correct item");
            }

            // Test TryPeek
            {
                var stackDir = Path.Combine(_testDirectory, "try_peek");
                using var stack = new PersistentStack<string>(stackDir);

                // Test on empty stack
                bool success = stack.TryPeek(out string emptyResult);
                AssertTrue(!success, "TryPeek on empty stack should return false");
                AssertEquals(default, emptyResult, "TryPeek on empty stack should set out parameter to default value");

                // Add items and test
                stack.Push("Item1");
                stack.Push("Item2");

                success = stack.TryPeek(out string result);
                AssertTrue(success, "TryPeek on non-empty stack should return true");
                AssertEquals("Item2", result, "TryPeek should return the top item");
                AssertEquals(2, stack.Count, "TryPeek should not change stack count");
            }

            // Test TryPop
            {
                var stackDir = Path.Combine(_testDirectory, "try_pop");
                using var stack = new PersistentStack<string>(stackDir);

                // Test on empty stack
                bool success = stack.TryPop(out string emptyResult);
                AssertTrue(!success, "TryPop on empty stack should return false");
                AssertEquals(default, emptyResult, "TryPop on empty stack should set out parameter to default value");

                // Add items and test
                stack.Push("Item1");
                stack.Push("Item2");

                success = stack.TryPop(out string result);
                AssertTrue(success, "TryPop on non-empty stack should return true");
                AssertEquals("Item2", result, "TryPop should return the top item");
                AssertEquals(1, stack.Count, "TryPop should reduce stack count");
            }

            // Test TryPeekAt
            {
                var stackDir = Path.Combine(_testDirectory, "try_peekat");
                using var stack = new PersistentStack<string>(stackDir);

                // Test on empty stack
                bool success = stack.TryPeekAt(0, out string emptyResult);
                AssertTrue(!success, "TryPeekAt on empty stack should return false");
                AssertEquals(default, emptyResult, "TryPeekAt on empty stack should set out parameter to default value");

                // Add items and test
                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                success = stack.TryPeekAt(1, out string result);
                AssertTrue(success, "TryPeekAt on valid index should return true");
                AssertEquals("Item2", result, "TryPeekAt should return the correct item");
                AssertEquals(3, stack.Count, "TryPeekAt should not change stack count");

                // Test with invalid index
                success = stack.TryPeekAt(5, out string invalidResult);
                AssertTrue(!success, "TryPeekAt with invalid index should return false");
                AssertEquals(default, invalidResult, "TryPeekAt with invalid index should set out parameter to default value");
            }

            // Test Contains
            {
                var stackDir = Path.Combine(_testDirectory, "contains");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                AssertTrue(stack.Contains("Item2"), "Contains should return true for existing item");
                AssertTrue(!stack.Contains("NonExistentItem"), "Contains should return false for non-existent item");
            }

            // Test ContainsKey
            {
                var stackDir = Path.Combine(_testDirectory, "contains_key");
                using var stack = new PersistentStack<string>(stackDir);

                string key1 = stack.Push("Item1");
                string key2 = stack.Push("Item2");

                AssertTrue(stack.ContainsKey(key1), "ContainsKey should return true for existing key");
                AssertTrue(stack.ContainsKey(key2), "ContainsKey should return true for existing key");
                AssertTrue(!stack.ContainsKey("nonexistent-key"), "ContainsKey should return false for non-existent key");
                AssertTrue(!stack.ContainsKey(null), "ContainsKey should return false for null key");
            }

            // Test CopyTo
            {
                var stackDir = Path.Combine(_testDirectory, "copyto");
                using var stack = new PersistentStack<string>(stackDir);

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
                var stackDir = Path.Combine(_testDirectory, "toarray");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                string[] array = stack.ToArray();

                AssertEquals(3, array.Length, "ToArray should return array with correct length");
                AssertEquals("Item3", array[0], "First element should be 'Item3'");
                AssertEquals("Item2", array[1], "Second element should be 'Item2'");
                AssertEquals("Item1", array[2], "Third element should be 'Item1'");
            }

            // Test Enumeration using foreach
            {
                var stackDir = Path.Combine(_testDirectory, "enumeration");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                List<string> items = new List<string>();
                foreach (var item in stack)
                {
                    items.Add(item);
                }

                AssertCollectionEquals(new[] { "Item3", "Item2", "Item1" }, items, "Enumeration should visit all items in stack order (top to bottom)");
            }
        }

        private static async Task RunEventTests()
        {
            Console.WriteLine("\nRunning Event Handler Tests...");

            // Test DataAdded event
            {
                var stackDir = Path.Combine(_testDirectory, "event_added");
                using var stack = new PersistentStack<string>(stackDir);

                string addedKey = null;
                stack.DataAdded += (sender, key) => { addedKey = key; };

                string key = stack.Push("TestItem");

                AssertEquals(key, addedKey, "DataAdded event should provide correct key");
            }

            // Test DataRemoved event
            {
                var stackDir = Path.Combine(_testDirectory, "event_removed");
                using var stack = new PersistentStack<string>(stackDir);

                string removedKey = null;
                stack.DataRemoved += (sender, key) => { removedKey = key; };

                string key = stack.Push("TestItem");
                stack.Pop(); // This should trigger DataRemoved event

                AssertEquals(key, removedKey, "DataRemoved event should provide correct key");
            }

            // Test DataUpdated event
            {
                var stackDir = Path.Combine(_testDirectory, "event_updated");
                using var stack = new PersistentStack<string>(stackDir);

                string updatedKey = null;
                stack.DataUpdated += (sender, key) => { updatedKey = key; };

                string key = stack.Push("TestItem");
                stack.UpdateByKey(key, "UpdatedItem");

                AssertEquals(key, updatedKey, "DataUpdated event should provide correct key");
            }

            // Test Cleared event
            {
                var stackDir = Path.Combine(_testDirectory, "event_cleared");
                using var stack = new PersistentStack<string>(stackDir);

                bool cleared = false;
                stack.Cleared += (sender, args) => { cleared = true; };

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Clear();

                AssertTrue(cleared, "Cleared event should be raised");
            }

            // Test ExceptionEncountered event
            // This is harder to test directly since it requires creating corrupt data
            // We'll skip this for now
        }

        private static async Task RunStackSpecificTests()
        {
            Console.WriteLine("\nRunning Stack-Specific Tests...");

            // Test LIFO behavior with mixed data types
            {
                var stackDir = Path.Combine(_testDirectory, "lifo_behavior");
                using var stack = new PersistentStack<string>(stackDir);

                // Instead of using object type, use string for all items
                stack.Push("String Item");
                stack.Push("42");
                stack.Push("Person:Alice:30");

                // Verify LIFO order with string parsing
                string personStr = stack.Pop();
                AssertTrue(personStr.StartsWith("Person:"), "First popped item should be the Person string");
                AssertTrue(personStr.Contains("Alice"), "First popped item should contain 'Alice'");

                string numberStr = stack.Pop();
                AssertEquals("42", numberStr, "Second popped item should be the number string");

                string str = stack.Pop();
                AssertEquals("String Item", str, "Last popped item should be the string");
            }

            // Test PopAt
            {
                var stackDir = Path.Combine(_testDirectory, "pop_at");
                using var stack = new PersistentStack<string>(stackDir);
                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");
                stack.Push("Item4");

                // Pop from the middle, removing the item
                string item = stack.PopAt(1, true);
                AssertEquals("Item3", item, "PopAt(1) should return the second item from the top");
                AssertEquals(3, stack.Count, "Stack should have one less item after PopAt with remove=true");

                // Instead of accessing by index, just pop all items and check their order
                string item1 = stack.Pop();
                string item2 = stack.Pop();
                string item3 = stack.Pop();

                AssertEquals("Item4", item1, "First popped item should be 'Item4'");
                AssertEquals("Item2", item2, "Second popped item should be 'Item2'");
                AssertEquals("Item1", item3, "Third popped item should be 'Item1'");
                AssertEquals(0, stack.Count, "Stack should be empty after popping all items");
            }

            // Test UpdateAt
            {
                var stackDir = Path.Combine(_testDirectory, "update_at");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                // Update the middle item
                stack.UpdateAt(1, "UpdatedItem2");

                AssertEquals("UpdatedItem2", stack[1], "UpdateAt should update the item at the specified index");
                AssertEquals(3, stack.Count, "Stack count should remain unchanged after UpdateAt");
            }

            // Test RemoveAt
            {
                var stackDir = Path.Combine(_testDirectory, "remove_at");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");
                stack.Push("Item3");

                // Remove the middle item
                stack.RemoveAt(1);

                AssertEquals(2, stack.Count, "Stack should have one less item after RemoveAt");
                AssertEquals("Item3", stack[0], "Top item should still be 'Item3'");
                AssertEquals("Item1", stack[1], "Second item should now be 'Item1'");
            }

            // Test ContainsIndex
            {
                var stackDir = Path.Combine(_testDirectory, "contains_index");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");
                stack.Push("Item2");

                AssertTrue(stack.ContainsIndex(0), "ContainsIndex should return true for index 0");
                AssertTrue(stack.ContainsIndex(1), "ContainsIndex should return true for index 1");
                AssertTrue(!stack.ContainsIndex(2), "ContainsIndex should return false for out-of-range index");
                AssertTrue(!stack.ContainsIndex(-1), "ContainsIndex should return false for negative index");
            }
        }

        private static async Task RunAdditionalFeatureTests()
        {
            Console.WriteLine("\nRunning Additional Feature Tests...");

            // Test GetKeyByIndex (private method test through public API)
            {
                var stackDir = Path.Combine(_testDirectory, "get_key_by_index");
                using var stack = new PersistentStack<string>(stackDir);

                string key1 = stack.Push("Item1");
                string key2 = stack.Push("Item2");
                string key3 = stack.Push("Item3");

                // Get the key indirectly by updating at index
                stack.UpdateAt(0, "UpdatedItem3");
                stack.UpdateAt(1, "UpdatedItem2");
                stack.UpdateAt(2, "UpdatedItem1");

                // Verify the updates worked by key
                AssertEquals("UpdatedItem3", stack.Peek(), "Update at index 0 should affect the top item");

                // Pop to verify all items were updated correctly
                AssertEquals("UpdatedItem3", stack.Pop(), "First pop should return updated top item");
                AssertEquals("UpdatedItem2", stack.Pop(), "Second pop should return updated middle item");
                AssertEquals("UpdatedItem1", stack.Pop(), "Third pop should return updated bottom item");
            }

            // Test serialization of complex objects
            {
                var stackDir = Path.Combine(_testDirectory, "complex_serialization");
                using var stack = new PersistentStack<Person>(stackDir);

                var person1 = new Person { Name = "Alice", Age = 25 };
                var person2 = new Person { Name = "Bob", Age = 30 };

                stack.Push(person1);
                stack.Push(person2);

                // Pop and verify integrity
                var poppedPerson2 = stack.Pop();
                AssertEquals("Bob", poppedPerson2.Name, "Popped complex object should maintain property values");
                AssertEquals(30, poppedPerson2.Age, "Popped complex object should maintain property values");

                var poppedPerson1 = stack.Pop();
                AssertEquals("Alice", poppedPerson1.Name, "Popped complex object should maintain property values");
                AssertEquals(25, poppedPerson1.Age, "Popped complex object should maintain property values");
            }

            // Test custom serializer
            {
                var stackDir = Path.Combine(_testDirectory, "custom_serializer");
                var customSerializer = new Serializer(); // In a real test, this would be a custom implementation

                using var stack = new PersistentStack<Person>(stackDir, customSerializer);

                stack.Push(new Person { Name = "Alice", Age = 25 });

                var popped = stack.Pop();
                AssertEquals("Alice", popped.Name, "Stack with custom serializer should correctly serialize/deserialize");
            }

            // Test Clear on Dispose
            {
                var stackDir = Path.Combine(_testDirectory, "clear_on_dispose");
                string key1, key2;

                // Create a stack that clears on dispose
                using (var stack = new PersistentStack<string>(stackDir, true))
                {
                    key1 = stack.Push("Item1");
                    key2 = stack.Push("Item2");

                    AssertEquals(2, stack.Count, "Stack should have items before dispose");
                }

                // Create a new stack with the same directory and verify it's empty
                using (var stack = new PersistentStack<string>(stackDir))
                {
                    AssertEquals(0, stack.Count, "Stack should be empty after previous instance was disposed with clearOnDispose=true");
                }
            }
        }

        private static async Task RunExceptionTests()
        {
            Console.WriteLine("\nRunning Exception Handling Tests...");

            // Test constructor with null directory
            try
            {
                var stack = new PersistentStack<string>(null);
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

            // Test constructor with null serializer
            try
            {
                var stack = new PersistentStack<string>(_testDirectory + "/null_serializer", null as Serializer);
                AssertTrue(false, "Should throw ArgumentNullException for null serializer");
            }
            catch (ArgumentNullException)
            {
                AssertTrue(true, "Correctly threw ArgumentNullException for null serializer");
            }
            catch (Exception ex)
            {
                AssertTrue(false, $"Threw wrong exception type: {ex.GetType().Name}");
            }

            // Test invalid index access
            {
                var stackDir = Path.Combine(_testDirectory, "exception_index");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("Item1");

                try
                {
                    var item = stack[-1];
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for negative index");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for negative index");
                }

                try
                {
                    var item = stack[1];
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for out of bounds index");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for out of bounds index");
                }
            }

            // Test Pop on empty stack
            {
                var stackDir = Path.Combine(_testDirectory, "exception_empty_pop");
                using var stack = new PersistentStack<string>(stackDir);

                try
                {
                    var item = stack.Pop();
                    AssertTrue(false, "Should throw InvalidOperationException for Pop on empty stack");
                }
                catch (InvalidOperationException)
                {
                    AssertTrue(true, "Correctly threw InvalidOperationException for Pop on empty stack");
                }
            }

            // Test Peek on empty stack
            {
                var stackDir = Path.Combine(_testDirectory, "exception_empty_peek");
                using var stack = new PersistentStack<string>(stackDir);

                try
                {
                    var item = stack.Peek();
                    AssertTrue(false, "Should throw InvalidOperationException for Peek on empty stack");
                }
                catch (InvalidOperationException)
                {
                    AssertTrue(true, "Correctly threw InvalidOperationException for Peek on empty stack");
                }
            }

            // Test invalid key access
            {
                var stackDir = Path.Combine(_testDirectory, "exception_key");
                using var stack = new PersistentStack<string>(stackDir);

                try
                {
                    stack.Purge(null);
                    AssertTrue(false, "Should throw ArgumentNullException for null key in Purge");
                }
                catch (ArgumentNullException)
                {
                    AssertTrue(true, "Correctly threw ArgumentNullException for null key in Purge");
                }

                try
                {
                    stack.Pop("nonexistent-key");
                    AssertTrue(false, "Should throw KeyNotFoundException for nonexistent key in Pop");
                }
                catch (KeyNotFoundException)
                {
                    AssertTrue(true, "Correctly threw KeyNotFoundException for nonexistent key in Pop");
                }
            }

            // Test UpdateByKey with invalid parameters
            {
                var stackDir = Path.Combine(_testDirectory, "exception_update");
                using var stack = new PersistentStack<string>(stackDir);

                stack.Push("TestItem");

                try
                {
                    stack.UpdateByKey(null, "UpdatedItem");
                    AssertTrue(false, "Should throw ArgumentNullException for null key in UpdateByKey");
                }
                catch (ArgumentNullException)
                {
                    AssertTrue(true, "Correctly threw ArgumentNullException for null key in UpdateByKey");
                }

                try
                {
                    stack.UpdateByKey("nonexistent-key", "UpdatedItem");
                    AssertTrue(false, "Should throw KeyNotFoundException for nonexistent key in UpdateByKey");
                }
                catch (KeyNotFoundException)
                {
                    AssertTrue(true, "Correctly threw KeyNotFoundException for nonexistent key in UpdateByKey");
                }

                try
                {
                    string key = stack.Push("Item");
                    stack.UpdateByKey(key, null as string);
                    AssertTrue(false, "Should throw ArgumentNullException for null value in UpdateByKey");
                }
                catch (ArgumentNullException)
                {
                    AssertTrue(true, "Correctly threw ArgumentNullException for null value in UpdateByKey");
                }
            }

            // Test invalid arguments for UpdateAt
            {
                var stackDir = Path.Combine(_testDirectory, "exception_update_at");
                using var stack = new PersistentStack<string>(stackDir);

                try
                {
                    stack.UpdateAt(-1, "Item");
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for negative index in UpdateAt");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for negative index in UpdateAt");
                }

                try
                {
                    stack.Push("Item");
                    stack.UpdateAt(1, "UpdatedItem");
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for out of bounds index in UpdateAt");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for out of bounds index in UpdateAt");
                }

                try
                {
                    stack.UpdateAt(0, null);
                    AssertTrue(false, "Should throw ArgumentNullException for null value in UpdateAt");
                }
                catch (ArgumentNullException)
                {
                    AssertTrue(true, "Correctly threw ArgumentNullException for null value in UpdateAt");
                }
            }

            // Test invalid arguments for RemoveAt
            {
                var stackDir = Path.Combine(_testDirectory, "exception_remove_at");
                using var stack = new PersistentStack<string>(stackDir);

                try
                {
                    stack.RemoveAt(-1);
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for negative index in RemoveAt");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for negative index in RemoveAt");
                }

                try
                {
                    stack.Push("Item");
                    stack.RemoveAt(1);
                    AssertTrue(false, "Should throw ArgumentOutOfRangeException for out of bounds index in RemoveAt");
                }
                catch (ArgumentOutOfRangeException)
                {
                    AssertTrue(true, "Correctly threw ArgumentOutOfRangeException for out of bounds index in RemoveAt");
                }
            }
        }

        private static async Task RunAsyncTests()
        {
            Console.WriteLine("\nRunning Asynchronous API Tests...");

            // Test PushAsync
            {
                var stackDir = Path.Combine(_testDirectory, "async_push");
                using var stack = new PersistentStack<string>(stackDir);

                string key = await stack.PushAsync("AsyncItem");

                AssertEquals(1, stack.Count, "PushAsync should add item");
                byte[] data = await stack.PopAsync(key, false);
                string value = Encoding.UTF8.GetString(data);
                AssertTrue(value.Contains("AsyncItem"), "PopAsync should retrieve correct item content");
            }

            // Test PushAsync with T
            {
                var stackDir = Path.Combine(_testDirectory, "async_push_t");
                using var stack = new PersistentStack<Person>(stackDir);

                string key = await stack.PushAsync(new Person { Name = "AsyncPerson", Age = 25 });

                AssertEquals(1, stack.Count, "PushAsync<T> should add item");
                var person = await stack.PopItemAsync(false);
                AssertEquals("AsyncPerson", person.Name, "PopItemAsync should retrieve correct item");
            }

            // Test PopAsync
            {
                var stackDir = Path.Combine(_testDirectory, "async_pop");
                using var stack = new PersistentStack<string>(stackDir);

                await stack.PushAsync("Item1");
                await stack.PushAsync("Item2");

                byte[] data = await stack.PopAsync();
                string value = Encoding.UTF8.GetString(data);
                AssertTrue(value.Contains("Item2"), "PopAsync should retrieve the top item");
                AssertEquals(1, stack.Count, "PopAsync should remove item from stack");
            }

            // Test PopItemAsync
            {
                var stackDir = Path.Combine(_testDirectory, "async_pop_item");
                using var stack = new PersistentStack<string>(stackDir);

                await stack.PushAsync("Item1");
                await stack.PushAsync("Item2");

                string item = await stack.PopItemAsync();
                AssertEquals("Item2", item, "PopItemAsync should retrieve the top item");
                AssertEquals(1, stack.Count, "PopItemAsync should remove item from stack");
            }

            // Test PopAtAsync
            {
                var stackDir = Path.Combine(_testDirectory, "async_pop_at");
                using var stack = new PersistentStack<string>(stackDir);

                await stack.PushAsync("Item1");
                await stack.PushAsync("Item2");
                await stack.PushAsync("Item3");

                string item = await stack.PopAtAsync(1, true);
                AssertEquals("Item2", item, "PopAtAsync should retrieve the item at the specified index");
                AssertEquals(2, stack.Count, "PopAtAsync with remove=true should remove the item");

                // Verify the remaining items
                AssertEquals("Item3", await stack.PopItemAsync(), "First remaining item should be correct");
                AssertEquals("Item1", await stack.PopItemAsync(), "Second remaining item should be correct");
            }

            // Test GetKeysAsync
            {
                var stackDir = Path.Combine(_testDirectory, "async_get_keys");
                using var stack = new PersistentStack<string>(stackDir);

                string key1 = await stack.PushAsync("Item1");
                string key2 = await stack.PushAsync("Item2");

                var keys = await stack.GetKeysAsync();

                AssertEquals(2, keys.Count, "GetKeysAsync should return correct number of keys");
                AssertTrue(keys.Contains(key1) && keys.Contains(key2), "GetKeysAsync should return all keys");
            }

            // Test UpdateByKeyAsync
            {
                var stackDir = Path.Combine(_testDirectory, "async_update_key");
                using var stack = new PersistentStack<string>(stackDir);

                string key = await stack.PushAsync("OriginalItem");
                await stack.UpdateByKeyAsync(key, "UpdatedItem");

                byte[] data = await stack.PopAsync(key, false);
                string value = Encoding.UTF8.GetString(data);
                AssertTrue(value.Contains("UpdatedItem"), "UpdateByKeyAsync should update the item");
            }

            // Test UpdateByKeyAsync with T
            {
                var stackDir = Path.Combine(_testDirectory, "async_update_key_t");
                using var stack = new PersistentStack<Person>(stackDir);

                string key = await stack.PushAsync(new Person { Name = "Original", Age = 25 });
                await stack.UpdateByKeyAsync(key, new Person { Name = "Updated", Age = 30 });

                var person = await stack.PopAtAsync(0, false);
                AssertEquals("Updated", person.Name, "UpdateByKeyAsync<T> should update the item properties");
                AssertEquals(30, person.Age, "UpdateByKeyAsync<T> should update the item properties");
            }

            // Test UpdateAtAsync
            {
                var stackDir = Path.Combine(_testDirectory, "async_update_at");
                using var stack = new PersistentStack<string>(stackDir);

                await stack.PushAsync("Item1");
                await stack.PushAsync("Item2");
                await stack.PushAsync("Item3");

                await stack.UpdateAtAsync(1, "UpdatedItem2");

                AssertEquals("UpdatedItem2", await stack.PopAtAsync(1, false), "UpdateAtAsync should update the item at the specified index");
            }

            // Test cancellation token
            {
                var stackDir = Path.Combine(_testDirectory, "async_cancel");
                using var stack = new PersistentStack<int>(stackDir);

                var cts = new CancellationTokenSource();
                cts.Cancel(); // Cancel immediately

                try
                {
                    // This should be cancelled
                    await stack.PushAsync(123, cts.Token);

                    // If we get here, the operation wasn't cancelled properly
                    AssertTrue(false, "PushAsync should respect cancellation token");
                }
                catch (OperationCanceledException)
                {
                    // This is expected
                    AssertTrue(true, "PushAsync correctly responded to cancellation token");
                }

                // The stack should be empty
                AssertEquals(0, stack.Count, "Stack should be empty after cancelled PushAsync");
            }
        }

        private static async Task RunConcurrentTests()
        {
            Console.WriteLine("\nRunning Concurrent Access Tests...");

            // Test concurrent reads
            {
                var stackDir = Path.Combine(_testDirectory, "concurrent_read");
                using var stack = new PersistentStack<int>(stackDir);

                // Add some items
                for (int i = 0; i < 10; i++)
                {
                    stack.Push(i);
                }

                // Create multiple concurrent read tasks
                var tasks = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            if (j < stack.Count)
                            {
                                int value = stack[j];
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
                var stackDir = Path.Combine(_testDirectory, "concurrent_write");
                using var stack = new PersistentStack<int>(stackDir);

                // Create multiple concurrent write tasks
                var tasks = new List<Task>();
                int numTasks = 5;
                int itemsPerTask = 5;

                for (int i = 0; i < numTasks; i++)
                {
                    int taskId = i;
                    tasks.Add(Task.Run(async () =>
                    {
                        for (int j = 0; j < itemsPerTask; j++)
                        {
                            Console.WriteLine("| Pushing value " + j);
                            int value = taskId * 100 + j;
                            await stack.PushAsync(value);
                        }
                    }));
                }

                await Task.WhenAll(tasks);

                AssertEquals(numTasks * itemsPerTask, stack.Count, "All concurrent pushes should be completed");
            }

            // Test concurrent mixed operations
            {
                var stackDir = Path.Combine(_testDirectory, "concurrent_mixed");
                using var stack = new PersistentStack<string>(stackDir);

                // Add initial items
                for (int i = 0; i < 10; i++)
                {
                    stack.Push($"Item{i}");
                }

                // Perform various operations concurrently
                var tasks = new List<Task>();

                // Task 1: Read operations
                tasks.Add(Task.Run(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        string value;
                        if (stack.TryPeekAt(i, out value))
                        {
                            // do nothing
                        }
                    }
                }));

                // Task 2: Push operations
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await stack.PushAsync($"NewItem{i}");
                    }
                }));

                // Task 3: Pop operations
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < 5; i++)
                    {
                        try
                        {
                            await stack.PopItemAsync();
                        }
                        catch (InvalidOperationException)
                        {
                            // Stack might be empty temporarily due to concurrent operations
                        }
                    }
                }));

                await Task.WhenAll(tasks);

                // We can't assert an exact count due to race conditions,
                // but the stack should have approximately (10 initial + 10 pushed - 5 popped) items
                AssertTrue(stack.Count >= 10, "Stack should have a reasonable number of items after concurrent operations");
            }

            // Test producer-consumer pattern
            {
                var stackDir = Path.Combine(_testDirectory, "concurrent_producer_consumer");
                using var stack = new PersistentStack<int>(stackDir);

                var producerCount = 2;
                var consumerCount = 2;
                var itemsPerProducer = 8;
                var totalItems = producerCount * itemsPerProducer;

                var counter = new CountdownEvent(totalItems);
                var producersCompleted = new ManualResetEventSlim(false);

                // Start producers
                var producers = new List<Task>();
                for (int p = 0; p < producerCount; p++)
                {
                    int producerId = p;
                    producers.Add(Task.Run(async () =>
                    {
                        for (int i = 0; i < itemsPerProducer; i++)
                        {
                            int val = producerId * 1000 + i;
                            Console.WriteLine("| Producing value " + val);
                            await stack.PushAsync(val);
                            await Task.Delay(5); // Small delay to simulate work
                        }
                    }));
                }

                // Start consumers
                var consumers = new List<Task>();
                for (int c = 0; c < consumerCount; c++)
                {
                    consumers.Add(Task.Run(async () =>
                    {
                        while (!producersCompleted.IsSet || stack.Count > 0)
                        {
                            try
                            {
                                if (stack.Count > 0)
                                {
                                    var item = await stack.PopItemAsync();
                                    counter.Signal();
                                }
                                else
                                {
                                    await Task.Delay(10); // Wait if stack is empty
                                }
                            }
                            catch (InvalidOperationException)
                            {
                                // Stack might be empty due to concurrent operations
                                await Task.Delay(10);
                            }
                        }
                    }));
                }

                // Wait for producers to complete
                await Task.WhenAll(producers);
                producersCompleted.Set();

                // Wait for all items to be processed
                var allProcessed = counter.Wait(TimeSpan.FromSeconds(10));

                // Wait for consumers to complete
                await Task.WhenAll(consumers);

                AssertTrue(allProcessed, "All produced items should be consumed");
                AssertEquals(0, stack.Count, "Stack should be empty after all items are consumed");
            }
        }

        private static async Task RunComplexScenarioTests()
        {
            Console.WriteLine("\nRunning Complex Scenario Tests...");

            // Scenario 1: Stack as a processing queue with multiple workers
            {
                var stackDir = Path.Combine(_testDirectory, "scenario_processing_queue");
                using var stack = new PersistentStack<StackItem>(stackDir);

                // Add work items
                int workItems = 20;
                for (int i = 0; i < workItems; i++)
                {
                    Console.WriteLine("| Pushing stack item " + i);
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
                                else
                                    Console.WriteLine("| Popped item " + item.Id);
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
                var stackDir = Path.Combine(_testDirectory, "scenario_multi_readwrite");
                using var stack = new PersistentStack<string>(stackDir);

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
                            Console.WriteLine($"| Pushing Writer{writerId}_Item{i}");
                            await stack.PushAsync($"Writer{writerId}_Item{i}");
                            await Task.Delay(5); // Small delay
                        }
                    }));
                }

                // Wait a bit to give writers a head start
                await Task.Delay(100);

                // Start readers (non-destructive reads using TryPeekAt)
                for (int r = 0; r < readerCount; r++)
                {
                    readers.Add(Task.Run(() =>
                    {
                        for (int i = 0; i < readOperations; i++)
                        {
                            int randomIndex = new Random().Next(0, stack.Count > 0 ? stack.Count : 1);
                            string item = null;
                            stack.TryPeekAt(randomIndex, out item);
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
                var stackDir = Path.Combine(_testDirectory, "scenario_multiple_instances");

                // First instance: add initial data
                string[] keys = new string[3];
                using (var stack1 = new PersistentStack<string>(stackDir))
                {
                    stack1.Clear(); // Ensure clean state
                    keys[0] = stack1.Push("First");
                    keys[1] = stack1.Push("Second");
                    keys[2] = stack1.Push("Third");
                }

                // Second instance: read and modify
                using (var stack2 = new PersistentStack<string>(stackDir))
                {
                    AssertEquals(3, stack2.Count, "Second instance should see all items");
                    AssertEquals("Third", stack2.Peek(), "Second instance should read correct top item");

                    // Modify data
                    stack2.UpdateByKey(keys[1], "Modified");
                    stack2.Push("Fourth");
                    stack2.Pop(); // Remove "Fourth"
                }

                // Third instance: verify changes
                using (var stack3 = new PersistentStack<string>(stackDir))
                {
                    AssertEquals(3, stack3.Count, "Third instance should see correct item count");

                    // Verify stack content
                    var items = new List<string>();
                    while (stack3.Count > 0)
                    {
                        items.Add(stack3.Pop());
                    }

                    AssertCollectionEquals(new[] { "Third", "Modified", "First" }, items,
                        "Final stack should contain the correct items in the correct order");
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

    // Helper class for stack scenarios
    [Serializable]
    public class StackItem
    {
        public int Id { get; set; }
        public string Value { get; set; }

        public override string ToString()
        {
            return $"Item {Id}: {Value}";
        }

#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
#pragma warning restore CS8602 // Dereference of a possibly null reference.
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning restore CS8604 // Possible null reference argument.
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
#pragma warning restore CS8629 // Nullable value type may be null.
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
    }
}