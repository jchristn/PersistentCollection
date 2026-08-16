namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using PersistentCollection;
    using Touchstone.Core;
    using static Test.Shared.TestSupport;

    public static partial class PersistentCollectionSuites
    {
        private static TestSuiteDescriptor ListSuite()
        {
            string s = "list";

            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>
            {
                new TestCaseDescriptor(s, "new_list_is_empty", "A new list reports a count of zero", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    AssertEquals(0, list.Count, "New list should be empty.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "constructor_null_throws", "Constructor rejects a null persistence file", _ =>
                {
                    AssertThrows<ArgumentNullException>(() => new PersistentList<string>(null),
                        "Constructor should throw ArgumentNullException for a null path.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "constructor_empty_throws", "Constructor rejects an empty persistence file", _ =>
                {
                    AssertThrows<ArgumentNullException>(() => new PersistentList<string>(string.Empty),
                        "Constructor should throw ArgumentNullException for an empty path.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "add_increments_count", "Add increases the count", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    list.Add("Item3");
                    AssertEquals(3, list.Count, "List should contain 3 items after adding.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "index_access", "Items are retrievable by index in insertion order", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    list.Add("Item3");
                    AssertEquals("Item1", list[0], "Index 0 should be Item1.");
                    AssertEquals("Item2", list[1], "Index 1 should be Item2.");
                    AssertEquals("Item3", list[2], "Index 2 should be Item3.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "indexer_set_updates", "The indexer setter replaces a value", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    list[1] = "Updated";
                    AssertEquals("Updated", list[1], "Indexer set should replace the value.");
                    AssertEquals(2, list.Count, "Indexer set should not change the count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "insert_middle", "Insert places an item at the requested index", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item3");
                    list.Insert(1, "Item2");
                    AssertSequenceEquals(new[] { "Item1", "Item2", "Item3" }, list, "Insert should shift later items.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "insert_beginning_and_end", "Insert works at the boundaries", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Middle");
                    list.Insert(0, "First");
                    list.Insert(list.Count, "Last");
                    AssertSequenceEquals(new[] { "First", "Middle", "Last" }, list, "Insert at boundaries should work.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "remove_at", "RemoveAt removes the item at an index", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    list.Add("Item3");
                    list.RemoveAt(1);
                    AssertSequenceEquals(new[] { "Item1", "Item3" }, list, "RemoveAt should remove the middle item.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "remove_value_first_occurrence", "Remove deletes the first matching value", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    list.Add("Item3");
                    list.Add("Item2");
                    bool removed = list.Remove("Item2");
                    Assert(removed, "Remove should return true when the value exists.");
                    AssertSequenceEquals(new[] { "Item1", "Item3", "Item2" }, list,
                        "Remove should delete only the first occurrence.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "remove_missing_returns_false", "Remove returns false for a missing value", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    bool removed = list.Remove("Nope");
                    Assert(!removed, "Remove should return false when the value is missing.");
                    AssertEquals(1, list.Count, "Remove of a missing value should not change the count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "index_of", "IndexOf returns the first index or -1", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    list.Add("Item2");
                    AssertEquals(1, list.IndexOf("Item2"), "IndexOf should return the first occurrence.");
                    AssertEquals(-1, list.IndexOf("Nope"), "IndexOf should return -1 for a missing value.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "contains", "Contains reflects list membership", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    Assert(list.Contains("Item2"), "Contains should return true for an existing item.");
                    Assert(!list.Contains("Nope"), "Contains should return false for a missing item.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "clear_empties", "Clear removes all items", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    list.Clear();
                    AssertEquals(0, list.Count, "List should be empty after Clear.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "copy_to_offset", "CopyTo writes items at the requested offset", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    list.Add("Item3");
                    string[] array = new string[5];
                    list.CopyTo(array, 1);
                    AssertEquals(null, array[0], "Element before the offset should be untouched.");
                    AssertEquals("Item1", array[1], "First copied element should be Item1.");
                    AssertEquals("Item2", array[2], "Second copied element should be Item2.");
                    AssertEquals("Item3", array[3], "Third copied element should be Item3.");
                    AssertEquals(null, array[4], "Element after the copied range should be untouched.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "is_read_only_false", "IsReadOnly is false", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    Assert(!list.IsReadOnly, "IsReadOnly should be false.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "enumeration_order", "Enumeration visits items in insertion order", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    list.Add("Item3");
                    AssertSequenceEquals(new[] { "Item1", "Item2", "Item3" }, list.ToList(),
                        "Enumeration should follow insertion order.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "int_element_type", "List supports value types", _ =>
                {
                    using PersistentList<int> list = new PersistentList<int>(NewFile(s));
                    list.Add(10);
                    list.Add(20);
                    list.Add(30);
                    AssertSequenceEquals(new[] { 10, 20, 30 }, list, "Integer list should preserve order.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "complex_element_type", "List supports complex reference types", _ =>
                {
                    using PersistentList<Person> list = new PersistentList<Person>(NewFile(s));
                    list.Add(new Person { Name = "Alice", Age = 25 });
                    list.Add(new Person { Name = "Bob", Age = 30 });
                    AssertEquals("Alice", list[0].Name, "First person should be Alice.");
                    AssertEquals(30, list[1].Age, "Second person's age should be 30.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "index_negative_throws", "Reading a negative index throws", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    AssertThrows<ArgumentOutOfRangeException>(() => { string _ = list[-1]; },
                        "Negative index read should throw ArgumentOutOfRangeException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "index_out_of_bounds_throws", "Reading an out-of-bounds index throws", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    AssertThrows<ArgumentOutOfRangeException>(() => { string _ = list[1]; },
                        "Out-of-bounds index read should throw ArgumentOutOfRangeException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "insert_out_of_range_throws", "Insert beyond Count throws", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    AssertThrows<ArgumentOutOfRangeException>(() => list.Insert(-1, "x"),
                        "Insert at a negative index should throw ArgumentOutOfRangeException.");
                    AssertThrows<ArgumentOutOfRangeException>(() => list.Insert(5, "x"),
                        "Insert beyond Count should throw ArgumentOutOfRangeException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "remove_at_out_of_range_throws", "RemoveAt out of range throws", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    AssertThrows<ArgumentOutOfRangeException>(() => list.RemoveAt(-1),
                        "RemoveAt a negative index should throw ArgumentOutOfRangeException.");
                    AssertThrows<ArgumentOutOfRangeException>(() => list.RemoveAt(1),
                        "RemoveAt an index at or beyond Count should throw ArgumentOutOfRangeException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "persistence_across_instances", "Added items survive being reopened", _ =>
                {
                    string file = NewFile(s);
                    using (PersistentList<string> list = new PersistentList<string>(file))
                    {
                        list.Add("Item1");
                        list.Add("Item2");
                        list.Add("Item3");
                    }

                    using (PersistentList<string> reopened = new PersistentList<string>(file))
                    {
                        AssertEquals(3, reopened.Count, "Reopened list should have all items.");
                        AssertSequenceEquals(new[] { "Item1", "Item2", "Item3" }, reopened,
                            "Reopened list should preserve order.");
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "persistence_after_modifications", "Modifications persist across instances", _ =>
                {
                    string file = NewFile(s);
                    using (PersistentList<string> a = new PersistentList<string>(file))
                    {
                        a.Add("First");
                        a.Add("Second");
                        a.Add("Third");
                    }

                    using (PersistentList<string> b = new PersistentList<string>(file))
                    {
                        b[1] = "Modified";
                        b.RemoveAt(2);
                        b.Add("Fourth");
                    }

                    using (PersistentList<string> c = new PersistentList<string>(file))
                    {
                        AssertSequenceEquals(new[] { "First", "Modified", "Fourth" }, c,
                            "Final state should reflect all cross-instance modifications.");
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "concurrent_add_count", "Concurrent adds all persist", async _ =>
                {
                    using PersistentList<int> list = new PersistentList<int>(NewFile(s));
                    List<Task> tasks = new List<Task>();
                    for (int t = 0; t < 10; t++)
                    {
                        int taskId = t;
                        tasks.Add(Task.Run(() =>
                        {
                            for (int i = 0; i < 10; i++) list.Add(taskId * 10 + i);
                        }));
                    }

                    await Task.WhenAll(tasks).ConfigureAwait(false);
                    AssertEquals(100, list.Count, "All concurrent adds should be counted.");
                }),

                new TestCaseDescriptor(s, "empty_enumeration_yields_nothing", "Enumerating an empty list yields no items", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    AssertEquals(0, list.ToList().Count, "An empty list should enumerate no items.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "enumerator_is_snapshot", "An enumerator reflects the list as it was when created", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    List<string> observed = new List<string>();
                    using (IEnumerator<string> e = list.GetEnumerator())
                    {
                        list.Add("Item3"); // mutate after acquiring the enumerator
                        while (e.MoveNext()) observed.Add(e.Current);
                    }
                    AssertSequenceEquals(new[] { "Item1", "Item2" }, observed,
                        "The enumerator should not observe items added after it was created.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "indexer_set_out_of_range_throws", "Setting an out-of-range index throws", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    AssertThrows<ArgumentOutOfRangeException>(() => { list[5] = "x"; },
                        "Setting an out-of-bounds index should throw ArgumentOutOfRangeException.");
                    AssertThrows<ArgumentOutOfRangeException>(() => { list[-1] = "x"; },
                        "Setting a negative index should throw ArgumentOutOfRangeException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "copy_to_null_throws", "CopyTo rejects a null array", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    AssertThrows<ArgumentNullException>(() => list.CopyTo(null, 0),
                        "CopyTo with a null array should throw ArgumentNullException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "copy_to_insufficient_space_throws", "CopyTo rejects an undersized array", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Add("Item2");
                    AssertThrows<ArgumentException>(() => list.CopyTo(new string[1], 0),
                        "CopyTo into an undersized array should throw ArgumentException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "sync_primitives", "SyncRoot and IsSynchronized are exposed", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    Assert(list.IsSynchronized, "IsSynchronized should be true.");
                    Assert(list.SyncRoot != null, "SyncRoot should not be null.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "reusable_after_clear", "The list is usable after Clear", _ =>
                {
                    using PersistentList<string> list = new PersistentList<string>(NewFile(s));
                    list.Add("Item1");
                    list.Clear();
                    list.Add("Item2");
                    AssertEquals(1, list.Count, "List should accept items after Clear.");
                    AssertEquals("Item2", list[0], "First item after Clear should be the newly added item.");
                    return Task.CompletedTask;
                })
            };

            return new TestSuiteDescriptor(s, "PersistentList contract", cases, CleanSuite(s), null);
        }
    }
}
