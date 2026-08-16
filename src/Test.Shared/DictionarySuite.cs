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
        private static TestSuiteDescriptor DictionarySuite()
        {
            string s = "dictionary";

            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>
            {
                new TestCaseDescriptor(s, "new_dictionary_is_empty", "A new dictionary reports a count of zero", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    AssertEquals(0, dict.Count, "New dictionary should be empty.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "constructor_null_throws", "Constructor rejects a null persistence file", _ =>
                {
                    AssertThrows<ArgumentNullException>(() => new PersistentDictionary<string, string>(null),
                        "Constructor should throw ArgumentNullException for a null path.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "constructor_empty_throws", "Constructor rejects an empty persistence file", _ =>
                {
                    AssertThrows<ArgumentNullException>(() => new PersistentDictionary<string, string>(string.Empty),
                        "Constructor should throw ArgumentNullException for an empty path.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "add_increments_count", "Add increases the count", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Add("Key2", "Value2");
                    dict.Add("Key3", "Value3");
                    AssertEquals(3, dict.Count, "Dictionary should contain 3 items after adding.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "get_by_key", "Values are retrievable by key", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Add("Key2", "Value2");
                    AssertEquals("Value1", dict["Key1"], "Value for Key1 should be Value1.");
                    AssertEquals("Value2", dict["Key2"], "Value for Key2 should be Value2.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "indexer_update", "The indexer setter replaces a value", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict["Key1"] = "Updated";
                    AssertEquals("Updated", dict["Key1"], "Indexer set should replace the value.");
                    AssertEquals(1, dict.Count, "Updating an existing key should not change the count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "indexer_add", "The indexer setter can add a new key", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict["Key1"] = "Value1";
                    AssertEquals("Value1", dict["Key1"], "Indexer set should add a new key.");
                    AssertEquals(1, dict.Count, "Adding a new key via the indexer should increase the count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "remove_key", "Remove by key deletes an entry", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Add("Key2", "Value2");
                    bool removed = dict.Remove("Key1");
                    Assert(removed, "Remove should return true for an existing key.");
                    Assert(!dict.ContainsKey("Key1"), "Removed key should be gone.");
                    AssertEquals(1, dict.Count, "Remove should decrease the count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "remove_missing_key_returns_false", "Remove of a missing key returns false", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    bool removed = dict.Remove("Nope");
                    Assert(!removed, "Remove should return false for a missing key.");
                    AssertEquals(1, dict.Count, "Remove of a missing key should not change the count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "try_get_value", "TryGetValue reports presence and outputs the value", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    bool found = dict.TryGetValue("Key1", out string value);
                    Assert(found, "TryGetValue should return true for an existing key.");
                    AssertEquals("Value1", value, "TryGetValue should output the value.");
                    bool missing = dict.TryGetValue("Nope", out string missingValue);
                    Assert(!missing, "TryGetValue should return false for a missing key.");
                    AssertEquals(default(string), missingValue, "TryGetValue should output default for a missing key.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "contains_key", "ContainsKey reflects key membership", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    Assert(dict.ContainsKey("Key1"), "ContainsKey should return true for an existing key.");
                    Assert(!dict.ContainsKey("Nope"), "ContainsKey should return false for a missing key.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "contains_kvp", "Contains matches on both key and value", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    Assert(dict.Contains(new KeyValuePair<string, string>("Key1", "Value1")),
                        "Contains should return true for a matching pair.");
                    Assert(!dict.Contains(new KeyValuePair<string, string>("Key1", "Wrong")),
                        "Contains should return false when the value differs.");
                    Assert(!dict.Contains(new KeyValuePair<string, string>("Nope", "Value1")),
                        "Contains should return false when the key is missing.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "add_kvp", "Add via ICollection accepts a key-value pair", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    ICollection<KeyValuePair<string, string>> collection = dict;
                    collection.Add(new KeyValuePair<string, string>("Key1", "Value1"));
                    AssertEquals("Value1", dict["Key1"], "Add via ICollection should store the pair.");
                    AssertEquals(1, dict.Count, "Add via ICollection should increase the count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "remove_kvp_value_must_match", "Remove via ICollection matches key and value", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Add("Key2", "Value2");
                    ICollection<KeyValuePair<string, string>> collection = dict;
                    bool wrong = collection.Remove(new KeyValuePair<string, string>("Key1", "Wrong"));
                    Assert(!wrong, "Remove via ICollection should fail when the value differs.");
                    bool ok = collection.Remove(new KeyValuePair<string, string>("Key1", "Value1"));
                    Assert(ok, "Remove via ICollection should succeed for a matching pair.");
                    AssertEquals(1, dict.Count, "Only the matching pair should be removed.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "keys_and_values", "Keys and Values expose the entries", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Add("Key2", "Value2");
                    dict.Add("Key3", "Value3");
                    List<string> keys = dict.Keys.ToList();
                    List<string> values = dict.Values.ToList();
                    AssertEquals(3, keys.Count, "Keys should contain 3 entries.");
                    AssertEquals(3, values.Count, "Values should contain 3 entries.");
                    Assert(keys.Contains("Key1") && keys.Contains("Key2") && keys.Contains("Key3"),
                        "Keys should contain all inserted keys.");
                    Assert(values.Contains("Value1") && values.Contains("Value2") && values.Contains("Value3"),
                        "Values should contain all inserted values.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "clear_empties", "Clear removes all entries", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Add("Key2", "Value2");
                    dict.Clear();
                    AssertEquals(0, dict.Count, "Dictionary should be empty after Clear.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "copy_to_offset", "CopyTo writes entries at the requested offset", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Add("Key2", "Value2");
                    dict.Add("Key3", "Value3");
                    KeyValuePair<string, string>[] array = new KeyValuePair<string, string>[5];
                    ((ICollection<KeyValuePair<string, string>>)dict).CopyTo(array, 1);
                    Assert(array[0].Equals(default(KeyValuePair<string, string>)), "Element before the offset should be default.");
                    Assert(array[4].Equals(default(KeyValuePair<string, string>)), "Element after the copied range should be default.");
                    Dictionary<string, string> extracted = new Dictionary<string, string>();
                    for (int i = 1; i < 4; i++) extracted[array[i].Key] = array[i].Value;
                    AssertEquals(3, extracted.Count, "All three entries should be copied.");
                    AssertEquals("Value2", extracted["Key2"], "Copied entry Key2 should retain its value.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "is_read_only_false", "IsReadOnly is false", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    Assert(!dict.IsReadOnly, "IsReadOnly should be false.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "enumeration", "Enumeration visits every entry", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Add("Key2", "Value2");
                    dict.Add("Key3", "Value3");
                    Dictionary<string, string> seen = new Dictionary<string, string>();
                    foreach (KeyValuePair<string, string> kvp in dict) seen[kvp.Key] = kvp.Value;
                    AssertEquals(3, seen.Count, "Enumeration should visit all entries.");
                    AssertEquals("Value2", seen["Key2"], "Enumerated value for Key2 should be correct.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "int_value_type", "Dictionary supports value-typed values", _ =>
                {
                    using PersistentDictionary<string, int> dict = new PersistentDictionary<string, int>(NewFile(s));
                    dict.Add("a", 1);
                    dict.Add("b", 2);
                    AssertEquals(1, dict["a"], "Value for 'a' should be 1.");
                    AssertEquals(2, dict["b"], "Value for 'b' should be 2.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "complex_value_type", "Dictionary supports complex reference values", _ =>
                {
                    using PersistentDictionary<string, Person> dict = new PersistentDictionary<string, Person>(NewFile(s));
                    dict.Add("alice", new Person { Name = "Alice", Age = 25 });
                    AssertEquals(25, dict["alice"].Age, "Stored person's age should be 25.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "missing_key_throws", "Indexing a missing key throws", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    AssertThrows<KeyNotFoundException>(() => { string _ = dict["Nope"]; },
                        "Indexing a missing key should throw KeyNotFoundException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "duplicate_key_throws", "Adding a duplicate key throws", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    AssertThrows<ArgumentException>(() => dict.Add("Key1", "Value2"),
                        "Adding a duplicate key should throw ArgumentException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "null_key_throws", "Adding a null key throws", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    AssertThrows<ArgumentNullException>(() => dict.Add(null, "Value"),
                        "Adding a null key should throw ArgumentNullException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "persistence_across_instances", "Entries survive being reopened", _ =>
                {
                    string file = NewFile(s);
                    using (PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(file))
                    {
                        dict.Add("Key1", "Value1");
                        dict.Add("Key2", "Value2");
                        dict.Add("Key3", "Value3");
                    }

                    using (PersistentDictionary<string, string> reopened = new PersistentDictionary<string, string>(file))
                    {
                        AssertEquals(3, reopened.Count, "Reopened dictionary should have all entries.");
                        AssertEquals("Value2", reopened["Key2"], "Reopened value for Key2 should persist.");
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "persistence_after_modifications", "Modifications persist across instances", _ =>
                {
                    string file = NewFile(s);
                    using (PersistentDictionary<string, string> a = new PersistentDictionary<string, string>(file))
                    {
                        a.Add("Key1", "First");
                        a.Add("Key2", "Second");
                        a.Add("Key3", "Third");
                    }

                    using (PersistentDictionary<string, string> b = new PersistentDictionary<string, string>(file))
                    {
                        b["Key2"] = "Modified";
                        b.Remove("Key3");
                        b.Add("Key4", "Fourth");
                    }

                    using (PersistentDictionary<string, string> c = new PersistentDictionary<string, string>(file))
                    {
                        AssertEquals(3, c.Count, "Final dictionary should have three entries.");
                        AssertEquals("First", c["Key1"], "Unchanged entry should persist.");
                        AssertEquals("Modified", c["Key2"], "Modified entry should persist.");
                        Assert(!c.ContainsKey("Key3"), "Removed entry should stay removed.");
                        AssertEquals("Fourth", c["Key4"], "New entry should persist.");
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "concurrent_add_count", "Concurrent adds of distinct keys all persist", async _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    List<Task> tasks = new List<Task>();
                    for (int t = 0; t < 5; t++)
                    {
                        int taskId = t;
                        tasks.Add(Task.Run(() =>
                        {
                            for (int i = 0; i < 10; i++) dict.Add("k_" + taskId + "_" + i, "v_" + taskId + "_" + i);
                        }));
                    }

                    await Task.WhenAll(tasks).ConfigureAwait(false);
                    AssertEquals(50, dict.Count, "All concurrent adds of distinct keys should be counted.");
                }),

                new TestCaseDescriptor(s, "empty_enumeration_yields_nothing", "Enumerating an empty dictionary yields no entries", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    int count = 0;
                    foreach (KeyValuePair<string, string> kvp in dict) count++;
                    AssertEquals(0, count, "An empty dictionary should enumerate no entries.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "enumerator_is_snapshot", "An enumerator reflects the dictionary as it was when created", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Add("Key2", "Value2");
                    int observed = 0;
                    using (IEnumerator<KeyValuePair<string, string>> e = dict.GetEnumerator())
                    {
                        dict.Add("Key3", "Value3"); // mutate after acquiring the enumerator
                        while (e.MoveNext()) observed++;
                    }
                    AssertEquals(2, observed, "The enumerator should not observe entries added after it was created.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "keys_values_are_snapshots", "Keys and Values are independent of later mutations", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    ICollection<string> keys = dict.Keys;
                    ICollection<string> values = dict.Values;
                    dict.Add("Key2", "Value2");
                    AssertEquals(1, keys.Count, "A previously returned Keys collection should not change when the dictionary grows.");
                    AssertEquals(1, values.Count, "A previously returned Values collection should not change when the dictionary grows.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "add_duplicate_kvp_throws", "Adding a duplicate key via a pair throws", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    AssertThrows<ArgumentException>(() => dict.Add(new KeyValuePair<string, string>("Key1", "Value2")),
                        "Adding a duplicate key via a key-value pair should throw ArgumentException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "indexer_set_null_key_throws", "Setting a null key via the indexer throws", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    AssertThrows<ArgumentNullException>(() => { dict[null] = "Value"; },
                        "Setting a null key via the indexer should throw ArgumentNullException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "sync_primitives", "SyncRoot and IsSynchronized are exposed", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    Assert(dict.IsSynchronized, "IsSynchronized should be true.");
                    Assert(dict.SyncRoot != null, "SyncRoot should not be null.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "reusable_after_clear", "The dictionary is usable after Clear", _ =>
                {
                    using PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s));
                    dict.Add("Key1", "Value1");
                    dict.Clear();
                    dict.Add("Key2", "Value2");
                    AssertEquals(1, dict.Count, "Dictionary should accept entries after Clear.");
                    AssertEquals("Value2", dict["Key2"], "Value after Clear should be the newly added entry.");
                    return Task.CompletedTask;
                })
            };

            return new TestSuiteDescriptor(s, "PersistentDictionary contract", cases, CleanSuite(s), null);
        }
    }
}
