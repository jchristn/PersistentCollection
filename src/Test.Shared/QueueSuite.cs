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
        private static TestSuiteDescriptor QueueSuite()
        {
            string s = "queue";

            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>
            {
                new TestCaseDescriptor(s, "new_queue_is_empty", "A new queue reports a count of zero", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    AssertEquals(0, queue.Count, "New queue should be empty.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "constructor_null_throws", "Constructor rejects a null persistence file", _ =>
                {
                    AssertThrows<ArgumentNullException>(() => new PersistentQueue<string>(null),
                        "Constructor should throw ArgumentNullException for a null path.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "constructor_empty_throws", "Constructor rejects an empty persistence file", _ =>
                {
                    AssertThrows<ArgumentNullException>(() => new PersistentQueue<string>(string.Empty),
                        "Constructor should throw ArgumentNullException for an empty path.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "enqueue_increments_count", "Enqueue increases the count", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    queue.Enqueue("Item3");
                    AssertEquals(3, queue.Count, "Queue should contain 3 items after enqueuing.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "dequeue_fifo_order", "Dequeue returns items first-in-first-out", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    queue.Enqueue("Item3");
                    AssertEquals("Item1", queue.Dequeue(), "First dequeue should return Item1 (FIFO).");
                    AssertEquals("Item2", queue.Dequeue(), "Second dequeue should return Item2.");
                    AssertEquals("Item3", queue.Dequeue(), "Third dequeue should return Item3.");
                    AssertEquals(0, queue.Count, "Queue should be empty after dequeuing all items.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "peek_returns_head", "Peek returns the head without removing it", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    AssertEquals("Item1", queue.Peek(), "Peek should return the head item.");
                    AssertEquals(2, queue.Count, "Peek should not change the count.");
                    AssertEquals("Item1", queue.Peek(), "Repeated peek should return the same head item.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "dequeue_empty_throws", "Dequeue on an empty queue throws", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    AssertThrows<InvalidOperationException>(() => queue.Dequeue(),
                        "Dequeue on an empty queue should throw InvalidOperationException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "peek_empty_throws", "Peek on an empty queue throws", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    AssertThrows<InvalidOperationException>(() => queue.Peek(),
                        "Peek on an empty queue should throw InvalidOperationException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "try_dequeue_success", "TryDequeue returns true and the head on a non-empty queue", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    bool ok = queue.TryDequeue(out string result);
                    Assert(ok, "TryDequeue should return true when the queue is not empty.");
                    AssertEquals("Item1", result, "TryDequeue should output the head item.");
                    AssertEquals(1, queue.Count, "Queue should have one item after a successful TryDequeue.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "try_dequeue_empty", "TryDequeue returns false and default on an empty queue", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    bool ok = queue.TryDequeue(out string result);
                    Assert(!ok, "TryDequeue should return false when the queue is empty.");
                    AssertEquals(default(string), result, "TryDequeue should output default on failure.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "try_peek_success", "TryPeek returns true and the head without removing it", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    bool ok = queue.TryPeek(out string result);
                    Assert(ok, "TryPeek should return true when the queue is not empty.");
                    AssertEquals("Item1", result, "TryPeek should output the head item.");
                    AssertEquals(1, queue.Count, "TryPeek should not change the count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "try_peek_empty", "TryPeek returns false and default on an empty queue", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    bool ok = queue.TryPeek(out string result);
                    Assert(!ok, "TryPeek should return false when the queue is empty.");
                    AssertEquals(default(string), result, "TryPeek should output default on failure.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "contains", "Contains reflects queue membership", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    Assert(queue.Contains("Item2"), "Contains should return true for an existing item.");
                    Assert(!queue.Contains("Nope"), "Contains should return false for a missing item.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "clear_empties", "Clear removes all items", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    queue.Clear();
                    AssertEquals(0, queue.Count, "Queue should be empty after Clear.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "to_array_fifo", "ToArray returns items in FIFO order", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    queue.Enqueue("Item3");
                    string[] array = queue.ToArray();
                    AssertSequenceEquals(new[] { "Item1", "Item2", "Item3" }, array, "ToArray should be FIFO ordered.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "copy_to_offset", "CopyTo writes items at the requested offset", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    queue.Enqueue("Item3");
                    string[] array = new string[5];
                    queue.CopyTo(array, 1);
                    AssertEquals(null, array[0], "Element before the offset should be untouched.");
                    AssertEquals("Item1", array[1], "First copied element should be Item1.");
                    AssertEquals("Item2", array[2], "Second copied element should be Item2.");
                    AssertEquals("Item3", array[3], "Third copied element should be Item3.");
                    AssertEquals(null, array[4], "Element after the copied range should be untouched.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "enumeration_fifo", "Enumeration visits items in FIFO order", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    queue.Enqueue("Item3");
                    List<string> items = queue.ToList();
                    AssertSequenceEquals(new[] { "Item1", "Item2", "Item3" }, items, "Enumeration should be FIFO ordered.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "int_element_type", "Queue supports value types", _ =>
                {
                    using PersistentQueue<int> queue = new PersistentQueue<int>(NewFile(s));
                    queue.Enqueue(10);
                    queue.Enqueue(20);
                    queue.Enqueue(30);
                    AssertSequenceEquals(new[] { 10, 20, 30 }, queue.ToArray(), "Integer queue should preserve FIFO order.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "complex_element_type", "Queue supports complex reference types", _ =>
                {
                    using PersistentQueue<Person> queue = new PersistentQueue<Person>(NewFile(s));
                    queue.Enqueue(new Person { Name = "Alice", Age = 25 });
                    queue.Enqueue(new Person { Name = "Bob", Age = 30 });
                    Person[] people = queue.ToArray();
                    AssertEquals("Alice", people[0].Name, "First person should be Alice.");
                    AssertEquals(30, people[1].Age, "Second person's age should be 30.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "persistence_across_instances", "Enqueued items survive being reopened", _ =>
                {
                    string file = NewFile(s);
                    using (PersistentQueue<string> queue = new PersistentQueue<string>(file))
                    {
                        queue.Enqueue("Item1");
                        queue.Enqueue("Item2");
                        queue.Enqueue("Item3");
                    }

                    using (PersistentQueue<string> reopened = new PersistentQueue<string>(file))
                    {
                        AssertEquals(3, reopened.Count, "Reopened queue should have all items.");
                        AssertSequenceEquals(new[] { "Item1", "Item2", "Item3" }, reopened.ToArray(),
                            "Reopened queue should preserve FIFO order.");
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "persistence_after_dequeue", "Dequeues are persisted across instances", _ =>
                {
                    string file = NewFile(s);
                    using (PersistentQueue<string> queue = new PersistentQueue<string>(file))
                    {
                        queue.Enqueue("First");
                        queue.Enqueue("Second");
                        queue.Enqueue("Third");
                    }

                    using (PersistentQueue<string> second = new PersistentQueue<string>(file))
                    {
                        AssertEquals("First", second.Dequeue(), "Second instance should dequeue First.");
                        second.Enqueue("Fourth");
                    }

                    using (PersistentQueue<string> third = new PersistentQueue<string>(file))
                    {
                        AssertSequenceEquals(new[] { "Second", "Third", "Fourth" }, third.ToArray(),
                            "Final state should reflect the dequeue and later enqueue.");
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "concurrent_enqueue_count", "Concurrent enqueues all persist", async _ =>
                {
                    using PersistentQueue<int> queue = new PersistentQueue<int>(NewFile(s));
                    List<Task> tasks = new List<Task>();
                    for (int t = 0; t < 10; t++)
                    {
                        int taskId = t;
                        tasks.Add(Task.Run(() =>
                        {
                            for (int i = 0; i < 10; i++) queue.Enqueue(taskId * 10 + i);
                        }));
                    }

                    await Task.WhenAll(tasks).ConfigureAwait(false);
                    AssertEquals(100, queue.Count, "All concurrent enqueues should be counted.");
                }),

                new TestCaseDescriptor(s, "empty_to_array_is_empty", "ToArray on an empty queue returns an empty array", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    AssertEquals(0, queue.ToArray().Length, "ToArray on an empty queue should be empty.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "empty_enumeration_yields_nothing", "Enumerating an empty queue yields no items", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    AssertEquals(0, queue.ToList().Count, "An empty queue should enumerate no items.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "enumerator_is_snapshot", "An enumerator reflects the queue as it was when created", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    List<string> observed = new List<string>();
                    using (IEnumerator<string> e = queue.GetEnumerator())
                    {
                        queue.Enqueue("Item3"); // mutate after acquiring the enumerator
                        while (e.MoveNext()) observed.Add(e.Current);
                    }
                    AssertSequenceEquals(new[] { "Item1", "Item2" }, observed,
                        "The enumerator should not observe items added after it was created.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "non_generic_copy_to", "The non-generic ICollection.CopyTo writes items in FIFO order", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Enqueue("Item2");
                    Array array = new string[2];
                    ((System.Collections.ICollection)queue).CopyTo(array, 0);
                    AssertEquals("Item1", (string)array.GetValue(0), "First element should be Item1.");
                    AssertEquals("Item2", (string)array.GetValue(1), "Second element should be Item2.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "copy_to_null_throws", "CopyTo rejects a null array", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    AssertThrows<ArgumentNullException>(() => queue.CopyTo((string[])null, 0),
                        "CopyTo with a null array should throw ArgumentNullException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "copy_to_negative_index_throws", "CopyTo rejects a negative index", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    AssertThrows<ArgumentOutOfRangeException>(() => queue.CopyTo(new string[2], -1),
                        "CopyTo with a negative index should throw ArgumentOutOfRangeException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "sync_primitives", "SyncRoot and IsSynchronized are exposed", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    Assert(queue.IsSynchronized, "IsSynchronized should be true.");
                    Assert(queue.SyncRoot != null, "SyncRoot should not be null.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "reusable_after_clear", "The queue is usable after Clear", _ =>
                {
                    using PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s));
                    queue.Enqueue("Item1");
                    queue.Clear();
                    queue.Enqueue("Item2");
                    AssertEquals(1, queue.Count, "Queue should accept items after Clear.");
                    AssertEquals("Item2", queue.Peek(), "Head after Clear should be the newly enqueued item.");
                    return Task.CompletedTask;
                })
            };

            return new TestSuiteDescriptor(s, "PersistentQueue contract", cases, CleanSuite(s), null);
        }
    }
}
