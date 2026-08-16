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
        private static TestSuiteDescriptor StackSuite()
        {
            string s = "stack";

            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>
            {
                new TestCaseDescriptor(s, "new_stack_is_empty", "A new stack reports a count of zero", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    AssertEquals(0, stack.Count, "New stack should be empty.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "constructor_null_throws", "Constructor rejects a null persistence file", _ =>
                {
                    AssertThrows<ArgumentNullException>(() => new PersistentStack<string>(null),
                        "Constructor should throw ArgumentNullException for a null path.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "constructor_empty_throws", "Constructor rejects an empty persistence file", _ =>
                {
                    AssertThrows<ArgumentNullException>(() => new PersistentStack<string>(string.Empty),
                        "Constructor should throw ArgumentNullException for an empty path.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "push_increments_count", "Push increases the count", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    stack.Push("Item3");
                    AssertEquals(3, stack.Count, "Stack should contain 3 items after pushing.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "pop_lifo_order", "Pop returns items last-in-first-out", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    stack.Push("Item3");
                    AssertEquals("Item3", stack.Pop(), "First pop should return Item3 (LIFO).");
                    AssertEquals("Item2", stack.Pop(), "Second pop should return Item2.");
                    AssertEquals("Item1", stack.Pop(), "Third pop should return Item1.");
                    AssertEquals(0, stack.Count, "Stack should be empty after popping all items.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "peek_returns_top", "Peek returns the top without removing it", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    AssertEquals("Item2", stack.Peek(), "Peek should return the top item.");
                    AssertEquals(2, stack.Count, "Peek should not change the count.");
                    stack.Pop();
                    AssertEquals("Item1", stack.Peek(), "Peek should return the new top after a pop.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "pop_empty_throws", "Pop on an empty stack throws", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    AssertThrows<InvalidOperationException>(() => stack.Pop(),
                        "Pop on an empty stack should throw InvalidOperationException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "peek_empty_throws", "Peek on an empty stack throws", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    AssertThrows<InvalidOperationException>(() => stack.Peek(),
                        "Peek on an empty stack should throw InvalidOperationException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "try_pop_success", "TryPop returns true and the top on a non-empty stack", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    bool ok = stack.TryPop(out string result);
                    Assert(ok, "TryPop should return true when the stack is not empty.");
                    AssertEquals("Item2", result, "TryPop should output the top item.");
                    AssertEquals(1, stack.Count, "Stack should have one item after a successful TryPop.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "try_pop_empty", "TryPop returns false and default on an empty stack", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    bool ok = stack.TryPop(out string result);
                    Assert(!ok, "TryPop should return false when the stack is empty.");
                    AssertEquals(default(string), result, "TryPop should output default on failure.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "try_peek_success", "TryPeek returns true and the top without removing it", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    bool ok = stack.TryPeek(out string result);
                    Assert(ok, "TryPeek should return true when the stack is not empty.");
                    AssertEquals("Item1", result, "TryPeek should output the top item.");
                    AssertEquals(1, stack.Count, "TryPeek should not change the count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "try_peek_empty", "TryPeek returns false and default on an empty stack", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    bool ok = stack.TryPeek(out string result);
                    Assert(!ok, "TryPeek should return false when the stack is empty.");
                    AssertEquals(default(string), result, "TryPeek should output default on failure.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "contains", "Contains reflects stack membership", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    Assert(stack.Contains("Item2"), "Contains should return true for an existing item.");
                    Assert(!stack.Contains("Nope"), "Contains should return false for a missing item.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "clear_empties", "Clear removes all items", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    stack.Clear();
                    AssertEquals(0, stack.Count, "Stack should be empty after Clear.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "to_array_lifo", "ToArray returns items top-first (LIFO)", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    stack.Push("Item3");
                    AssertSequenceEquals(new[] { "Item3", "Item2", "Item1" }, stack.ToArray(),
                        "ToArray should be LIFO ordered (top first).");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "copy_to_offset", "CopyTo writes items top-first at the requested offset", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    stack.Push("Item3");
                    string[] array = new string[5];
                    stack.CopyTo(array, 1);
                    AssertEquals(null, array[0], "Element before the offset should be untouched.");
                    AssertEquals("Item3", array[1], "First copied element should be the top (Item3).");
                    AssertEquals("Item2", array[2], "Second copied element should be Item2.");
                    AssertEquals("Item1", array[3], "Third copied element should be Item1.");
                    AssertEquals(null, array[4], "Element after the copied range should be untouched.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "enumeration_lifo", "Enumeration visits items top-first (LIFO)", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    stack.Push("Item3");
                    List<string> items = stack.ToList();
                    AssertSequenceEquals(new[] { "Item3", "Item2", "Item1" }, items, "Enumeration should be LIFO ordered.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "int_element_type", "Stack supports value types", _ =>
                {
                    using PersistentStack<int> stack = new PersistentStack<int>(NewFile(s));
                    stack.Push(10);
                    stack.Push(20);
                    stack.Push(30);
                    AssertSequenceEquals(new[] { 30, 20, 10 }, stack.ToArray(), "Integer stack should be LIFO ordered.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "complex_element_type", "Stack supports complex reference types", _ =>
                {
                    using PersistentStack<Person> stack = new PersistentStack<Person>(NewFile(s));
                    stack.Push(new Person { Name = "Alice", Age = 25 });
                    stack.Push(new Person { Name = "Bob", Age = 30 });
                    Person[] people = stack.ToArray();
                    AssertEquals("Bob", people[0].Name, "Top person should be Bob.");
                    AssertEquals(25, people[1].Age, "Bottom person's age should be 25.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "persistence_across_instances", "Pushed items survive being reopened in LIFO order", _ =>
                {
                    string file = NewFile(s);
                    using (PersistentStack<string> stack = new PersistentStack<string>(file))
                    {
                        stack.Push("Item1");
                        stack.Push("Item2");
                        stack.Push("Item3");
                    }

                    using (PersistentStack<string> reopened = new PersistentStack<string>(file))
                    {
                        AssertEquals(3, reopened.Count, "Reopened stack should have all items.");
                        AssertSequenceEquals(new[] { "Item3", "Item2", "Item1" }, reopened.ToArray(),
                            "Reopened stack should preserve LIFO order.");
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "persistence_after_pop", "Pops are persisted across instances", _ =>
                {
                    string file = NewFile(s);
                    using (PersistentStack<string> stack = new PersistentStack<string>(file))
                    {
                        stack.Push("First");
                        stack.Push("Second");
                        stack.Push("Third");
                    }

                    using (PersistentStack<string> second = new PersistentStack<string>(file))
                    {
                        AssertEquals("Third", second.Pop(), "Second instance should pop Third.");
                        second.Push("Fourth");
                    }

                    using (PersistentStack<string> third = new PersistentStack<string>(file))
                    {
                        List<string> drained = new List<string>();
                        while (third.Count > 0) drained.Add(third.Pop());
                        AssertSequenceEquals(new[] { "Fourth", "Second", "First" }, drained,
                            "Final state should reflect the pop and later push.");
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "concurrent_push_count", "Concurrent pushes all persist", async _ =>
                {
                    using PersistentStack<int> stack = new PersistentStack<int>(NewFile(s));
                    List<Task> tasks = new List<Task>();
                    for (int t = 0; t < 10; t++)
                    {
                        int taskId = t;
                        tasks.Add(Task.Run(() =>
                        {
                            for (int i = 0; i < 10; i++) stack.Push(taskId * 10 + i);
                        }));
                    }

                    await Task.WhenAll(tasks).ConfigureAwait(false);
                    AssertEquals(100, stack.Count, "All concurrent pushes should be counted.");
                }),

                new TestCaseDescriptor(s, "empty_to_array_is_empty", "ToArray on an empty stack returns an empty array", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    AssertEquals(0, stack.ToArray().Length, "ToArray on an empty stack should be empty.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "empty_enumeration_yields_nothing", "Enumerating an empty stack yields no items", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    AssertEquals(0, stack.ToList().Count, "An empty stack should enumerate no items.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "enumerator_is_snapshot", "An enumerator reflects the stack as it was when created", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    List<string> observed = new List<string>();
                    using (IEnumerator<string> e = stack.GetEnumerator())
                    {
                        stack.Push("Item3"); // mutate after acquiring the enumerator
                        while (e.MoveNext()) observed.Add(e.Current);
                    }
                    AssertSequenceEquals(new[] { "Item2", "Item1" }, observed,
                        "The enumerator should not observe items pushed after it was created (top first).");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "non_generic_copy_to", "The non-generic ICollection.CopyTo writes items top-first", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Push("Item2");
                    Array array = new string[2];
                    ((System.Collections.ICollection)stack).CopyTo(array, 0);
                    AssertEquals("Item2", (string)array.GetValue(0), "First element should be the top (Item2).");
                    AssertEquals("Item1", (string)array.GetValue(1), "Second element should be Item1.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "copy_to_null_throws", "CopyTo rejects a null array", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    AssertThrows<ArgumentNullException>(() => stack.CopyTo((string[])null, 0),
                        "CopyTo with a null array should throw ArgumentNullException.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "sync_primitives", "SyncRoot and IsSynchronized are exposed", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    Assert(stack.IsSynchronized, "IsSynchronized should be true.");
                    Assert(stack.SyncRoot != null, "SyncRoot should not be null.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "reusable_after_clear", "The stack is usable after Clear", _ =>
                {
                    using PersistentStack<string> stack = new PersistentStack<string>(NewFile(s));
                    stack.Push("Item1");
                    stack.Clear();
                    stack.Push("Item2");
                    AssertEquals(1, stack.Count, "Stack should accept items after Clear.");
                    AssertEquals("Item2", stack.Peek(), "Top after Clear should be the newly pushed item.");
                    return Task.CompletedTask;
                })
            };

            return new TestSuiteDescriptor(s, "PersistentStack contract", cases, CleanSuite(s), null);
        }
    }
}
