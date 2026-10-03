namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using PersistentCollection;
    using Touchstone.Core;
    using static Test.Shared.TestSupport;
    using T = PersistentCollection.PersistentCollectionTelemetry;

    public static partial class PersistentCollectionSuites
    {
        private static TestSuiteDescriptor TelemetrySuite()
        {
            string s = "telemetry";

            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>
            {
                new TestCaseDescriptor(s, "names_are_stable", "Meter, source, and metric names are the documented public contract", _ =>
                {
                    AssertEquals("PersistentCollection", T.MeterName, "Meter name changed.");
                    AssertEquals("PersistentCollection", T.ActivitySourceName, "ActivitySource name changed.");
                    AssertEquals("persistentcollection.operation.duration", T.OperationDuration, "Operation duration name changed.");
                    AssertEquals("persistentcollection.lock.wait.duration", T.LockWaitDuration, "Lock wait name changed.");
                    AssertEquals("persistentcollection.persistence.duration", T.PersistenceDuration, "Persistence duration name changed.");
                    AssertEquals("persistentcollection.persistence.size", T.PersistenceSize, "Persistence size name changed.");
                    AssertEquals("persistentcollection.persistence.last_success", T.PersistenceLastSuccess, "Last success name changed.");
                    AssertEquals("persistentcollection.errors", T.Errors, "Errors name changed.");
                    AssertEquals("persistentcollection.items", T.Items, "Items name changed.");
                    AssertEquals("persistentcollection.instances", T.Instances, "Instances name changed.");
                    AssertEquals("persistentcollection.build.info", T.BuildInfo, "Build info name changed.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "no_listener_does_not_throw", "Every collection works with no listener subscribed", _ =>
                {
                    using (PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s)))
                    {
                        queue.Enqueue("a");
                        AssertEquals("a", queue.Dequeue(), "Queue should work without a listener.");
                        AssertEquals(false, queue.TryDequeue(out string _), "TryDequeue on empty should return false.");
                        queue.Clear();
                    }

                    using (PersistentStack<string> stack = new PersistentStack<string>(NewFile(s)))
                    {
                        stack.Push("a");
                        AssertEquals("a", stack.Pop(), "Stack should work without a listener.");
                    }

                    using (PersistentList<string> list = new PersistentList<string>(NewFile(s)))
                    {
                        list.Add("a");
                        AssertEquals(true, list.Remove("a"), "List should work without a listener.");
                    }

                    using (PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s)))
                    {
                        dict["k"] = "v";
                        AssertEquals("v", dict["k"], "Dictionary should work without a listener.");
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "queue_operations_emit_metrics", "Queue operations record duration, outcome, lock wait, persistence, and depth", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    using (PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s)))
                    {
                        queue.Enqueue("a");
                        queue.Enqueue("b");
                        queue.Dequeue();
                        queue.TryDequeue(out string _);
                        queue.TryDequeue(out string _);
                        AssertEquals(0, queue.Count, "Queue should be empty.");
                    }

                    AssertEquals(2, capture.Find(T.OperationDuration, T.CollectionTypeKey, "queue", T.OperationKey, "enqueue", T.OutcomeKey, "success").Count, "Two successful enqueue durations expected.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "queue", T.OperationKey, "dequeue", T.OutcomeKey, "success").Count, "One dequeue duration expected.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "queue", T.OperationKey, "try_dequeue", T.OutcomeKey, "success").Count, "One successful try_dequeue expected.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "queue", T.OperationKey, "try_dequeue", T.OutcomeKey, "miss").Count, "One try_dequeue miss expected (empty queue is not an error).");
                    AssertEquals(0, capture.Find(T.Errors, T.CollectionTypeKey, "queue").Count, "No errors expected.");
                    Assert(capture.Find(T.OperationDuration, T.OperationKey, "count").Count >= 1, "Count should be measured.");
                    Assert(capture.Find(T.OperationDuration).All(m => m.Value >= 0), "Durations must be non-negative seconds.");
                    Assert(capture.Find(T.LockWaitDuration, T.CollectionTypeKey, "queue", T.OperationKey, "enqueue").Count == 2, "Lock wait should be recorded per enqueue.");
                    AssertEquals(4, capture.Find(T.PersistenceDuration, T.CollectionTypeKey, "queue", T.OperationKey, "write", T.OutcomeKey, "success").Count, "Four persistence writes expected (2 enqueue, dequeue, try_dequeue).");
                    Assert(capture.Find(T.PersistenceSize, T.CollectionTypeKey, "queue", T.OperationKey, "write").All(m => m.Value > 0), "Written size must be positive bytes.");

                    List<CapturedMeasurement> items = capture.Find(T.Items, T.CollectionTypeKey, "queue");
                    AssertEquals(2.0, items.Where(m => m.Value > 0).Sum(m => m.Value), "Depth should rise by 2.");
                    AssertEquals(0.0, items.Sum(m => m.Value), "Depth should net to zero after draining.");

                    List<CapturedMeasurement> instances = capture.Find(T.Instances, T.CollectionTypeKey, "queue");
                    AssertEquals(0.0, instances.Sum(m => m.Value), "Instance count should net to zero after dispose.");
                    AssertEquals(1, instances.Count(m => m.Value > 0), "One instance opened.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "stack_operations_emit_metrics", "Stack push, pop, and try_pop miss are measured", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    using (PersistentStack<int> stack = new PersistentStack<int>(NewFile(s)))
                    {
                        stack.Push(1);
                        stack.Peek();
                        stack.Pop();
                        stack.TryPop(out int _);
                        stack.TryPeek(out int _);
                    }

                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "stack", T.OperationKey, "push", T.OutcomeKey, "success").Count, "Push measured.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "stack", T.OperationKey, "pop", T.OutcomeKey, "success").Count, "Pop measured.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "stack", T.OperationKey, "peek", T.OutcomeKey, "success").Count, "Peek measured.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "stack", T.OperationKey, "try_pop", T.OutcomeKey, "miss").Count, "TryPop miss measured.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "stack", T.OperationKey, "try_peek", T.OutcomeKey, "miss").Count, "TryPeek miss measured.");
                    AssertEquals(0.0, capture.Find(T.Items, T.CollectionTypeKey, "stack").Sum(m => m.Value), "Stack depth nets to zero.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "list_operations_emit_metrics", "List add, insert, set, remove, and remove miss are measured", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    using (PersistentList<string> list = new PersistentList<string>(NewFile(s)))
                    {
                        list.Add("a");
                        list.Insert(0, "b");
                        list[0] = "c";
                        list.Remove("zzz");
                        list.RemoveAt(0);
                        list.Clear();
                    }

                    foreach (string op in new[] { "add", "insert", "set", "remove_at", "clear" })
                    {
                        AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "list", T.OperationKey, op, T.OutcomeKey, "success").Count, "List " + op + " should be measured once.");
                    }

                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "list", T.OperationKey, "remove", T.OutcomeKey, "miss").Count, "Remove of a missing item is a miss.");
                    AssertEquals(1, capture.Find(T.PersistenceDuration, T.CollectionTypeKey, "list", T.OperationKey, "truncate", T.OutcomeKey, "success").Count, "Clear truncates the file.");
                    AssertEquals(0.0, capture.Find(T.Items, T.CollectionTypeKey, "list").Sum(m => m.Value), "List size nets to zero.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "dictionary_operations_emit_metrics", "Dictionary add, set, remove, and lookup misses are measured", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    using (PersistentDictionary<string, int> dict = new PersistentDictionary<string, int>(NewFile(s)))
                    {
                        dict.Add("a", 1);
                        dict["b"] = 2;
                        dict["b"] = 3;
                        dict.TryGetValue("missing", out int _);
                        dict.Remove("missing");
                        dict.Remove("a");
                        dict.ContainsKey("b");
                    }

                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "dictionary", T.OperationKey, "add", T.OutcomeKey, "success").Count, "Add measured.");
                    AssertEquals(2, capture.Find(T.OperationDuration, T.CollectionTypeKey, "dictionary", T.OperationKey, "set", T.OutcomeKey, "success").Count, "Set measured twice.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "dictionary", T.OperationKey, "try_get_value", T.OutcomeKey, "miss").Count, "TryGetValue miss measured.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "dictionary", T.OperationKey, "remove", T.OutcomeKey, "miss").Count, "Remove miss measured.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "dictionary", T.OperationKey, "remove", T.OutcomeKey, "success").Count, "Remove success measured.");
                    AssertEquals(1, capture.Find(T.OperationDuration, T.CollectionTypeKey, "dictionary", T.OperationKey, "contains_key", T.OutcomeKey, "success").Count, "ContainsKey measured.");

                    List<CapturedMeasurement> items = capture.Find(T.Items, T.CollectionTypeKey, "dictionary");
                    AssertEquals(2.0, items.Where(m => m.Value > 0).Sum(m => m.Value), "Only new keys add items (overwrite does not).");
                    AssertEquals(0.0, items.Sum(m => m.Value), "Dictionary size nets to zero after dispose.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "mutation_spans_nest_persistence", "A mutation emits an operation span with a child persistence span", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    string file = NewFile(s);
                    using (PersistentQueue<string> queue = new PersistentQueue<string>(file))
                    {
                        queue.Enqueue("a");
                        queue.Peek();
                    }

                    Activity op = capture.Spans("PersistentQueue enqueue").Single();
                    Activity write = capture.Spans("PersistentQueue persistence.write").Single();
                    AssertEquals(op.SpanId, write.ParentSpanId, "Persistence span should be a child of the operation span.");
                    AssertEquals(op.TraceId, write.TraceId, "Both spans share a trace.");
                    AssertEquals(ActivityStatusCode.Ok, op.Status, "Operation span status should be Ok.");
                    AssertEquals(ActivityStatusCode.Ok, write.Status, "Write span status should be Ok.");
                    AssertEquals("queue", op.GetTagItem(T.CollectionTypeKey) as string, "collection.type tag.");
                    AssertEquals("enqueue", op.GetTagItem(T.OperationKey) as string, "operation tag.");
                    AssertEquals("success", op.GetTagItem(T.OutcomeKey) as string, "outcome tag.");
                    Assert(op.GetTagItem(T.LockWaitKey) != null, "Lock wait should be on the span.");
                    AssertEquals(Path.GetFileName(file), write.GetTagItem(T.FileNameKey) as string, "file.name tag should be the file name only.");
                    Assert(Convert.ToInt64(write.GetTagItem(T.BytesKey)) > 0, "bytes tag should be positive.");
                    AssertEquals(1, Convert.ToInt32(write.GetTagItem(T.ItemCountKey)), "item_count tag.");
                    AssertEquals(0, capture.Spans("PersistentQueue peek").Count, "Read-only operations are metrics-only (no span).");
                    AssertEquals(1, capture.Spans("PersistentQueue open").Count, "Open emits a span.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "spans_join_ambient_trace", "Spans adopt the caller's Activity.Current (W3C context)", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    using Activity parent = new Activity("host-request").SetIdFormat(ActivityIdFormat.W3C).Start();
                    using (PersistentList<int> list = new PersistentList<int>(NewFile(s)))
                    {
                        list.Add(1);
                    }

                    parent.Stop();
                    Activity add = capture.Spans("PersistentList add").Single();
                    AssertEquals(parent.TraceId, add.TraceId, "Operation span should join the host trace.");
                    AssertEquals(parent.SpanId, add.ParentSpanId, "Operation span should be a child of the host span.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "operation_failure_is_recorded", "A throwing operation records outcome=error, error.type, the errors counter, and an Error span", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    using (PersistentStack<string> stack = new PersistentStack<string>(NewFile(s)))
                    {
                        AssertThrows<InvalidOperationException>(() => stack.Pop(), "Pop on empty should still throw.");
                    }

                    CapturedMeasurement duration = capture.Find(T.OperationDuration, T.CollectionTypeKey, "stack", T.OperationKey, "pop").Single();
                    AssertEquals("error", duration.Tags[T.OutcomeKey], "outcome should be error.");
                    AssertEquals("System.InvalidOperationException", duration.Tags[T.ErrorTypeKey], "error.type should be the exception type.");
                    AssertEquals(1, capture.Find(T.Errors, T.CollectionTypeKey, "stack", T.OperationKey, "pop", T.ErrorTypeKey, "System.InvalidOperationException").Count, "errors counter should increment.");

                    Activity span = capture.Spans("PersistentStack pop").Single();
                    AssertEquals(ActivityStatusCode.Error, span.Status, "Span status should be Error.");
                    Assert(span.Events.Any(e => e.Name == "exception"), "Span should carry an exception event.");
                    Assert(capture.Find(T.OperationDuration, T.OutcomeKey, "success").All(m => !m.Has(T.ErrorTypeKey)), "error.type only appears on errors.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "corrupt_file_on_open_is_recorded", "A corrupt persistence file fails open and records the read failure", _ =>
                {
                    string file = NewFile(s);
                    File.WriteAllText(file, "{ this is not json");

                    using TelemetryCapture capture = new TelemetryCapture();
                    bool threw = false;
                    try
                    {
                        new PersistentList<string>(file).Dispose();
                    }
                    catch (Exception)
                    {
                        threw = true;
                    }

                    Assert(threw, "Opening a corrupt file should still throw.");
                    CapturedMeasurement read = capture.Find(T.PersistenceDuration, T.CollectionTypeKey, "list", T.OperationKey, "read").Single();
                    AssertEquals("error", read.Tags[T.OutcomeKey], "read outcome should be error.");
                    Assert(read.Tags[T.ErrorTypeKey].Contains("Json"), "error.type should identify the JSON failure, was " + read.Tags[T.ErrorTypeKey] + ".");
                    AssertEquals(1, capture.Find(T.Errors, T.CollectionTypeKey, "list", T.OperationKey, "open").Count, "errors counter should count the failed open.");
                    AssertEquals(ActivityStatusCode.Error, capture.Spans("PersistentList open").Single().Status, "Open span should be Error.");
                    AssertEquals(ActivityStatusCode.Error, capture.Spans("PersistentList persistence.read").Single().Status, "Read span should be Error.");
                    AssertEquals(0, capture.Find(T.Instances, T.CollectionTypeKey, "list").Count, "A failed open is not a live instance.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "write_failure_is_recorded", "A disk write failure records the persistence error under the operation", _ =>
                {
                    // A directory at the persistence path makes every write fail.
                    string file = NewFile(s);
                    Directory.CreateDirectory(file);

                    using TelemetryCapture capture = new TelemetryCapture();
                    using (PersistentQueue<string> queue = new PersistentQueue<string>(file))
                    {
                        bool threw = false;
                        try { queue.Enqueue("a"); }
                        catch (Exception) { threw = true; }
                        Assert(threw, "Enqueue should throw when the file cannot be written.");

                        AssertEquals(false, queue.TryDequeue(out string _), "TryDequeue should swallow the write failure and return false.");
                    }

                    List<CapturedMeasurement> writes = capture.Find(T.PersistenceDuration, T.CollectionTypeKey, "queue", T.OperationKey, "write", T.OutcomeKey, "error");
                    AssertEquals(2, writes.Count, "Both failed writes should be recorded.");
                    AssertEquals(1, capture.Find(T.Errors, T.CollectionTypeKey, "queue", T.OperationKey, "enqueue").Count, "Enqueue error counted.");
                    AssertEquals(1, capture.Find(T.Errors, T.CollectionTypeKey, "queue", T.OperationKey, "try_dequeue").Count, "Swallowed TryDequeue failure still counted.");
                    AssertEquals(ActivityStatusCode.Error, capture.Spans("PersistentQueue try_dequeue").Single().Status, "TryDequeue span should be Error.");
                    Directory.Delete(file);
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "open_existing_file_records_read", "Reopening a persisted collection records the read size and restores depth", _ =>
                {
                    string file = NewFile(s);
                    using (PersistentQueue<string> seed = new PersistentQueue<string>(file))
                    {
                        seed.Enqueue("a");
                        seed.Enqueue("b");
                        seed.Enqueue("c");
                    }

                    using TelemetryCapture capture = new TelemetryCapture();
                    using (PersistentQueue<string> queue = new PersistentQueue<string>(file))
                    {
                        AssertEquals(3, queue.Count, "Reopened queue should have 3 items.");
                    }

                    CapturedMeasurement size = capture.Find(T.PersistenceSize, T.CollectionTypeKey, "queue", T.OperationKey, "read").Single();
                    AssertEquals((double)new FileInfo(file).Length, size.Value, "Read size should equal the file length.");
                    AssertEquals(1, capture.Find(T.PersistenceDuration, T.OperationKey, "read", T.OutcomeKey, "success").Count, "Read duration recorded.");
                    AssertEquals(3.0, capture.Find(T.Items, T.CollectionTypeKey, "queue").Where(m => m.Value > 0).Sum(m => m.Value), "Depth restored on open.");
                    AssertEquals(3, Convert.ToInt32(capture.Spans("PersistentQueue open").Single().GetTagItem(T.ItemCountKey)), "Open span carries the item count.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "gauges_report_build_and_last_success", "Build info and last-success gauges are observable", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    using (PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s)))
                    {
                        dict["k"] = "v";
                    }

                    capture.CollectObservables();

                    CapturedMeasurement build = capture.Find(T.BuildInfo).Single();
                    AssertEquals(1.0, build.Value, "build.info is always 1.");
                    Assert(!String.IsNullOrEmpty(build.Tags[T.VersionKey]) && !build.Tags[T.VersionKey].Contains("+"), "version label should be set without commit metadata.");

                    CapturedMeasurement last = capture.Find(T.PersistenceLastSuccess, T.CollectionTypeKey, "dictionary").Single();
                    double now = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
                    Assert(Math.Abs(now - last.Value) < 60, "Last success should be a recent Unix time in seconds.");
                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "labels_are_bounded", "Metric labels never carry file paths, keys, or values", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    string file = NewFile(s);
                    using (PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(file))
                    {
                        dict["secret-key"] = "secret-value";
                    }

                    HashSet<string> allowed = new HashSet<string> { T.CollectionTypeKey, T.OperationKey, T.OutcomeKey, T.ErrorTypeKey, T.VersionKey };
                    foreach (CapturedMeasurement m in capture.Measurements)
                    {
                        foreach (KeyValuePair<string, string> tag in m.Tags)
                        {
                            Assert(allowed.Contains(tag.Key), "Unexpected metric label " + tag.Key + " on " + m.Instrument + ".");
                            Assert(tag.Value == null || (!tag.Value.Contains("secret") && !tag.Value.Contains(Path.GetFileName(file))), "Label value leaked data on " + m.Instrument + ".");
                        }
                    }

                    return Task.CompletedTask;
                }),

                new TestCaseDescriptor(s, "throwing_listener_never_breaks_operations", "A listener that throws on every measurement does not affect collection behavior", _ =>
                {
                    using TelemetryCapture capture = new TelemetryCapture();
                    capture.ThrowOnMeasurement = true;

                    using (PersistentQueue<string> queue = new PersistentQueue<string>(NewFile(s)))
                    {
                        queue.Enqueue("a");
                        AssertEquals(1, queue.Count, "Count should work.");
                        AssertEquals("a", queue.Dequeue(), "Dequeue should work.");
                        AssertEquals(false, queue.TryDequeue(out string _), "TryDequeue should return false on empty.");
                    }

                    using (PersistentDictionary<string, string> dict = new PersistentDictionary<string, string>(NewFile(s)))
                    {
                        dict["k"] = "v";
                        AssertEquals("v", dict["k"], "Dictionary should work.");
                    }

                    return Task.CompletedTask;
                })
            };

            return new TestSuiteDescriptor(s, "Telemetry emission", cases, CleanSuite(s), null);
        }
    }
}
