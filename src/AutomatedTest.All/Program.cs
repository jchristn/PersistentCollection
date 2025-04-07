namespace AutomatedTest.All
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using PersistentCollection;

    class Program
    {
        // Store file paths as static variables for cleanup
        private static string listFile = "./persistentList.idx";
        private static string queueFile = "./persistentQueue.idx";
        private static string stackFile = "./persistentStack.idx";

        static void Main(string[] args)
        {
            Console.WriteLine("=== Persistent Collections Demo ===\n");

            try
            {
                DemonstratePersistentList(listFile);
                DemonstratePersistentQueue(queueFile);
                DemonstratePersistentStack(stackFile);
            }
            finally
            {
                // Clean up all test files
                Console.WriteLine("=== CLEANUP ===");
                CleanupFile(listFile);
                CleanupFile(queueFile);
                CleanupFile(stackFile);
            }

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }

        static void CleanupFile(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                    Console.WriteLine($"Deleted file: {filePath}");
                }
                else
                {
                    Console.WriteLine($"File not found: {filePath}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error deleting {filePath}: {ex.Message}");
            }
        }

        static void DemonstratePersistentList(string filePath)
        {
            Console.WriteLine("=== PERSISTENT LIST DEMONSTRATION ===");

            // First instance - add data
            Console.WriteLine("Creating first PersistentList instance and adding data...");
            using (var list = new PersistentList<string>(filePath))
            {
                // Clear any existing data
                list.Clear();

                // Add items
                Console.WriteLine("Adding items to list:");
                Console.WriteLine("  Adding: foo");
                list.Add("foo");
                Console.WriteLine("  Adding: bar");
                list.Add("bar");
                Console.WriteLine("  Adding: baz");
                list.Add("baz");

                Console.WriteLine($"Added 3 items to list. Count: {list.Count}");
            }
            Console.WriteLine("First instance disposed.");

            // Second instance - retrieve data
            Console.WriteLine("\nCreating second PersistentList instance to retrieve data...");
            using (var list = new PersistentList<string>(filePath))
            {
                Console.WriteLine($"Retrieved list with {list.Count} items:");

                int index = 0;
                foreach (var item in list)
                {
                    Console.WriteLine($"  [{index}]: {item}");
                    index++;
                }

                // Remove items
                Console.WriteLine("\nRemoving all items:");
                while (list.Count > 0)
                {
                    string item = list[0];
                    list.RemoveAt(0);
                    Console.WriteLine($"  Removed: {item}");
                }

                Console.WriteLine($"List count after removal: {list.Count}");
            }
            Console.WriteLine("Second instance disposed.\n");
        }

        static void DemonstratePersistentQueue(string filePath)
        {
            Console.WriteLine("=== PERSISTENT QUEUE DEMONSTRATION ===");

            // First instance - add data
            Console.WriteLine("Creating first PersistentQueue instance and adding data...");
            using (var queue = new PersistentQueue<string>(filePath))
            {
                // Clear any existing data
                queue.Clear();

                // Enqueue items
                Console.WriteLine("Enqueuing items to queue:");
                Console.WriteLine("  Enqueuing: foo");
                queue.Enqueue("foo");
                Console.WriteLine("  Enqueuing: bar");
                queue.Enqueue("bar");
                Console.WriteLine("  Enqueuing: baz");
                queue.Enqueue("baz");

                Console.WriteLine($"Enqueued 3 items to queue. Count: {queue.Count}");
            }
            Console.WriteLine("First instance disposed.");

            // Second instance - retrieve data
            Console.WriteLine("\nCreating second PersistentQueue instance to retrieve data...");
            using (var queue = new PersistentQueue<string>(filePath))
            {
                Console.WriteLine($"Retrieved queue with {queue.Count} items:");

                // Get an array of items to inspect them all
                string[] items = queue.ToArray();
                for (int i = 0; i < items.Length; i++)
                {
                    Console.WriteLine($"  [{i}]: {items[i]}");
                }

                // Dequeue all items
                Console.WriteLine("\nDequeuing items in FIFO order:");
                while (queue.Count > 0)
                {
                    string item = queue.Dequeue();
                    Console.WriteLine($"  Dequeued: {item}");
                }

                Console.WriteLine($"Queue count after dequeuing: {queue.Count}");
            }
            Console.WriteLine("Second instance disposed.\n");
        }

        static void DemonstratePersistentStack(string filePath)
        {
            Console.WriteLine("=== PERSISTENT STACK DEMONSTRATION ===");

            // First instance - add data
            Console.WriteLine("Creating first PersistentStack instance and adding data...");
            using (var stack = new PersistentStack<string>(filePath))
            {
                // Clear any existing data
                stack.Clear();

                // Push items
                Console.WriteLine("Pushing items to stack:");
                Console.WriteLine("  Pushing: foo");
                stack.Push("foo");
                Console.WriteLine("  Pushing: bar");
                stack.Push("bar");
                Console.WriteLine("  Pushing: baz");
                stack.Push("baz");

                Console.WriteLine($"Pushed 3 items to stack. Count: {stack.Count}");
            }
            Console.WriteLine("First instance disposed.");

            // Second instance - retrieve data
            Console.WriteLine("\nCreating second PersistentStack instance to retrieve data...");
            using (var stack = new PersistentStack<string>(filePath))
            {
                Console.WriteLine($"Retrieved stack with {stack.Count} items:");

                // Get an array of items to inspect them all (should be in LIFO order)
                string[] items = stack.ToArray();
                for (int i = 0; i < items.Length; i++)
                {
                    Console.WriteLine($"  [{i}]: {items[i]}");
                }

                // Pop all items
                Console.WriteLine("\nPopping items in LIFO order:");
                while (stack.Count > 0)
                {
                    string item = stack.Pop();
                    Console.WriteLine($"  Popped: {item}");
                }

                Console.WriteLine($"Stack count after popping: {stack.Count}");
            }
            Console.WriteLine("Second instance disposed.\n");
        }
    }
}