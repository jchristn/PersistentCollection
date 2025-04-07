namespace Test.Queue
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using GetSomeInput;
    using PersistentCollection;

    public static class Program
    {
        private static bool _RunForever = true;
        private static PersistentQueue<string> _Queue = new PersistentQueue<string>("./temp/queue.idx");

        public static void Main(string[] args)
        {
            Console.WriteLine("PersistentQueue Test Application");
            Console.WriteLine("Queue file: ./temp/queue.idx");
            Console.WriteLine("Type '?' for help menu");
            Console.WriteLine();

            while (_RunForever)
            {
                string input = Inputty.GetString("Command [?/help]:", null, false);

                switch (input)
                {
                    case "q":
                        _RunForever = false;
                        break;

                    case "?":
                    case "help":
                        Menu();
                        break;

                    case "cls":
                        Console.Clear();
                        break;

                    case "enqueue":
                        Enqueue();
                        break;

                    case "dequeue":
                        Dequeue();
                        break;

                    case "peek":
                        Peek();
                        break;

                    case "count":
                        Console.WriteLine($"Count: {_Queue.Count}");
                        break;

                    case "trydequeue":
                        TryDequeue();
                        break;

                    case "trypeek":
                        TryPeek();
                        break;

                    case "contains":
                        Contains();
                        break;

                    case "enumerate":
                        Enumerate();
                        break;

                    case "toarray":
                        ToArray();
                        break;

                    case "clear":
                        _Queue.Clear();
                        Console.WriteLine("Queue cleared");
                        break;

                    default:
                        Console.WriteLine("Unknown command. Type '?' for help.");
                        break;
                }
            }

            _Queue.Dispose();
        }

        private static void Menu()
        {
            Console.WriteLine("");
            Console.WriteLine("Available commands:");
            Console.WriteLine("   q               quit");
            Console.WriteLine("   ?               help, this menu");
            Console.WriteLine("   cls             clear the screen");
            Console.WriteLine("   enqueue         add to the queue");
            Console.WriteLine("   dequeue         read and remove oldest item from the queue");
            Console.WriteLine("   peek            read oldest item without removing");
            Console.WriteLine("   count           show the queue count");
            Console.WriteLine("   trydequeue      try to dequeue an item");
            Console.WriteLine("   trypeek         try to peek at the next item");
            Console.WriteLine("   contains        check if an item exists in the queue");
            Console.WriteLine("   enumerate       list all items in the queue");
            Console.WriteLine("   toarray         convert queue to array and display items");
            Console.WriteLine("   clear           empty the queue");
            Console.WriteLine("");
        }

        private static void Enqueue()
        {
            string data = Inputty.GetString("Data:", null, true);
            if (String.IsNullOrEmpty(data)) return;

            _Queue.Enqueue(data);
            Console.WriteLine("Item enqueued successfully");
        }

        private static void Dequeue()
        {
            try
            {
                string msg = _Queue.Dequeue();
                Console.WriteLine($"Dequeued: {msg}");
            }
            catch (InvalidOperationException)
            {
                Console.WriteLine("Queue is empty");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static void Peek()
        {
            try
            {
                string msg = _Queue.Peek();
                Console.WriteLine($"Peek: {msg}");
            }
            catch (InvalidOperationException)
            {
                Console.WriteLine("Queue is empty");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static void TryDequeue()
        {
            if (_Queue.TryDequeue(out string result))
            {
                Console.WriteLine($"Dequeued: {result}");
            }
            else
            {
                Console.WriteLine("Queue is empty");
            }
        }

        private static void TryPeek()
        {
            if (_Queue.TryPeek(out string result))
            {
                Console.WriteLine($"Peek: {result}");
            }
            else
            {
                Console.WriteLine("Queue is empty");
            }
        }

        private static void Contains()
        {
            string data = Inputty.GetString("Data to check:", null, true);
            if (String.IsNullOrEmpty(data)) return;

            bool contains = _Queue.Contains(data);
            Console.WriteLine($"Queue {(contains ? "contains" : "does not contain")} '{data}'");
        }

        private static void Enumerate()
        {
            Console.WriteLine("");

            if (_Queue.Count == 0)
            {
                Console.WriteLine("Queue is empty");
                return;
            }

            Console.WriteLine($"Listing {_Queue.Count} items in queue:");

            int index = 0;
            foreach (string item in _Queue)
            {
                Console.WriteLine($"[{index}]: {item}");
                index++;
            }

            Console.WriteLine("");
        }

        private static void ToArray()
        {
            Console.WriteLine("");

            if (_Queue.Count == 0)
            {
                Console.WriteLine("Queue is empty");
                return;
            }

            string[] items = _Queue.ToArray();
            Console.WriteLine($"Listing {items.Length} items in queue:");

            for (int i = 0; i < items.Length; i++)
            {
                Console.WriteLine($"[{i}]: {items[i]}");
            }

            Console.WriteLine("");
        }
    }
}