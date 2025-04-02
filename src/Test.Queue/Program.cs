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
        private static PersistentQueue<string> _Queue = new PersistentQueue<string>("./temp/", true);

        public static void Main(string[] args)
        {
            _Queue.DataEnqueued += (s, key) => Console.WriteLine("Data enqueued: " + key);
            _Queue.DataDequeued += (s, key) => Console.WriteLine("Data dequeued: " + key);
            _Queue.Cleared += (s, _) => Console.WriteLine("Queue cleared");

            while (_RunForever)
            {
                string input = Inputty.GetString("Command [?/help]:", null, false);

                switch (input)
                {
                    case "q":
                        _RunForever = false;
                        break;

                    case "?":
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

                    case "dequeuebykey":
                        DequeueByKey();
                        break;

                    case "dequeueat":
                        DequeueAt();
                        break;

                    case "peek":
                        Peek();
                        break;

                    case "peekat":
                        PeekAt();
                        break;

                    case "getbyindex":
                        GetByIndex();
                        break;

                    case "count":
                        Console.WriteLine(_Queue.Count);
                        break;

                    case "length":
                        Console.WriteLine(_Queue.Length + " bytes");
                        break;

                    case "remove":
                        Remove();
                        break;

                    case "removeat":
                        RemoveAt();
                        break;

                    case "enumerate":
                        Enumerate();
                        break;

                    case "keys":
                        ListKeys();
                        break;

                    case "clear":
                        _Queue.Clear();
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
            Console.WriteLine("   dequeuebykey    read and remove item with specific key from the queue");
            Console.WriteLine("   dequeueat       read and remove item at specific index from the queue");
            Console.WriteLine("   peek            read oldest item without removing");
            Console.WriteLine("   peekat          read item at specific index without removing");
            Console.WriteLine("   getbyindex      read item at specific index (same as peekat)");
            Console.WriteLine("   count           show the queue count");
            Console.WriteLine("   length          show the queue length in bytes");
            Console.WriteLine("   remove          remove record from queue by key");
            Console.WriteLine("   removeat        remove record from queue by index");
            Console.WriteLine("   expire          expire a record and remove it from the queue");
            Console.WriteLine("   getexp          retrieve the expiration for a given record");
            Console.WriteLine("   enumerate       list all items in the queue");
            Console.WriteLine("   keys            list all keys in the queue");
            Console.WriteLine("   clear           empty the queue");
            Console.WriteLine("");
        }

        private static void Enqueue()
        {
            string data = Inputty.GetString("Data:", null, true);
            if (String.IsNullOrEmpty(data)) return;

            string key = _Queue.Enqueue(data);
            Console.WriteLine("Key: " + key);
        }

        private static void Dequeue()
        {
            string msg = _Queue.Dequeue();
            if (String.IsNullOrEmpty(msg))
            {
                Console.WriteLine("Queue is empty");
                return;
            }
            Console.WriteLine(msg);
        }

        private static void DequeueByKey()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            try
            {
                string msg = _Queue.Dequeue(key, true);
                if (String.IsNullOrEmpty(msg))
                {
                    Console.WriteLine("No data found for key");
                    return;
                }
                Console.WriteLine(msg);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void DequeueAt()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0) return;

            try
            {
                string msg = _Queue.DequeueAt(index, true);
                if (String.IsNullOrEmpty(msg))
                {
                    Console.WriteLine("No data found at index");
                    return;
                }
                Console.WriteLine(msg);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Peek()
        {
            string msg = _Queue.Peek();
            if (String.IsNullOrEmpty(msg))
            {
                Console.WriteLine("Queue is empty");
                return;
            }
            Console.WriteLine(msg);
        }

        private static void PeekAt()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0) return;

            try
            {
                string msg = _Queue.PeekAt(index);
                if (String.IsNullOrEmpty(msg))
                {
                    Console.WriteLine("No data found at index");
                    return;
                }
                Console.WriteLine(msg);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void GetByIndex()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0) return;

            try
            {
                string msg = _Queue[index];
                if (String.IsNullOrEmpty(msg))
                {
                    Console.WriteLine("No data found at index");
                    return;
                }
                Console.WriteLine(msg);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Remove()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            try
            {
                _Queue.Remove(key);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void RemoveAt()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0) return;

            try
            {
                _Queue.RemoveAt(index);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
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

        private static void ListKeys()
        {
            List<string> keys = _Queue.GetKeys();

            if (keys.Count == 0)
            {
                Console.WriteLine("Queue is empty");
                return;
            }

            Console.WriteLine("");
            Console.WriteLine($"Listing {keys.Count} keys in queue (index order):");

            for (int i = 0; i < keys.Count; i++)
            {
                string key = keys[i];
                Console.WriteLine($"[{i}]: {key}");
            }

            Console.WriteLine("");
        }
    }
}