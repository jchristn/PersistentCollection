namespace Test.Stack
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using GetSomeInput;
    using PersistentCollection;

    public static class Program
    {
        private static bool _RunForever = true;
        private static PersistentStack<string> _Stack = new PersistentStack<string>("./temp/", true);

        public static void Main(string[] args)
        {
            _Stack.DataAdded += (s, key) => Console.WriteLine("Data pushed: " + key);
            _Stack.DataRemoved += (s, key) => Console.WriteLine("Data removed: " + key);
            _Stack.DataUpdated += (s, key) => Console.WriteLine("Data updated: " + key);
            _Stack.Cleared += (s, _) => Console.WriteLine("Stack cleared");

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

                    case "push":
                        Push();
                        break;

                    case "pop":
                        Pop();
                        break;

                    case "popat":
                        PopAt();
                        break;

                    case "depth":
                        Console.WriteLine(_Stack.Count);
                        break;

                    case "length":
                        Console.WriteLine(_Stack.Length + " bytes");
                        break;

                    case "purge":
                        Purge();
                        break;

                    case "removeat":
                        RemoveAt();
                        break;

                    case "clear":
                        _Stack.Clear();
                        break;

                    case "enumerate":
                        Enumerate();
                        break;

                    case "contains":
                        Contains();
                        break;

                    case "containskey":
                        ContainsKey();
                        break;

                    case "containsindex":
                        ContainsIndex();
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

                    case "updatebykey":
                        UpdateByKey();
                        break;

                    case "updateat":
                        UpdateAt();
                        break;

                    case "toarray":
                        ToArray();
                        break;

                    case "keys":
                        ListKeys();
                        break;
                }
            }

            _Stack.Dispose();
        }

        private static void Menu()
        {
            Console.WriteLine("");
            Console.WriteLine("Available commands:");
            Console.WriteLine("   q               quit");
            Console.WriteLine("   ?               help, this menu");
            Console.WriteLine("   cls             clear the screen");
            Console.WriteLine("   push            add to the stack");
            Console.WriteLine("   pop             read from the stack (top item)");
            Console.WriteLine("   popat           read from the stack at specific index");
            Console.WriteLine("   peek            view top item without removing it");
            Console.WriteLine("   peekat          view item at specific index without removing it");
            Console.WriteLine("   getbyindex      get item by index (using indexer)");
            Console.WriteLine("   depth           show the stack depth");
            Console.WriteLine("   length          show the stack length in bytes");
            Console.WriteLine("   purge           purge record from stack by key");
            Console.WriteLine("   removeat        remove record from stack by index");
            Console.WriteLine("   expire          expire a record and purge it from the stack");
            Console.WriteLine("   getexp          retrieve the expiration for a given record");
            Console.WriteLine("   clear           empty the stack");
            Console.WriteLine("   enumerate       list all items in the stack");
            Console.WriteLine("   contains        check if stack contains a value");
            Console.WriteLine("   containskey     check if stack contains a key");
            Console.WriteLine("   containsindex   check if stack contains an index");
            Console.WriteLine("   updatebykey     update an item by key");
            Console.WriteLine("   updateat        update an item by index");
            Console.WriteLine("   toarray         display the stack as an array");
            Console.WriteLine("   keys            list all keys in the stack");
            Console.WriteLine("");
        }

        private static void Push()
        {
            string data = Inputty.GetString("Data:", null, true);
            if (String.IsNullOrEmpty(data)) return;

            string key = _Stack.Push(data);
            Console.WriteLine("Key: " + key);
        }

        private static void Pop()
        {
            try
            {
                string data = _Stack.Pop();
                Console.WriteLine("Popped: " + data);
            }
            catch (InvalidOperationException)
            {
                Console.WriteLine("Stack is empty");
            }
        }

        private static void PopAt()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0) return;

            bool remove = Inputty.GetBoolean("Remove item?", true);

            try
            {
                string data = _Stack.PopAt(index, remove);
                Console.WriteLine($"Item at index {index}: {data}");
                if (remove) Console.WriteLine("Item was removed from stack");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Peek()
        {
            try
            {
                string data = _Stack.Peek();
                Console.WriteLine("Peeked: " + data);
            }
            catch (InvalidOperationException)
            {
                Console.WriteLine("Stack is empty");
            }
        }

        private static void PeekAt()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0) return;

            try
            {
                string data = _Stack.PeekAt(index);
                Console.WriteLine($"Item at index {index}: {data}");
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
                string data = _Stack[index];
                Console.WriteLine($"Item at index {index}: {data}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void UpdateByKey()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            try
            {
                if (!_Stack.ContainsKey(key))
                {
                    Console.WriteLine("Key not found in stack");
                    return;
                }

                string newData = Inputty.GetString("New data:", null, true);
                if (String.IsNullOrEmpty(newData)) return;

                _Stack.UpdateByKey(key, newData);
                Console.WriteLine($"Item with key {key} updated successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void UpdateAt()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0) return;

            try
            {
                if (!_Stack.ContainsIndex(index))
                {
                    Console.WriteLine("Index not found in stack");
                    return;
                }

                string newData = Inputty.GetString("New data:", null, true);
                if (String.IsNullOrEmpty(newData)) return;

                _Stack.UpdateAt(index, newData);
                Console.WriteLine($"Item at index {index} updated successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Purge()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            try
            {
                _Stack.Purge(key);
                Console.WriteLine($"Key {key} purged from stack");
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
                _Stack.RemoveAt(index);
                Console.WriteLine($"Item at index {index} removed from stack");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Enumerate()
        {
            Console.WriteLine("Items in stack (newest to oldest):");
            int count = 0;

            foreach (string item in _Stack)
            {
                Console.WriteLine($"{count}. {item}");
                count++;
            }

            if (count == 0)
            {
                Console.WriteLine("Stack is empty");
            }
        }

        private static void Contains()
        {
            string searchItem = Inputty.GetString("Search for:", null, true);
            if (String.IsNullOrEmpty(searchItem)) return;

            bool contains = _Stack.Contains(searchItem);
            Console.WriteLine(contains ? "Item found in stack" : "Item not found in stack");
        }

        private static void ContainsKey()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            bool contains = _Stack.ContainsKey(key);
            Console.WriteLine(contains ? "Key found in stack" : "Key not found in stack");
        }

        private static void ContainsIndex()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0) return;

            bool contains = _Stack.ContainsIndex(index);
            Console.WriteLine(contains ? $"Index {index} exists in stack" : $"Index {index} does not exist in stack");
        }

        private static void ToArray()
        {
            string[] array = _Stack.ToArray();

            if (array.Length == 0)
            {
                Console.WriteLine("Stack is empty");
                return;
            }

            Console.WriteLine("Stack as array (newest to oldest):");
            for (int i = 0; i < array.Length; i++)
            {
                Console.WriteLine($"[{i}]: {array[i]}");
            }
        }

        private static void ListKeys()
        {
            List<string> keys = _Stack.GetKeys();

            if (keys.Count == 0)
            {
                Console.WriteLine("Stack is empty");
                return;
            }

            Console.WriteLine("");
            Console.WriteLine($"Listing {keys.Count} keys in stack (newest to oldest):");

            for (int i = 0; i < keys.Count; i++)
            {
                string key = keys[i];
                Console.WriteLine($"[{i}]");
            }

            Console.WriteLine("");
        }
    }
}