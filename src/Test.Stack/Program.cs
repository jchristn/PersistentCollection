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
        private static PersistentStack<string> _Stack = new PersistentStack<string>("./temp/stack.idx");

        public static void Main(string[] args)
        {
            Console.WriteLine("PersistentStack Test Application");
            Console.WriteLine("Stack file: ./temp/stack.idx");
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

                    case "push":
                        Push();
                        break;

                    case "pop":
                        Pop();
                        break;

                    case "peek":
                        Peek();
                        break;

                    case "depth":
                    case "count":
                        Console.WriteLine($"Stack depth: {_Stack.Count}");
                        break;

                    case "clear":
                        _Stack.Clear();
                        Console.WriteLine("Stack cleared");
                        break;

                    case "enumerate":
                        Enumerate();
                        break;

                    case "contains":
                        Contains();
                        break;

                    case "trypop":
                        TryPop();
                        break;

                    case "trypeek":
                        TryPeek();
                        break;

                    case "toarray":
                        ToArray();
                        break;

                    case "copyto":
                        CopyTo();
                        break;

                    default:
                        Console.WriteLine("Unknown command. Type '?' for help.");
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
            Console.WriteLine("   peek            view top item without removing it");
            Console.WriteLine("   trypop          safely attempt to pop an item");
            Console.WriteLine("   trypeek         safely attempt to peek at the top item");
            Console.WriteLine("   depth/count     show the stack depth");
            Console.WriteLine("   clear           empty the stack");
            Console.WriteLine("   enumerate       list all items in the stack");
            Console.WriteLine("   contains        check if stack contains a value");
            Console.WriteLine("   toarray         display the stack as an array");
            Console.WriteLine("   copyto          copy stack to an array");
            Console.WriteLine("");
        }

        private static void Push()
        {
            string data = Inputty.GetString("Data:", null, true);
            if (String.IsNullOrEmpty(data)) return;

            _Stack.Push(data);
            Console.WriteLine("Item pushed to stack");
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

        private static void TryPop()
        {
            if (_Stack.TryPop(out string result))
            {
                Console.WriteLine("Popped: " + result);
            }
            else
            {
                Console.WriteLine("Stack is empty");
            }
        }

        private static void TryPeek()
        {
            if (_Stack.TryPeek(out string result))
            {
                Console.WriteLine("Peeked: " + result);
            }
            else
            {
                Console.WriteLine("Stack is empty");
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

        private static void CopyTo()
        {
            if (_Stack.Count == 0)
            {
                Console.WriteLine("Stack is empty");
                return;
            }

            int arraySize = Inputty.GetInteger("Array size (must be >= stack size):", _Stack.Count, true, true);
            if (arraySize < _Stack.Count)
            {
                Console.WriteLine("Array size must be at least equal to the stack size");
                return;
            }

            int startIndex = Inputty.GetInteger("Start index:", 0, true, true);
            if (startIndex < 0 || startIndex + _Stack.Count > arraySize)
            {
                Console.WriteLine("Invalid start index. Stack items must fit in the array");
                return;
            }

            string[] array = new string[arraySize];

            // Initialize array with placeholder values
            for (int i = 0; i < arraySize; i++)
            {
                array[i] = "[empty]";
            }

            _Stack.CopyTo(array, startIndex);

            Console.WriteLine("Array after CopyTo operation:");
            for (int i = 0; i < array.Length; i++)
            {
                Console.WriteLine($"[{i}]: {array[i]}");
            }
        }
    }
}