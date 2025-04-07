namespace Test.List
{
    using System;
    using System.Text;
    using GetSomeInput;
    using PersistentCollection;

    public static class Program
    {
        private static bool _RunForever = true;
        private static PersistentList<string> _List = new PersistentList<string>("./temp/");

        public static void Main(string[] args)
        {
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

                    case "add":
                        Add();
                        break;

                    case "get":
                        Get();
                        break;

                    case "set":
                        Set();
                        break;

                    case "count":
                        Console.WriteLine(_List.Count);
                        break;

                    case "remove":
                        Remove();
                        break;

                    case "removeat":
                        RemoveAt();
                        break;

                    case "clear":
                        _List.Clear();
                        break;

                    case "enumerate":
                        Enumerate();
                        break;
                }
            }

            _List.Dispose();
        }

        private static void Menu()
        {
            Console.WriteLine("");
            Console.WriteLine("Available commands:");
            Console.WriteLine("   q             quit");
            Console.WriteLine("   ?             help, this menu");
            Console.WriteLine("   cls           clear the screen");
            Console.WriteLine("   add           add to the list");
            Console.WriteLine("   get           read from the list by index");
            Console.WriteLine("   set           set the value at a specific index");
            Console.WriteLine("   count         show the list count");
            Console.WriteLine("   remove        remove record from list by key");
            Console.WriteLine("   removeat      remove record from list by index");
            Console.WriteLine("   clear         empty the list");
            Console.WriteLine("   enumerate     iterate through all items in the list");
            Console.WriteLine("");
        }

        private static void Add()
        {
            string data = Inputty.GetString("Data:", null, true);
            if (String.IsNullOrEmpty(data)) return;
            _List.Add(data);
        }

        private static void Get()
        {
            int idx = Inputty.GetInteger("Index:", 0, true, true);

            try
            {
                string data = _List[idx];
                Console.WriteLine(data);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Set()
        {
            int idx = Inputty.GetInteger("Index:", 0, true, true);
            string data = Inputty.GetString("Data :", null, true);
            if (String.IsNullOrEmpty(data)) return;

            try
            {
                _List[idx] = data;
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
                _List.Remove(key);
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
                _List.RemoveAt(index);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void UpdateByIndex()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0) return;

            try
            {
                if (index >= _List.Count)
                {
                    Console.WriteLine("Index out of range");
                    return;
                }

                string data = Inputty.GetString("New data:", null, true);
                if (String.IsNullOrEmpty(data)) return;

                _List[index] = data;
                Console.WriteLine($"Item at index {index} updated successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Enumerate()
        {
            if (_List.Count == 0)
            {
                Console.WriteLine("List is empty");
                return;
            }

            Console.WriteLine("");
            Console.WriteLine($"Enumerating {_List.Count} items:");

            int index = 0;
            foreach (string item in _List)
            {
                Console.WriteLine($"[{index}]: {item}");
                index++;
            }

            Console.WriteLine("");
        }
    }
}