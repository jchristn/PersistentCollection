namespace Test.List
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using GetSomeInput;
    using PersistentCollection;

    public static class Program
    {
        private static bool _RunForever = true;
        private static PersistentList<string> _List = new PersistentList<string>("./temp/list.idx");
        private static string _LastFilePath = "./temp/list.idx";

        public static void Main(string[] args)
        {
            Directory.CreateDirectory("./temp/");
            Console.WriteLine("PersistentList Test Program");
            Console.WriteLine($"Using persistence file: {_LastFilePath}");

            Menu();

            while (_RunForever)
            {
                string input = Inputty.GetString("\nCommand [?/help]:", null, false);

                switch (input.ToLower())
                {
                    case "q":
                    case "quit":
                    case "exit":
                        _RunForever = false;
                        break;

                    case "?":
                    case "help":
                        Menu();
                        break;

                    case "cls":
                        Console.Clear();
                        break;

                    case "add":
                        Add();
                        break;

                    case "addbulk":
                        AddBulk();
                        break;

                    case "get":
                        Get();
                        break;

                    case "set":
                        Set();
                        break;

                    case "count":
                        Console.WriteLine($"List count: {_List.Count}");
                        break;

                    case "remove":
                        Remove();
                        break;

                    case "removeat":
                        RemoveAt();
                        break;

                    case "insert":
                        Insert();
                        break;

                    case "contains":
                        Contains();
                        break;

                    case "indexof":
                        IndexOf();
                        break;

                    case "copyto":
                        CopyTo();
                        break;

                    case "clear":
                        Clear();
                        break;

                    case "enumerate":
                        Enumerate();
                        break;

                    case "stats":
                        ShowStats();
                        break;

                    case "changefile":
                        ChangeFile();
                        break;

                    case "loadfile":
                        LoadFile();
                        break;

                    default:
                        Console.WriteLine("Unknown command. Type '?' for help.");
                        break;
                }
            }

            _List.Dispose();
            Console.WriteLine("List disposed, program terminated.");
        }

        private static void Menu()
        {
            Console.WriteLine("");
            Console.WriteLine("Available commands:");
            Console.WriteLine("   q/quit/exit    quit the program");
            Console.WriteLine("   ?/help         help, this menu");
            Console.WriteLine("   cls            clear the screen");
            Console.WriteLine("   add            add to the list");
            Console.WriteLine("   addbulk        add multiple items to the list");
            Console.WriteLine("   get            read from the list by index");
            Console.WriteLine("   set            set the value at a specific index");
            Console.WriteLine("   count          show the list count");
            Console.WriteLine("   remove         remove record from list by value");
            Console.WriteLine("   removeat       remove record from list by index");
            Console.WriteLine("   insert         insert an item at specific index");
            Console.WriteLine("   contains       check if list contains an item");
            Console.WriteLine("   indexof        find index of an item");
            Console.WriteLine("   copyto         copy list to an array");
            Console.WriteLine("   clear          empty the list");
            Console.WriteLine("   enumerate      iterate through all items in the list");
            Console.WriteLine("   stats          show information about the list");
            Console.WriteLine("   changefile     switch to a different persistence file");
            Console.WriteLine("   loadfile       load a new persistence file");
            Console.WriteLine("");
        }

        private static void Add()
        {
            string data = Inputty.GetString("Data:", null, true);
            if (String.IsNullOrEmpty(data)) return;

            _List.Add(data);
            Console.WriteLine($"Added item. New count: {_List.Count}");
        }

        private static void AddBulk()
        {
            int count = Inputty.GetInteger("Number of items to add:", 5, true, true);
            if (count <= 0) return;

            string prefix = Inputty.GetString("Prefix for items (optional):", "Item", true);

            for (int i = 0; i < count; i++)
            {
                string item = $"{prefix} {_List.Count + i + 1}";
                _List.Add(item);
                Console.WriteLine($"Added: {item}");
            }

            Console.WriteLine($"Added {count} items. New count: {_List.Count}");
        }

        private static void Get()
        {
            int idx = Inputty.GetInteger("Index:", 0, true, true);

            try
            {
                string data = _List[idx];
                Console.WriteLine($"Item at index {idx}: {data}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Set()
        {
            int idx = Inputty.GetInteger("Index:", 0, true, true);

            try
            {
                if (idx >= 0 && idx < _List.Count)
                {
                    string currentValue = _List[idx];
                    Console.WriteLine($"Current value: {currentValue}");

                    string data = Inputty.GetString("New data:", null, true);
                    if (String.IsNullOrEmpty(data)) return;

                    _List[idx] = data;
                    Console.WriteLine($"Updated item at index {idx}");
                }
                else
                {
                    Console.WriteLine("Index out of range");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Remove()
        {
            string item = Inputty.GetString("Item to remove:", null, true);
            if (String.IsNullOrEmpty(item)) return;

            try
            {
                bool removed = _List.Remove(item);
                if (removed)
                {
                    Console.WriteLine($"Item removed successfully. New count: {_List.Count}");
                }
                else
                {
                    Console.WriteLine("Item not found in the list");
                }
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
                if (index < _List.Count)
                {
                    string item = _List[index];
                    _List.RemoveAt(index);
                    Console.WriteLine($"Removed item '{item}' at index {index}. New count: {_List.Count}");
                }
                else
                {
                    Console.WriteLine("Index out of range");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Insert()
        {
            int index = Inputty.GetInteger("Index:", 0, true, true);
            if (index < 0 || index > _List.Count)
            {
                Console.WriteLine($"Index must be between 0 and {_List.Count}");
                return;
            }

            string data = Inputty.GetString("Data:", null, true);
            if (String.IsNullOrEmpty(data)) return;

            try
            {
                _List.Insert(index, data);
                Console.WriteLine($"Inserted '{data}' at index {index}. New count: {_List.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Contains()
        {
            string item = Inputty.GetString("Item to find:", null, true);
            if (String.IsNullOrEmpty(item)) return;

            bool contains = _List.Contains(item);
            Console.WriteLine($"List {(contains ? "contains" : "does not contain")} '{item}'");
        }

        private static void IndexOf()
        {
            string item = Inputty.GetString("Item to find index of:", null, true);
            if (String.IsNullOrEmpty(item)) return;

            int index = _List.IndexOf(item);
            if (index >= 0)
            {
                Console.WriteLine($"Item '{item}' found at index {index}");
            }
            else
            {
                Console.WriteLine($"Item '{item}' not found in the list");
            }
        }

        private static void CopyTo()
        {
            if (_List.Count == 0)
            {
                Console.WriteLine("List is empty, nothing to copy");
                return;
            }

            string[] array = new string[_List.Count];
            _List.CopyTo(array, 0);

            Console.WriteLine($"Copied {array.Length} items to array:");
            for (int i = 0; i < array.Length; i++)
            {
                Console.WriteLine($"[{i}]: {array[i]}");
            }
        }

        private static void Clear()
        {
            if (_List.Count == 0)
            {
                Console.WriteLine("List is already empty");
                return;
            }

            string confirm = Inputty.GetString($"Clear all {_List.Count} items? (y/n):", "n", true);
            if (confirm.ToLower() == "y")
            {
                _List.Clear();
                Console.WriteLine("List cleared");
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

        private static void ShowStats()
        {
            Console.WriteLine("\nList Statistics:");
            Console.WriteLine($"Count: {_List.Count}");
            Console.WriteLine($"IsReadOnly: {_List.IsReadOnly}");
            Console.WriteLine($"IsSynchronized: {_List.IsSynchronized}");
            Console.WriteLine($"Persistence file: {_LastFilePath}");

            if (_List.Count > 0)
            {
                Console.WriteLine($"First item: {_List[0]}");
                Console.WriteLine($"Last item: {_List[_List.Count - 1]}");
            }

            // Check if file exists
            bool fileExists = File.Exists(_LastFilePath);
            Console.WriteLine($"Persistence file exists: {fileExists}");

            if (fileExists)
            {
                long fileSize = new FileInfo(_LastFilePath).Length;
                Console.WriteLine($"Persistence file size: {fileSize} bytes");
            }
        }

        private static void ChangeFile()
        {
            string newFile = Inputty.GetString("New persistence file path:", "./temp/new_list.idx", true);
            if (String.IsNullOrEmpty(newFile)) return;

            try
            {
                // Dispose current list
                _List.Dispose();

                // Create directory if needed
                string dir = Path.GetDirectoryName(newFile);
                if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                {
                    Directory.CreateDirectory(dir);
                }

                // Create new list with new file
                _List = new PersistentList<string>(newFile);
                _LastFilePath = newFile;

                Console.WriteLine($"Switched to new persistence file: {newFile}");
                Console.WriteLine($"List count: {_List.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error switching files: {ex.Message}");

                // Try to recover by creating a new list with the original file
                _List = new PersistentList<string>(_LastFilePath);
            }
        }

        private static void LoadFile()
        {
            // First show available files in temp directory
            string[] files = Directory.GetFiles("./temp/", "*.idx");

            Console.WriteLine("\nAvailable persistence files:");
            for (int i = 0; i < files.Length; i++)
            {
                Console.WriteLine($"[{i}]: {files[i]}");
            }

            if (files.Length == 0)
            {
                Console.WriteLine("No persistence files found in ./temp/");
            }

            string filePath = Inputty.GetString("File path to load (or empty to cancel):", null, true);
            if (String.IsNullOrEmpty(filePath)) return;

            try
            {
                // Dispose current list
                _List.Dispose();

                // Create new list with the specified file
                _List = new PersistentList<string>(filePath);
                _LastFilePath = filePath;

                Console.WriteLine($"Loaded persistence file: {filePath}");
                Console.WriteLine($"List count: {_List.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading file: {ex.Message}");

                // Try to recover by creating a new list with the original file
                _List = new PersistentList<string>(_LastFilePath);
            }
        }
    }
}