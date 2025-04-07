namespace Test.Dictionary
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
        private static PersistentDictionary<string, string> _Dictionary = new PersistentDictionary<string, string>("./temp/dictionary.idx");
        private static string _LastFilePath = "./temp/dictionary.idx";

        public static void Main(string[] args)
        {
            Directory.CreateDirectory("./temp/");
            Console.WriteLine("PersistentDictionary Test Program");
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

                    case "tryget":
                        TryGet();
                        break;

                    case "count":
                        Console.WriteLine($"Dictionary count: {_Dictionary.Count}");
                        break;

                    case "remove":
                        Remove();
                        break;

                    case "removepair":
                        RemovePair();
                        break;

                    case "contains":
                        Contains();
                        break;

                    case "containskey":
                        ContainsKey();
                        break;

                    case "copyto":
                        CopyTo();
                        break;

                    case "clear":
                        Clear();
                        break;

                    case "keys":
                        ShowKeys();
                        break;

                    case "values":
                        ShowValues();
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

            _Dictionary.Dispose();
            Console.WriteLine("Dictionary disposed, program terminated.");
        }

        private static void Menu()
        {
            Console.WriteLine("");
            Console.WriteLine("Available commands:");
            Console.WriteLine("   q/quit/exit    quit the program");
            Console.WriteLine("   ?/help         help, this menu");
            Console.WriteLine("   cls            clear the screen");
            Console.WriteLine("   add            add key-value pair to the dictionary");
            Console.WriteLine("   addbulk        add multiple key-value pairs to the dictionary");
            Console.WriteLine("   get            get a value by key");
            Console.WriteLine("   set            set a value for an existing key");
            Console.WriteLine("   tryget         try to get a value by key (no exceptions)");
            Console.WriteLine("   count          show the dictionary count");
            Console.WriteLine("   remove         remove entry by key");
            Console.WriteLine("   removepair     remove entry by key-value pair");
            Console.WriteLine("   contains       check if dictionary contains a key-value pair");
            Console.WriteLine("   containskey    check if dictionary contains a key");
            Console.WriteLine("   copyto         copy dictionary to an array");
            Console.WriteLine("   clear          empty the dictionary");
            Console.WriteLine("   keys           show all keys in the dictionary");
            Console.WriteLine("   values         show all values in the dictionary");
            Console.WriteLine("   enumerate      iterate through all entries in the dictionary");
            Console.WriteLine("   stats          show information about the dictionary");
            Console.WriteLine("   changefile     switch to a different persistence file");
            Console.WriteLine("   loadfile       load a new persistence file");
            Console.WriteLine("");
        }

        private static void Add()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            string value = Inputty.GetString("Value:", null, true);
            if (String.IsNullOrEmpty(value)) return;

            try
            {
                _Dictionary.Add(key, value);
                Console.WriteLine($"Added key-value pair. New count: {_Dictionary.Count}");
            }
            catch (ArgumentException)
            {
                Console.WriteLine($"Error: Key '{key}' already exists in the dictionary");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static void AddBulk()
        {
            int count = Inputty.GetInteger("Number of items to add:", 5, true, true);
            if (count <= 0) return;

            string keyPrefix = Inputty.GetString("Key prefix for items (optional):", "Key", true);
            string valuePrefix = Inputty.GetString("Value prefix for items (optional):", "Value", true);

            int addedCount = 0;
            for (int i = 0; i < count; i++)
            {
                string key = $"{keyPrefix}_{_Dictionary.Count + i + 1}";
                string value = $"{valuePrefix}_{_Dictionary.Count + i + 1}";

                try
                {
                    _Dictionary.Add(key, value);
                    Console.WriteLine($"Added: {key} => {value}");
                    addedCount++;
                }
                catch (ArgumentException)
                {
                    Console.WriteLine($"Skipped: Key '{key}' already exists");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error adding '{key}': {ex.Message}");
                }
            }

            Console.WriteLine($"Added {addedCount} key-value pairs. New count: {_Dictionary.Count}");
        }

        private static void Get()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            try
            {
                string value = _Dictionary[key];
                Console.WriteLine($"Value for key '{key}': {value}");
            }
            catch (KeyNotFoundException)
            {
                Console.WriteLine($"Key '{key}' not found in the dictionary");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void TryGet()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            if (_Dictionary.TryGetValue(key, out string value))
            {
                Console.WriteLine($"Value for key '{key}': {value}");
            }
            else
            {
                Console.WriteLine($"Key '{key}' not found in the dictionary");
            }
        }

        private static void Set()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            try
            {
                if (_Dictionary.ContainsKey(key))
                {
                    string currentValue = _Dictionary[key];
                    Console.WriteLine($"Current value for key '{key}': {currentValue}");

                    string newValue = Inputty.GetString("New value:", null, true);
                    if (String.IsNullOrEmpty(newValue)) return;

                    _Dictionary[key] = newValue;
                    Console.WriteLine($"Updated value for key '{key}'");
                }
                else
                {
                    Console.WriteLine($"Key '{key}' not found. Use 'add' to add a new key-value pair.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Remove()
        {
            string key = Inputty.GetString("Key to remove:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            try
            {
                bool removed = _Dictionary.Remove(key);
                if (removed)
                {
                    Console.WriteLine($"Key '{key}' removed successfully. New count: {_Dictionary.Count}");
                }
                else
                {
                    Console.WriteLine($"Key '{key}' not found in the dictionary");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void RemovePair()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            string value = Inputty.GetString("Value:", null, true);
            if (String.IsNullOrEmpty(value)) return;

            try
            {
                KeyValuePair<string, string> pair = new KeyValuePair<string, string>(key, value);
                bool removed = _Dictionary.Remove(pair);

                if (removed)
                {
                    Console.WriteLine($"Key-value pair removed successfully. New count: {_Dictionary.Count}");
                }
                else
                {
                    Console.WriteLine("Key-value pair not found in the dictionary");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void Contains()
        {
            string key = Inputty.GetString("Key:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            string value = Inputty.GetString("Value:", null, true);
            if (String.IsNullOrEmpty(value)) return;

            try
            {
                KeyValuePair<string, string> pair = new KeyValuePair<string, string>(key, value);
                bool contains = _Dictionary.Contains(pair);

                Console.WriteLine($"Dictionary {(contains ? "contains" : "does not contain")} the specified key-value pair");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        private static void ContainsKey()
        {
            string key = Inputty.GetString("Key to check:", null, true);
            if (String.IsNullOrEmpty(key)) return;

            bool contains = _Dictionary.ContainsKey(key);
            Console.WriteLine($"Dictionary {(contains ? "contains" : "does not contain")} key '{key}'");
        }

        private static void CopyTo()
        {
            if (_Dictionary.Count == 0)
            {
                Console.WriteLine("Dictionary is empty, nothing to copy");
                return;
            }

            KeyValuePair<string, string>[] array = new KeyValuePair<string, string>[_Dictionary.Count];
            _Dictionary.CopyTo(array, 0);

            Console.WriteLine($"Copied {array.Length} items to array:");
            for (int i = 0; i < array.Length; i++)
            {
                Console.WriteLine($"[{i}]: {array[i].Key} => {array[i].Value}");
            }
        }

        private static void Clear()
        {
            if (_Dictionary.Count == 0)
            {
                Console.WriteLine("Dictionary is already empty");
                return;
            }

            string confirm = Inputty.GetString($"Clear all {_Dictionary.Count} entries? (y/n):", "n", true);
            if (confirm.ToLower() == "y")
            {
                _Dictionary.Clear();
                Console.WriteLine("Dictionary cleared");
            }
        }

        private static void ShowKeys()
        {
            if (_Dictionary.Count == 0)
            {
                Console.WriteLine("Dictionary is empty");
                return;
            }

            Console.WriteLine($"Keys in dictionary ({_Dictionary.Count}):");
            int index = 0;
            foreach (string key in _Dictionary.Keys)
            {
                Console.WriteLine($"[{index}]: {key}");
                index++;
            }
        }

        private static void ShowValues()
        {
            if (_Dictionary.Count == 0)
            {
                Console.WriteLine("Dictionary is empty");
                return;
            }

            Console.WriteLine($"Values in dictionary ({_Dictionary.Count}):");
            int index = 0;
            foreach (string value in _Dictionary.Values)
            {
                Console.WriteLine($"[{index}]: {value}");
                index++;
            }
        }

        private static void Enumerate()
        {
            if (_Dictionary.Count == 0)
            {
                Console.WriteLine("Dictionary is empty");
                return;
            }

            Console.WriteLine("");
            Console.WriteLine($"Enumerating {_Dictionary.Count} key-value pairs:");

            int index = 0;
            foreach (KeyValuePair<string, string> pair in _Dictionary)
            {
                Console.WriteLine($"[{index}]: {pair.Key} => {pair.Value}");
                index++;
            }

            Console.WriteLine("");
        }

        private static void ShowStats()
        {
            Console.WriteLine("\nDictionary Statistics:");
            Console.WriteLine($"Count: {_Dictionary.Count}");
            Console.WriteLine($"IsReadOnly: {_Dictionary.IsReadOnly}");
            Console.WriteLine($"IsSynchronized: {_Dictionary.IsSynchronized}");
            Console.WriteLine($"Persistence file: {_LastFilePath}");

            if (_Dictionary.Count > 0)
            {
                Console.WriteLine($"First key: {_Dictionary.Keys.First()}");

                // Try to get first and last keys in alphabetical order
                try
                {
                    string[] sortedKeys = _Dictionary.Keys.OrderBy(k => k).ToArray();
                    Console.WriteLine($"First key (alphabetical): {sortedKeys.First()}");
                    Console.WriteLine($"Last key (alphabetical): {sortedKeys.Last()}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error sorting keys: {ex.Message}");
                }
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
            string newFile = Inputty.GetString("New persistence file path:", "./temp/new_dictionary.idx", true);
            if (String.IsNullOrEmpty(newFile)) return;

            try
            {
                // Dispose current dictionary
                _Dictionary.Dispose();

                // Create directory if needed
                string dir = Path.GetDirectoryName(newFile);
                if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                {
                    Directory.CreateDirectory(dir);
                }

                // Create new dictionary with new file
                _Dictionary = new PersistentDictionary<string, string>(newFile);
                _LastFilePath = newFile;

                Console.WriteLine($"Switched to new persistence file: {newFile}");
                Console.WriteLine($"Dictionary count: {_Dictionary.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error switching files: {ex.Message}");

                // Try to recover by creating a new dictionary with the original file
                _Dictionary = new PersistentDictionary<string, string>(_LastFilePath);
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
                // Dispose current dictionary
                _Dictionary.Dispose();

                // Create new dictionary with the specified file
                _Dictionary = new PersistentDictionary<string, string>(filePath);
                _LastFilePath = filePath;

                Console.WriteLine($"Loaded persistence file: {filePath}");
                Console.WriteLine($"Dictionary count: {_Dictionary.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading file: {ex.Message}");

                // Try to recover by creating a new dictionary with the original file
                _Dictionary = new PersistentDictionary<string, string>(_LastFilePath);
            }
        }
    }
}