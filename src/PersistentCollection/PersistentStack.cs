namespace PersistentCollection
{
    using SerializationHelper;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Persistent stack.  Queued entries are backed on disk.
    /// Data is popped from the stack in a last-in-first-out manner.
    /// Provides index-based access to stack elements.
    /// </summary>
    public class PersistentStack<T> : IDisposable, IEnumerable<T>, IReadOnlyCollection<T>
    {
        #region Public-Members

        /// <summary>
        /// Number of entries waiting in the stack.
        /// </summary>
        public int Count
        {
            get
            {
                _Semaphore.Wait();

                try
                {
                    string[] files = Directory.GetFiles(_Directory, "*", SearchOption.TopDirectoryOnly);
                    if (files != null)
                    {
                        files = files.Where(f => !Path.GetFileName(f).Equals(_IndexFile)).ToArray();
                        return files.Length;
                    }
                    else
                    {
                        return 0;
                    }
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        /// <summary>
        /// Number of bytes waiting in the stack.
        /// </summary>
        public long Length
        {
            get
            {
                _Semaphore.Wait();

                try
                {
                    return Task.Run(() =>
                    {
                        IEnumerable<FileInfo> files = _DirectoryInfo.EnumerateFiles("*", SearchOption.AllDirectories).ToArray();
                        if (files != null && files.Count() > 0)
                        {
                            files = files.Where(f => !Path.GetFileName(f.Name).Equals(_IndexFile));
                            return files.Sum(f => f.Length);
                        }
                        else
                        {
                            return 0;
                        }

                    }).Result;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        /// <summary>
        /// Gets or sets the element at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the element to get or set.</param>
        /// <returns>The element at the specified index.</returns>
        public T this[int index]
        {
            get
            {
                byte[] data = GetBytes(index);
                try
                {
                    return Serializer.DeserializeJson<T>(data);
                }
                catch (System.Text.Json.JsonException)
                {
                    // If T is string, try treating it as a raw string
                    if (typeof(T) == typeof(string))
                    {
                        return (T)(object)Encoding.UTF8.GetString(data);
                    }
                    throw; // Re-throw for non-string types
                }
            }
            set
            {
                string key = GetKeyByIndex(index);
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

                string json = Serializer.SerializeJson(value, false);
                byte[] data = Encoding.UTF8.GetBytes(json);
                UpdateByKey(key, data);
            }
        }

        /// <summary>
        /// Event handler for when data is added to the stack.
        /// </summary>
        public EventHandler<string> DataAdded { get; set; }

        /// <summary>
        /// Event handler for when data is removed from the stack.
        /// </summary>
        public EventHandler<string> DataRemoved { get; set; }

        /// <summary>
        /// Event handler for when data is updated in the stack.
        /// </summary>
        public EventHandler<string> DataUpdated { get; set; }

        /// <summary>
        /// Event handler for when an exception is raised.
        /// </summary>
        public EventHandler<Exception> ExceptionEncountered { get; set; }

        /// <summary>
        /// Event handler for when the stack is cleared.
        /// </summary>
        public EventHandler Cleared { get; set; }

        /// <summary>
        /// Name of the index file. This file will live in the same directory as data objects.
        /// </summary>
        public string IndexFile
        {
            get
            {
                return _IndexFile;
            }
            set
            {
                if (String.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(IndexFile));
                FileInfo fi = new FileInfo(value);
                _IndexFile = fi.Name;
            }
        }

        /// <summary>
        /// The serializer used for the stack's objects.
        /// </summary>
        public Serializer Serializer { get; set; } = new Serializer();

        #endregion

        #region Private-Members

        private readonly bool _ClearOnDispose = false;
        private SemaphoreSlim _Semaphore = new SemaphoreSlim(1, 1);
        private string _Directory = null;
        private DirectoryInfo _DirectoryInfo = null;

        private string _IndexFile = ".index";
        private readonly object _IndexFileLock = new object();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate.
        /// </summary>
        /// <param name="directory">Directory.</param>
        /// <param name="clearOnDispose">Clear the stack's contents on dispose.  This will delete saved data.</param>
        public PersistentStack(string directory, bool clearOnDispose = false)
        {
            if (String.IsNullOrEmpty(directory)) throw new ArgumentNullException(nameof(directory));

            _Directory = directory;

            InitializeDirectory();

            _ClearOnDispose = clearOnDispose;

            // Create index file if it doesn't exist
            if (!File.Exists(GetKey(_IndexFile)))
                File.WriteAllBytes(GetKey(_IndexFile), Array.Empty<byte>());
        }

        /// <summary>
        /// Instantiate with a custom serializer.
        /// </summary>
        /// <param name="directory">Directory.</param>
        /// <param name="serializer">Custom serializer.</param>
        /// <param name="clearOnDispose">Clear the stack's contents on dispose.  This will delete saved data.</param>
        public PersistentStack(string directory, Serializer serializer, bool clearOnDispose = false)
            : this(directory, clearOnDispose)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            Serializer = serializer;
        }

        #endregion

        #region Stack<T> Implementation

        /// <summary>
        /// Returns a value that indicates whether there is an object at the top of the Stack, and if one
        /// is present, copies it to the result parameter. The object is not removed from the Stack.
        /// </summary>
        /// <param name="result">If present, the object at the top of the Stack; otherwise, the default value of T.</param>
        /// <returns>true if there is an object at the top of the Stack; false if the Stack is empty.</returns>
        public bool TryPeek(out T result)
        {
            try
            {
                result = Peek();
                return true;
            }
            catch
            {
                result = default;
                return false;
            }
        }

        /// <summary>
        /// Returns a value that indicates whether there is an object at the top of the Stack, and if one
        /// is present, copies it to the result parameter and removes it from the Stack.
        /// </summary>
        /// <param name="result">If present, the object at the top of the Stack; otherwise, the default value of T.</param>
        /// <returns>true if there is an object at the top of the Stack; false if the Stack is empty.</returns>
        public bool TryPop(out T result)
        {
            try
            {
                result = Pop();
                return true;
            }
            catch
            {
                result = default;
                return false;
            }
        }

        /// <summary>
        /// Attempts to retrieve an item at a specific index without removing it.
        /// </summary>
        /// <param name="index">The zero-based index of the item to retrieve.</param>
        /// <param name="result">If present, the object at the specified index; otherwise, the default value of T.</param>
        /// <returns>true if there is an object at the specified index; false if the index is out of range.</returns>
        public bool TryPeekAt(int index, out T result)
        {
            try
            {
                result = PeekAt(index);
                return true;
            }
            catch
            {
                result = default;
                return false;
            }
        }

        /// <summary>
        /// Returns the object at the top of the Stack without removing it.
        /// </summary>
        /// <returns>The object at the top of the Stack.</returns>
        /// <exception cref="InvalidOperationException">The Stack is empty.</exception>
        public T Peek()
        {
            byte[] data = PeekBytes();
            if (data == null) throw new InvalidOperationException("The Stack is empty.");

            try
            {
                // Try to deserialize as JSON first
                return Serializer.DeserializeJson<T>(data);
            }
            catch (System.Text.Json.JsonException)
            {
                // If T is string, try treating it as a raw string
                if (typeof(T) == typeof(string))
                {
                    return (T)(object)Encoding.UTF8.GetString(data);
                }
                throw; // Re-throw for non-string types
            }
        }

        /// <summary>
        /// Retrieves an item at a specific index without removing it.
        /// </summary>
        /// <param name="index">The zero-based index of the item to retrieve.</param>
        /// <returns>The item at the specified index.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Index is out of range.</exception>
        public T PeekAt(int index)
        {
            byte[] data = GetBytes(index);
            try
            {
                return Serializer.DeserializeJson<T>(data);
            }
            catch (System.Text.Json.JsonException)
            {
                // If T is string, try treating it as a raw string
                if (typeof(T) == typeof(string))
                {
                    return (T)(object)Encoding.UTF8.GetString(data);
                }
                throw; // Re-throw for non-string types
            }
        }

        /// <summary>
        /// Returns the object at the top of the Stack and removes it.
        /// </summary>
        /// <returns>The object at the top of the Stack.</returns>
        /// <exception cref="InvalidOperationException">The Stack is empty.</exception>
        public T Pop()
        {
            byte[] data = null;
            string key = null;

            _Semaphore.Wait();

            try
            {
                // Get the item at index 0 (top of stack)
                Dictionary<string, int> indexMap;
                lock (_IndexFileLock)
                {
                    indexMap = GetIndexMap();
                    if (indexMap.Count == 0)
                        throw new InvalidOperationException("The Stack is empty.");

                    // Find the key with index 0 (newest item)
                    foreach (var kvp in indexMap)
                    {
                        if (kvp.Value == 0)
                        {
                            key = kvp.Key;
                            break;
                        }
                    }

                    if (key == null)
                        throw new InvalidOperationException("Unable to find top item in stack.");
                }

                // Read the data
                string actualKey = GetKey(key);
                int size = GetFileSize(key);
                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    data = new byte[size];
                    fs.Read(data, 0, size);
                }

                // Atomically update all indices
                lock (_IndexFileLock)
                {
                    // Get latest index map to handle any concurrent changes
                    indexMap = GetIndexMap();

                    // Remove the popped item and prepare updated indices
                    List<KeyValuePair<string, int>> updatedIndices = new List<KeyValuePair<string, int>>();
                    foreach (var kvp in indexMap)
                    {
                        if (kvp.Key != key)
                        {
                            // Decrement indices for items after the popped one
                            if (kvp.Value > 0)
                            {
                                updatedIndices.Add(new KeyValuePair<string, int>(kvp.Key, kvp.Value - 1));
                            }
                            else
                            {
                                updatedIndices.Add(kvp);
                            }
                        }
                    }

                    // Write all updated indices at once
                    SaveIndexMap(updatedIndices);
                }

                // Delete the file
                if (File.Exists(actualKey))
                    File.Delete(actualKey);
            }
            finally
            {
                _Semaphore.Release();
            }

            DataRemoved?.Invoke(this, key);

            try
            {
                return Serializer.DeserializeJson<T>(data);
            }
            catch (System.Text.Json.JsonException)
            {
                // If T is string, try treating it as a raw string
                if (typeof(T) == typeof(string))
                {
                    return (T)(object)Encoding.UTF8.GetString(data);
                }
                throw; // Re-throw for non-string types
            }
        }

        /// <summary>
        /// Retrieves and optionally removes an item at a specific index.
        /// </summary>
        /// <param name="index">The zero-based index of the item to retrieve.</param>
        /// <param name="remove">Whether to remove the item from the stack.</param>
        /// <returns>The item at the specified index.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Index is out of range.</exception>
        public T PopAt(int index, bool remove = true)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            byte[] data = null;
            string key = null;

            _Semaphore.Wait();
            try
            {
                key = GetKeyByIndex(index);
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

                // Get the data first
                string actualKey = GetKey(key);
                int size = GetFileSize(key);
                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    data = new byte[size];
                    fs.Read(data, 0, size);
                }

                // Only remove and update indices if needed
                if (remove)
                {
                    // Remove the key from the index
                    RemoveFromIndex(key);

                    // Update all other indices
                    Dictionary<string, int> indexMap = GetIndexMap();
                    foreach (var kvp in indexMap)
                    {
                        if (kvp.Value > index)
                        {
                            // Decrement indices for items after the removed one
                            AddToIndex(kvp.Key, kvp.Value - 1);
                        }
                    }

                    // Delete the file
                    File.Delete(actualKey);
                    DataRemoved?.Invoke(this, key);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            try
            {
                return Serializer.DeserializeJson<T>(data);
            }
            catch (System.Text.Json.JsonException)
            {
                // If T is string, try treating it as a raw string
                if (typeof(T) == typeof(string))
                {
                    return (T)(object)Encoding.UTF8.GetString(data);
                }
                throw; // Re-throw for non-string types
            }
        }

        /// <summary>
        /// Inserts an object at the top of the Stack.
        /// </summary>
        /// <param name="item">The object to push onto the Stack.</param>
        /// <returns>Key of the pushed item.</returns>
        public string Push(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            string json = Serializer.SerializeJson(item, false);
            return Push(json);
        }

        /// <summary>
        /// Determines whether an element is in the Stack.
        /// </summary>
        /// <param name="item">The object to locate in the Stack.</param>
        /// <returns>true if item is found in the Stack; otherwise, false.</returns>
        public bool Contains(T item)
        {
            foreach (T stackItem in this)
            {
                if (EqualityComparer<T>.Default.Equals(stackItem, item))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Determines whether a specific key exists in the Stack.
        /// </summary>
        /// <param name="key">The key to locate in the Stack.</param>
        /// <returns>true if the key is found in the Stack; otherwise, false.</returns>
        public bool ContainsKey(string key)
        {
            if (String.IsNullOrEmpty(key)) return false;
            return KeyExists(key);
        }

        /// <summary>
        /// Determines whether an index is valid in the Stack.
        /// </summary>
        /// <param name="index">The zero-based index to check.</param>
        /// <returns>true if the index is valid in the Stack; otherwise, false.</returns>
        public bool ContainsIndex(int index)
        {
            if (index < 0) return false;
            return GetKeyByIndex(index) != null;
        }

        /// <summary>
        /// Copies the Stack to an existing one-dimensional Array, starting at the specified array index.
        /// </summary>
        /// <param name="array">The one-dimensional Array that is the destination of the elements copied from Stack.</param>
        /// <param name="arrayIndex">The zero-based index in array at which copying begins.</param>
        /// <exception cref="ArgumentNullException">array is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">arrayIndex is less than zero.</exception>
        /// <exception cref="ArgumentException">The number of elements in the source Stack is greater than the available space from arrayIndex to the end of the destination array.</exception>
        public void CopyTo(T[] array, int arrayIndex)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex), "Index is less than zero.");
            if (Count > array.Length - arrayIndex) throw new ArgumentException("The number of elements in the Stack is greater than the available space in the array.");

            int i = arrayIndex;
            foreach (T item in this)
            {
                array[i++] = item;
            }
        }

        /// <summary>
        /// Copies the stack to a new array.
        /// </summary>
        /// <returns>A new array containing copies of the elements of the Stack.</returns>
        public T[] ToArray()
        {
            T[] array = new T[Count];
            CopyTo(array, 0);
            return array;
        }

        #endregion

        #region IEnumerable<T> Implementation

        /// <summary>
        /// Returns an enumerator that iterates through the stack.
        /// </summary>
        /// <returns>An enumerator for the stack.</returns>
        public IEnumerator<T> GetEnumerator()
        {
            _Semaphore.Wait();
            List<string> keys;

            try
            {
                // Get keys in order by index (lower index = newer item)
                var indexMap = GetIndexMap();
                keys = indexMap.OrderBy(kvp => kvp.Value).Select(kvp => kvp.Key).ToList();

                // If no index file or empty, fallback to timestamp ordering
                if (keys.Count == 0)
                {
                    keys = GetKeysInStackOrder().ToList();
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            foreach (string key in keys)
            {
                T item = default;
                bool success = false;

                byte[] data = Pop(key, false);
                if (data != null)
                {
                    try
                    {
                        item = Serializer.DeserializeJson<T>(data);
                        success = true;
                    }
                    catch (System.Text.Json.JsonException)
                    {
                        // If T is string, try treating the raw data as a string
                        if (typeof(T) == typeof(string))
                        {
                            item = (T)(object)Encoding.UTF8.GetString(data);
                            success = true;
                        }
                    }
                }

                if (success)
                {
                    yield return item;
                }
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the stack.
        /// </summary>
        /// <returns>An enumerator for the stack.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Dispose.
        /// </summary>
        public void Dispose()
        {
            if (_ClearOnDispose)
            {
                Clear();
                Directory.Delete(_Directory, true);
            }

            _Directory = null;
            _DirectoryInfo = null;
            _Semaphore = null;
            _IndexFile = null;
        }

        /// <summary>
        /// Add data to the stack.
        /// </summary>
        /// <param name="data">Data.</param>
        /// <returns>Key.</returns>
        public string Push(string data)
        {
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return Push(Encoding.UTF8.GetBytes(data));
        }

        /// <summary>
        /// Add data to the stack.
        /// </summary>
        /// <param name="data">Data.</param>
        /// <returns>Key.</returns>
        public string Push(byte[] data)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));

            string key = Guid.NewGuid().ToString();

            _Semaphore.Wait();

            try
            {
                // First write the file
                using (FileStream fs = new FileStream(GetKey(key), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    fs.Write(data, 0, data.Length);
                }

                // Then atomically update the indices
                lock (_IndexFileLock)
                {
                    // Get current index map
                    Dictionary<string, int> indexMap = GetIndexMap();

                    // Move all existing items down one position (increment index)
                    List<KeyValuePair<string, int>> updatedIndices = new List<KeyValuePair<string, int>>();
                    foreach (var kvp in indexMap)
                    {
                        updatedIndices.Add(new KeyValuePair<string, int>(kvp.Key, kvp.Value + 1));
                    }

                    // Add the new item at index 0 (top of stack)
                    updatedIndices.Add(new KeyValuePair<string, int>(key, 0));

                    // Write all updated indices at once
                    SaveIndexMap(updatedIndices);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            DataAdded?.Invoke(this, key);

            return key;
        }


        /// <summary>
        /// Add data to the stack asynchronously.
        /// </summary>
        /// <param name="data">Data.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Key.</returns>
        public async Task<string> PushAsync(string data, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return await PushAsync(Encoding.UTF8.GetBytes(data), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Add data to the stack asynchronously.
        /// </summary>
        /// <param name="data">Data.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Key.</returns>
        public async Task<string> PushAsync(byte[] data, CancellationToken token = default)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));

            // Check for cancellation before starting
            token.ThrowIfCancellationRequested();

            string key = Guid.NewGuid().ToString();

            await _Semaphore.WaitAsync(token);

            try
            {
                // Check for cancellation again after acquiring the semaphore
                token.ThrowIfCancellationRequested();

                // First write the file
                using (FileStream fs = new FileStream(GetKey(key), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    await fs.WriteAsync(data, 0, data.Length, token).ConfigureAwait(false);
                }

                // Then atomically update the indices
                lock (_IndexFileLock)
                {
                    // Get current index map
                    Dictionary<string, int> indexMap = GetIndexMap();

                    // Move all existing items down one position (increment index)
                    List<KeyValuePair<string, int>> updatedIndices = new List<KeyValuePair<string, int>>();
                    foreach (var kvp in indexMap)
                    {
                        updatedIndices.Add(new KeyValuePair<string, int>(kvp.Key, kvp.Value + 1));
                    }

                    // Add the new item at index 0 (top of stack)
                    updatedIndices.Add(new KeyValuePair<string, int>(key, 0));

                    // Write all updated indices at once
                    SaveIndexMap(updatedIndices);
                }
            }
            catch (OperationCanceledException)
            {
                // Cleanup if cancelled after starting the file write
                try
                {
                    string actualKey = GetKey(key);
                    if (File.Exists(actualKey))
                    {
                        File.Delete(actualKey);
                    }
                    RemoveFromIndex(key);
                }
                catch
                {
                    // Ignore cleanup errors
                }
                throw; // Re-throw the cancellation exception
            }
            finally
            {
                _Semaphore.Release();
            }

            DataAdded?.Invoke(this, key);

            return key;
        }

        /// <summary>
        /// Push an item to the stack asynchronously.
        /// </summary>
        /// <param name="item">The item to push onto the stack.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Key of the pushed item.</returns>
        public async Task<string> PushAsync(T item, CancellationToken token = default)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            string json = Serializer.SerializeJson(item, false);
            return await PushAsync(json, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieves the byte data from the top of the stack without removing it.
        /// </summary>
        /// <param name="key">Key, if a specific key is needed. If null, gets the most recent item.</param>
        /// <returns>Data in bytes.</returns>
        public byte[] PeekBytes(string key = null)
        {
            return Pop(key, false);
        }

        /// <summary>
        /// Retrieves the byte data at a specific index without removing it.
        /// </summary>
        /// <param name="index">The zero-based index of the item to retrieve.</param>
        /// <returns>Data in bytes.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Index is out of range.</exception>
        public byte[] GetBytes(int index)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            _Semaphore.Wait();
            try
            {
                string key = GetKeyByIndex(index);
                if (string.IsNullOrEmpty(key))
                    throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

                // Get the data within the same lock scope to prevent race conditions
                string actualKey = GetKey(key);
                int size = GetFileSize(key);

                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    byte[] data = new byte[size];
                    fs.Read(data, 0, size);
                    return data;
                }
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Retrieve data from the stack.
        /// </summary>
        /// <param name="key">Key, if a specific key is needed.</param>
        /// <param name="purge">Boolean flag indicating whether or not the entry should be removed from the stack once read.</param>
        /// <returns>Data.</returns>
        public byte[] Pop(string key = null, bool purge = true)
        {
            byte[] ret = null;
            string actualKey = null;
            int removedIndex = -1;

            _Semaphore.Wait();

            try
            {
                if (String.IsNullOrEmpty(key))
                {
                    // Get the item at index 0 (top of stack)
                    Dictionary<string, int> indexMap = GetIndexMap();
                    if (indexMap.Count == 0)
                        return null;

                    // Find the key with index 0 (newest item)
                    foreach (var kvp in indexMap)
                    {
                        if (kvp.Value == 0)
                        {
                            key = kvp.Key;
                            removedIndex = 0;
                            break;
                        }
                    }

                    if (key == null)
                        return null;
                }
                else
                {
                    // Get the index of the key if we're purging
                    if (purge)
                    {
                        Dictionary<string, int> indexMap = GetIndexMap();
                        foreach (var kvp in indexMap)
                        {
                            if (kvp.Key == key)
                            {
                                removedIndex = kvp.Value;
                                break;
                            }
                        }
                    }
                }

                if (!KeyExists(key))
                    throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

                actualKey = GetKey(key);
                int size = GetFileSize(key);

                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    ret = new byte[size];
                    fs.Read(ret, 0, size);
                }

                if (purge)
                {
                    // Remove from index first
                    RemoveFromIndex(key);

                    // Now update all other indices if needed
                    if (removedIndex >= 0)
                    {
                        Dictionary<string, int> indexMap = GetIndexMap();
                        foreach (var kvp in indexMap)
                        {
                            if (kvp.Value > removedIndex)
                            {
                                // Decrement indices for items after the removed one
                                AddToIndex(kvp.Key, kvp.Value - 1);
                            }
                        }
                    }

                    // Now delete the file
                    if (File.Exists(actualKey))
                        File.Delete(actualKey);

                    DataRemoved?.Invoke(this, key);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            return ret;
        }

        /// <summary>
        /// Retrieve data from the stack asynchronously.
        /// </summary>
        /// <param name="key">Key, if a specific key is needed.</param>
        /// <param name="purge">Boolean flag indicating whether or not the entry should be removed from the stack once read.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Data.</returns>
        public async Task<byte[]> PopAsync(string key = null, bool purge = true, CancellationToken token = default)
        {
            byte[] ret = null;
            string actualKey = null;

            await _Semaphore.WaitAsync(token);

            try
            {
                if (String.IsNullOrEmpty(key))
                {
                    // Get the item at index 0 (top of stack)
                    Dictionary<string, int> indexMap = GetIndexMap();
                    if (indexMap.Count == 0)
                        return null;

                    // Find the key with index 0 (newest item)
                    foreach (var kvp in indexMap)
                    {
                        if (kvp.Value == 0)
                        {
                            key = kvp.Key;
                            break;
                        }
                    }

                    if (key == null)
                        return null;

                    actualKey = GetKey(key);
                    int size = GetFileSize(key);

                    using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                    {
                        ret = new byte[size];
                        await fs.ReadAsync(ret, 0, size, token).ConfigureAwait(false);
                    }
                }
                else
                {
                    // Get specific key
                    if (!KeyExists(key))
                        throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

                    actualKey = GetKey(key);
                    int size = GetFileSize(key);

                    using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                    {
                        ret = new byte[size];
                        await fs.ReadAsync(ret, 0, size, token).ConfigureAwait(false);
                    }
                }

                if (purge)
                {
                    // Remove from index
                    Dictionary<string, int> indexMap = GetIndexMap();
                    int removedIndex = -1;

                    // Find the index of the removed item
                    foreach (var kvp in indexMap)
                    {
                        if (kvp.Key == key)
                        {
                            removedIndex = kvp.Value;
                            break;
                        }
                    }

                    RemoveFromIndex(key);

                    // Decrement all indices greater than the removed index
                    if (removedIndex >= 0)
                    {
                        foreach (var kvp in indexMap)
                        {
                            if (kvp.Key != key && kvp.Value > removedIndex)
                            {
                                AddToIndex(kvp.Key, kvp.Value - 1);
                            }
                        }
                    }

                    // Delete the file
                    if (File.Exists(actualKey))
                        File.Delete(actualKey);

                    DataRemoved?.Invoke(this, key);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            return ret;
        }

        /// <summary>
        /// Asynchronously retrieves an item from the stack.
        /// </summary>
        /// <param name="purge">Boolean flag indicating whether or not the entry should be removed from the stack once read.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The item at the top of the stack.</returns>
        public async Task<T> PopItemAsync(bool purge = true, CancellationToken token = default)
        {
            byte[] data = await PopAsync(null, purge, token).ConfigureAwait(false);
            if (data == null) throw new InvalidOperationException("The Stack is empty.");

            try
            {
                return Serializer.DeserializeJson<T>(data);
            }
            catch (System.Text.Json.JsonException)
            {
                // If T is string, try treating it as a raw string
                if (typeof(T) == typeof(string))
                {
                    return (T)(object)Encoding.UTF8.GetString(data);
                }
                throw; // Re-throw for non-string types
            }
        }

        /// <summary>
        /// Asynchronously retrieves an item at a specific index.
        /// </summary>
        /// <param name="index">The zero-based index of the item to retrieve.</param>
        /// <param name="remove">Whether to remove the item from the stack.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The item at the specified index.</returns>
        public async Task<T> PopAtAsync(int index, bool remove = true, CancellationToken token = default)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            string key = GetKeyByIndex(index);
            if (string.IsNullOrEmpty(key)) throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            byte[] data = await PopAsync(key, remove, token).ConfigureAwait(false);
            try
            {
                return Serializer.DeserializeJson<T>(data);
            }
            catch (System.Text.Json.JsonException)
            {
                // If T is string, try treating it as a raw string
                if (typeof(T) == typeof(string))
                {
                    return (T)(object)Encoding.UTF8.GetString(data);
                }
                throw; // Re-throw for non-string types
            }
        }

        /// <summary>
        /// Updates an item at a specific key.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="data">The new data to store.</param>
        public void UpdateByKey(string key, byte[] data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (data == null) throw new ArgumentNullException(nameof(data));
            if (!KeyExists(key)) throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

            _Semaphore.Wait();

            try
            {
                string actualKey = GetKey(key);
                using (FileStream fs = new FileStream(actualKey, FileMode.Truncate, FileAccess.Write))
                {
                    fs.Write(data, 0, data.Length);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            DataUpdated?.Invoke(this, key);
        }

        /// <summary>
        /// Updates an item at a specific key asynchronously.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="data">The new data to store.</param>
        /// <param name="token">Cancellation token.</param>
        public async Task UpdateByKeyAsync(string key, byte[] data, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (data == null) throw new ArgumentNullException(nameof(data));
            if (!KeyExists(key)) throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

            await _Semaphore.WaitAsync(token);

            try
            {
                string actualKey = GetKey(key);
                using (FileStream fs = new FileStream(actualKey, FileMode.Truncate, FileAccess.Write))
                {
                    await fs.WriteAsync(data, 0, data.Length, token).ConfigureAwait(false);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            DataUpdated?.Invoke(this, key);
        }

        /// <summary>
        /// Updates an item at a specific key.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="item">The new item to store.</param>
        public void UpdateByKey(string key, T item)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (item == null) throw new ArgumentNullException(nameof(item));

            string json = Serializer.SerializeJson(item, false);
            byte[] data = Encoding.UTF8.GetBytes(json);
            UpdateByKey(key, data);
        }

        /// <summary>
        /// Updates an item at a specific key asynchronously.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="item">The new item to store.</param>
        /// <param name="token">Cancellation token.</param>
        public async Task UpdateByKeyAsync(string key, T item, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (item == null) throw new ArgumentNullException(nameof(item));

            string json = Serializer.SerializeJson(item, false);
            byte[] data = Encoding.UTF8.GetBytes(json);
            await UpdateByKeyAsync(key, data, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates an item at a specific index.
        /// </summary>
        /// <param name="index">The zero-based index of the item to update.</param>
        /// <param name="item">The new item to store.</param>
        public void UpdateAt(int index, T item)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));
            if (item == null) throw new ArgumentNullException(nameof(item));

            string key = GetKeyByIndex(index);
            if (string.IsNullOrEmpty(key)) throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            UpdateByKey(key, item);
        }

        /// <summary>
        /// Updates an item at a specific index asynchronously.
        /// </summary>
        /// <param name="index">The zero-based index of the item to update.</param>
        /// <param name="item">The new item to store.</param>
        /// <param name="token">Cancellation token.</param>
        public async Task UpdateAtAsync(int index, T item, CancellationToken token = default)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));
            if (item == null) throw new ArgumentNullException(nameof(item));

            string key = GetKeyByIndex(index);
            if (string.IsNullOrEmpty(key)) throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            await UpdateByKeyAsync(key, item, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Remove a specific entry from the stack.
        /// </summary>
        /// <param name="key">Key.</param>
        public void Purge(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            string actualKey = GetKey(key);

            _Semaphore.Wait();

            try
            {
                if (File.Exists(actualKey)) File.Delete(actualKey);
                RemoveFromIndex(key);
            }
            finally
            {
                _Semaphore.Release();
            }

            DataRemoved?.Invoke(this, key);
        }

        /// <summary>
        /// Remove an item at a specific index.
        /// </summary>
        /// <param name="index">The zero-based index of the item to remove.</param>
        public void RemoveAt(int index)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            string key = GetKeyByIndex(index);
            if (string.IsNullOrEmpty(key)) throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            // Use the modified Pop method which handles index updates properly
            Pop(key, true);
        }

        /// <summary>
        /// Destructively empty the stack.  This will delete all of the data files in the directory.
        /// </summary>
        public void Clear()
        {
            _Semaphore.Wait();

            try
            {
                foreach (FileInfo file in _DirectoryInfo.GetFiles())
                {
                    if (!file.Name.Equals(_IndexFile)) // Don't delete the index file, just clear it
                    {
                        file.Delete();
                    }
                }

                // Clear index file
                File.WriteAllText(GetKey(_IndexFile), string.Empty);

                foreach (DirectoryInfo dir in _DirectoryInfo.GetDirectories())
                {
                    dir.Delete(true);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            Cleared?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Gets all keys in the stack, ordered by index (most recent first).
        /// </summary>
        /// <returns>List of keys.</returns>
        public List<string> GetKeys()
        {
            Dictionary<string, int> indexMap = GetIndexMap();
            return indexMap.OrderByDescending(kvp => kvp.Value).Select(kvp => kvp.Key).ToList();
        }

        /// <summary>
        /// Gets all keys in the stack asynchronously, ordered by index (most recent first).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>List of keys.</returns>
        public async Task<List<string>> GetKeysAsync(CancellationToken token = default)
        {
            await _Semaphore.WaitAsync(token);
            try
            {
                Dictionary<string, int> indexMap = GetIndexMap();
                return indexMap.OrderByDescending(kvp => kvp.Value).Select(kvp => kvp.Key).ToList();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        #endregion

        #region Private-Methods

        private void InitializeDirectory()
        {
            _Directory = _Directory.Replace("\\", "/");
            if (!_Directory.EndsWith("/")) _Directory += "/";
            if (!Directory.Exists(_Directory)) Directory.CreateDirectory(_Directory);

            _DirectoryInfo = new DirectoryInfo(_Directory);
        }

        private string GetKey(string str)
        {
            return _Directory + str;
        }

        private string GetLatestKey()
        {
            IOrderedEnumerable<FileInfo> files = _DirectoryInfo.GetFiles()
                .Where(f => !f.Name.Equals(_IndexFile))
                .OrderByDescending(f => f.LastWriteTime);

            if (files != null && files.Count() > 0) return files.ToArray()[0].Name;
            else return null;
        }

        private string[] GetKeysInStackOrder()
        {
            return _DirectoryInfo.GetFiles()
                .Where(f => !f.Name.Equals(_IndexFile))
                .OrderByDescending(f => f.LastWriteTime)
                .Select(f => f.Name)
                .ToArray();
        }

        private int GetFileSize(string str)
        {
            string key = GetKey(str);
            return (int)(new FileInfo(key).Length);
        }

        private bool KeyExists(string str)
        {
            return File.Exists(GetKey(str));
        }

        private Dictionary<string, int> GetIndexMap()
        {
            Dictionary<string, int> indexMap = new Dictionary<string, int>();
            string filename = GetKey(_IndexFile);

            // Note: This method should only be called within a lock(_IndexFileLock) block
            if (!File.Exists(filename))
                File.WriteAllBytes(filename, Array.Empty<byte>());

            string[] lines = File.ReadAllLines(filename);

            if (lines != null && lines.Length > 0)
            {
                for (int i = 0; i < lines.Length; i++)
                {
                    KeyValuePair<string, int>? line = ParseIndexLine(i, lines[i]);
                    if (line == null) continue;
                    indexMap[line.Value.Key] = line.Value.Value;
                }
            }

            return indexMap;
        }

        private void AddToIndex(string key, int index)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            string filename = GetKey(_IndexFile);

            List<string> updatedLines = new List<string>();

            lock (_IndexFileLock)
            {
                if (!File.Exists(filename))
                    File.WriteAllBytes(filename, Array.Empty<byte>());

                string[] lines = File.ReadAllLines(filename);

                if (lines != null && lines.Length > 0)
                {
                    for (int i = 0; i < lines.Length; i++)
                    {
                        KeyValuePair<string, int>? line = ParseIndexLine(i, lines[i]);
                        if (line == null) continue;
                        if (!line.Value.Key.Equals(key)) updatedLines.Add(line.Value.Key + " " + line.Value.Value.ToString());
                    }
                }

                updatedLines.Add(key + " " + index.ToString());
                File.WriteAllLines(filename, updatedLines);
            }
        }

        private void RemoveFromIndex(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            string filename = GetKey(_IndexFile);

            List<string> updatedLines = new List<string>();

            lock (_IndexFileLock)
            {
                if (!File.Exists(filename))
                    File.WriteAllBytes(filename, Array.Empty<byte>());

                string[] lines = File.ReadAllLines(filename);

                if (lines != null && lines.Length > 0)
                {
                    for (int i = 0; i < lines.Length; i++)
                    {
                        KeyValuePair<string, int>? line = ParseIndexLine(i, lines[i]);
                        if (line == null) continue;
                        if (!line.Value.Key.Equals(key)) updatedLines.Add(line.Value.Key + " " + line.Value.Value.ToString());
                    }
                }

                File.WriteAllLines(filename, updatedLines);
            }
        }

        private string GetKeyByIndex(int index)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            Dictionary<string, int> indexMap = GetIndexMap();
            string key = null;

            // More robust search through the index map
            foreach (var kvp in indexMap)
            {
                if (kvp.Value == index)
                {
                    key = kvp.Key;
                    break;
                }
            }

            // Verify the key exists as a file
            if (!string.IsNullOrEmpty(key) && KeyExists(key))
            {
                return key;
            }

            return null;
        }

        private KeyValuePair<string, int>? ParseIndexLine(int lineNumber, string line)
        {
            if (string.IsNullOrEmpty(line)) return null;

            string[] parts = line.Split(new char[] { ' ' }, 2);
            string filename = GetKey(_IndexFile);

            if (parts.Length != 2)
            {
                ExceptionEncountered?.Invoke(
                    this,
                    new ArgumentException("Invalid line format detected in line " + lineNumber + " of index file " + filename));

                return null;
            }

            int index = 0;
            try
            {
                index = int.Parse(parts[1]);
            }
            catch (Exception)
            {
                ExceptionEncountered?.Invoke(
                    this,
                    new ArgumentException("Invalid index format detected in line " + lineNumber + " of index file " + filename));

                return null;
            }

            return new KeyValuePair<string, int>(parts[0], index);
        }

        private void SaveIndexMap(IEnumerable<KeyValuePair<string, int>> indexMap)
        {
            string filename = GetKey(_IndexFile);
            List<string> lines = new List<string>();

            foreach (var kvp in indexMap)
            {
                lines.Add(kvp.Key + " " + kvp.Value.ToString());
            }

            // This should only be called when already holding the _IndexFileLock
            File.WriteAllLines(filename, lines);
        }

        #endregion
    }
}