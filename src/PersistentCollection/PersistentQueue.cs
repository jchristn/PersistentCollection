namespace PersistentCollection
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Persistent generic queue. Queued entries are backed on disk.
    /// Data is dequeued from the queue in a first-in-first-out manner.
    /// Implements the standard Queue&lt;T&gt; interface methods with additional indexed access.
    /// </summary>
    /// <typeparam name="T">Type of elements in the queue</typeparam>
    public class PersistentQueue<T> : IDisposable, IEnumerable<T>
    {
        #region Public-Members

        /// <summary>
        /// Number of entries waiting in the queue.
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
        /// Number of bytes waiting in the queue.
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
        /// Gets an item at the specified index in the queue without removing it.
        /// </summary>
        /// <param name="index">The zero-based index of the element to get.</param>
        /// <returns>The element at the specified index.</returns>
        public T this[int index]
        {
            get
            {
                byte[] data = GetBytes(index);
                return _Deserializer(data);
            }
        }

        /// <summary>
        /// Event handler for when data is added to the queue.
        /// </summary>
        public EventHandler<string> DataEnqueued { get; set; }

        /// <summary>
        /// Event handler for when data is removed from the queue.
        /// </summary>
        public EventHandler<string> DataDequeued { get; set; }

        /// <summary>
        /// Event handler for when an exception is raised.
        /// </summary>
        public EventHandler<Exception> ExceptionEncountered { get; set; }

        /// <summary>
        /// Event handler for when the queue is cleared.
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

        #endregion

        #region Private-Members

        private readonly bool _ClearOnDispose = false;
        private SemaphoreSlim _Semaphore = new SemaphoreSlim(1, 1);
        private string _Directory = null;
        private DirectoryInfo _DirectoryInfo = null;

        private string _IndexFile = ".index";
        private readonly object _IndexFileLock = new object();

        private readonly Func<T, byte[]> _Serializer;
        private readonly Func<byte[], T> _Deserializer;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate a PersistentQueue with default JSON serialization.
        /// For byte[] type, no serialization is performed.
        /// </summary>
        /// <param name="directory">Directory where queue data will be stored.</param>
        /// <param name="clearOnDispose">Clear the queue's contents on dispose. This will delete saved data.</param>
        public PersistentQueue(string directory, bool clearOnDispose = false)
        {
            if (String.IsNullOrEmpty(directory)) throw new ArgumentNullException(nameof(directory));

            _Directory = directory;
            InitializeDirectory();
            _ClearOnDispose = clearOnDispose;

            // Set up serialization based on type T
            if (typeof(T) == typeof(byte[]))
            {
                // For byte arrays, no serialization needed
                _Serializer = obj => (byte[])(object)obj;
                _Deserializer = bytes => (T)(object)bytes;
            }
            else if (typeof(T) == typeof(string))
            {
                // For strings, use UTF8 encoding
                _Serializer = obj => Encoding.UTF8.GetBytes((string)(object)obj);
                _Deserializer = bytes => (T)(object)Encoding.UTF8.GetString(bytes);
            }
            else
            {
                // For other types, use System.Text.Json
                _Serializer = obj => System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(obj);
                _Deserializer = bytes => System.Text.Json.JsonSerializer.Deserialize<T>(bytes);
            }

            // Create index file if it doesn't exist
            if (!File.Exists(GetKey(_IndexFile)))
                File.WriteAllBytes(GetKey(_IndexFile), Array.Empty<byte>());
        }

        /// <summary>
        /// Instantiate with custom serialization functions.
        /// </summary>
        /// <param name="directory">Directory where queue data will be stored.</param>
        /// <param name="serializer">Function to serialize T to byte array.</param>
        /// <param name="deserializer">Function to deserialize byte array to T.</param>
        /// <param name="clearOnDispose">Clear the queue's contents on dispose. This will delete saved data.</param>
        public PersistentQueue(
            string directory,
            Func<T, byte[]> serializer,
            Func<byte[], T> deserializer,
            bool clearOnDispose = false)
        {
            if (String.IsNullOrEmpty(directory)) throw new ArgumentNullException(nameof(directory));
            _Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _Deserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));

            _Directory = directory;
            InitializeDirectory();
            _ClearOnDispose = clearOnDispose;

            // Create index file if it doesn't exist
            if (!File.Exists(GetKey(_IndexFile)))
                File.WriteAllBytes(GetKey(_IndexFile), Array.Empty<byte>());
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Dispose of resources.
        /// </summary>
        public void Dispose()
        {
            if (_ClearOnDispose)
            {
                Clear();
                Directory.Delete(_Directory);
            }

            _Directory = null;
            _DirectoryInfo = null;
            _Semaphore = null;
            _IndexFile = null;
        }

        /// <summary>
        /// Add an item to the queue.
        /// </summary>
        /// <param name="item">The item to add to the queue.</param>
        /// <returns>Key that can be used to retrieve the item.</returns>
        public string Enqueue(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            byte[] data = _Serializer(item);
            string key = Guid.NewGuid().ToString();

            _Semaphore.Wait();

            try
            {
                using (FileStream fs = new FileStream(GetKey(key), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    fs.Write(data, 0, data.Length);
                }

                // Add to index with the next available index
                int index = GetLastIndex() + 1;
                AddToIndex(key, index);
            }
            finally
            {
                _Semaphore.Release();
            }

            DataEnqueued?.Invoke(this, key);

            return key;
        }

        /// <summary>
        /// Add an item to the queue asynchronously.
        /// </summary>
        /// <param name="item">The item to add to the queue.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Key that can be used to retrieve the item.</returns>
        public async Task<string> EnqueueAsync(T item, CancellationToken token = default)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            byte[] data = _Serializer(item);
            string key = Guid.NewGuid().ToString();

            await _Semaphore.WaitAsync(token);

            try
            {
                using (FileStream fs = new FileStream(GetKey(key), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    await fs.WriteAsync(data, 0, data.Length, token).ConfigureAwait(false);
                }

                // Add to index with the next available index
                int index = GetLastIndex() + 1;
                AddToIndex(key, index);
            }
            finally
            {
                _Semaphore.Release();
            }

            DataEnqueued?.Invoke(this, key);

            return key;
        }

        /// <summary>
        /// Retrieve and remove the oldest item from the queue.
        /// </summary>
        /// <returns>The oldest item in the queue.</returns>
        public T Dequeue()
        {
            string key = null;
            byte[] data = null;

            _Semaphore.Wait();

            try
            {
                // Get oldest key (smallest index)
                var indexMap = GetIndexMap();
                if (indexMap.Count == 0)
                    return default;

                var orderedKeys = indexMap.OrderBy(kvp => kvp.Value).ToList();
                key = orderedKeys.First().Key;

                string actualKey = GetKey(key);
                int size = GetFileSize(key);

                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    data = new byte[size];
                    fs.Read(data, 0, size);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            Remove(key);
            DataDequeued?.Invoke(this, key);

            return _Deserializer(data);
        }

        /// <summary>
        /// Retrieve a specific item from the queue.
        /// </summary>
        /// <param name="key">The key of the item to retrieve.</param>
        /// <param name="remove">Whether to remove the item from the queue.</param>
        /// <returns>The requested item.</returns>
        public T Dequeue(string key, bool remove = true)
        {
            if (String.IsNullOrEmpty(key))
            {
                // If key is null, use the overload to get the oldest item
                return remove ? Dequeue() : Peek();
            }

            byte[] data = null;
            string actualKey = null;

            _Semaphore.Wait();

            try
            {
                if (!KeyExists(key))
                    throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

                actualKey = GetKey(key);
                int size = GetFileSize(key);

                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    data = new byte[size];
                    fs.Read(data, 0, size);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            if (remove)
            {
                Remove(key);
                RemoveFromIndex(key);
                DataDequeued?.Invoke(this, key);
            }

            return _Deserializer(data);
        }

        /// <summary>
        /// Retrieve an item at the specified index from the queue.
        /// </summary>
        /// <param name="index">Zero-based index of the item.</param>
        /// <param name="remove">Whether to remove the item from the queue.</param>
        /// <returns>The item at the specified index.</returns>
        public T DequeueAt(int index, bool remove = true)
        {
            string key = GetKeyByIndex(index);
            if (string.IsNullOrEmpty(key))
                throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            return Dequeue(key, remove);
        }

        /// <summary>
        /// Retrieve and remove the oldest item from the queue asynchronously.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with the oldest item.</returns>
        public async Task<T> DequeueAsync(CancellationToken token = default)
        {
            string key = null;
            byte[] data = null;

            await _Semaphore.WaitAsync(token);

            try
            {
                // Get oldest key (smallest index)
                var indexMap = GetIndexMap();
                if (indexMap.Count == 0)
                    return default;

                var orderedKeys = indexMap.OrderBy(kvp => kvp.Value).ToList();
                key = orderedKeys.First().Key;

                string actualKey = GetKey(key);
                int size = GetFileSize(key);

                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    data = new byte[size];
                    await fs.ReadAsync(data, 0, size, token).ConfigureAwait(false);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            Remove(key);
            RemoveFromIndex(key);
            DataDequeued?.Invoke(this, key);

            return _Deserializer(data);
        }

        /// <summary>
        /// Retrieve a specific item from the queue asynchronously.
        /// </summary>
        /// <param name="key">The key of the item to retrieve.</param>
        /// <param name="remove">Whether to remove the item from the queue.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with the requested item.</returns>
        public async Task<T> DequeueAsync(string key, bool remove = true, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(key))
            {
                // If key is null, use the overload to get the oldest item
                return remove ? await DequeueAsync(token).ConfigureAwait(false) : await PeekAsync(token).ConfigureAwait(false);
            }

            byte[] data = null;
            string actualKey = null;

            await _Semaphore.WaitAsync(token);

            try
            {
                if (!KeyExists(key))
                    throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

                actualKey = GetKey(key);
                int size = GetFileSize(key);

                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    data = new byte[size];
                    await fs.ReadAsync(data, 0, size, token).ConfigureAwait(false);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            if (remove)
            {
                Remove(key);
                RemoveFromIndex(key);
                DataDequeued?.Invoke(this, key);
            }

            return _Deserializer(data);
        }

        /// <summary>
        /// Retrieve an item at the specified index from the queue asynchronously.
        /// </summary>
        /// <param name="index">Zero-based index of the item.</param>
        /// <param name="remove">Whether to remove the item from the queue.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with the item at the specified index.</returns>
        public async Task<T> DequeueAtAsync(int index, bool remove = true, CancellationToken token = default)
        {
            string key = GetKeyByIndex(index);
            if (string.IsNullOrEmpty(key))
                throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            return await DequeueAsync(key, remove, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Examine the next item in the queue without removing it.
        /// </summary>
        /// <returns>The oldest item in the queue.</returns>
        public T Peek()
        {
            string key = null;
            byte[] data = null;

            _Semaphore.Wait();

            try
            {
                // Get oldest key (smallest index)
                var indexMap = GetIndexMap();
                if (indexMap.Count == 0)
                    return default;

                var orderedKeys = indexMap.OrderBy(kvp => kvp.Value).ToList();
                key = orderedKeys.First().Key;

                string actualKey = GetKey(key);
                int size = GetFileSize(key);

                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    data = new byte[size];
                    fs.Read(data, 0, size);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            return _Deserializer(data);
        }

        /// <summary>
        /// Examine an item at the specified index without removing it.
        /// </summary>
        /// <param name="index">Zero-based index of the item.</param>
        /// <returns>The item at the specified index.</returns>
        public T PeekAt(int index)
        {
            return Get(index);
        }

        /// <summary>
        /// Get data from the queue by index.
        /// </summary>
        /// <param name="index">Zero-based index.</param>
        /// <returns>Data at the specified index.</returns>
        public T Get(int index)
        {
            byte[] data = GetBytes(index);
            return _Deserializer(data);
        }

        /// <summary>
        /// Get data from the queue by index.
        /// </summary>
        /// <param name="index">Zero-based index.</param>
        /// <returns>Data as byte array at the specified index.</returns>
        public byte[] GetBytes(int index)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            string key = GetKeyByIndex(index);
            if (String.IsNullOrEmpty(key)) throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            return GetBytes(key);
        }

        /// <summary>
        /// Get data from the queue by key.
        /// </summary>
        /// <param name="key">Key of the item.</param>
        /// <returns>Data as byte array for the specified key.</returns>
        public byte[] GetBytes(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (!KeyExists(key)) throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

            string actualKey = GetKey(key);
            int size = GetFileSize(key);

            _Semaphore.Wait();

            try
            {
                return File.ReadAllBytes(actualKey);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Get data from the queue by key asynchronously.
        /// </summary>
        /// <param name="key">Key of the item.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with the data for the specified key.</returns>
        public async Task<byte[]> GetBytesAsync(string key, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (!KeyExists(key)) throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

            byte[] ret = null;
            string actualKey = GetKey(key);
            int size = GetFileSize(key);

            await _Semaphore.WaitAsync(token);

            try
            {
                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    ret = new byte[size];
                    await fs.ReadAsync(ret, 0, size, token).ConfigureAwait(false);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            return ret;
        }

        /// <summary>
        /// Get data from the queue by index asynchronously.
        /// </summary>
        /// <param name="index">Zero-based index.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with the data at the specified index.</returns>
        public async Task<byte[]> GetBytesAsync(int index, CancellationToken token = default)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            string key = GetKeyByIndex(index);
            if (String.IsNullOrEmpty(key)) throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            return await GetBytesAsync(key, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Examine the next item in the queue asynchronously without removing it.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with the oldest item.</returns>
        public async Task<T> PeekAsync(CancellationToken token = default)
        {
            string key = null;
            byte[] data = null;

            await _Semaphore.WaitAsync(token);

            try
            {
                // Get oldest key (smallest index)
                var indexMap = GetIndexMap();
                if (indexMap.Count == 0)
                    return default;

                var orderedKeys = indexMap.OrderBy(kvp => kvp.Value).ToList();
                key = orderedKeys.First().Key;

                string actualKey = GetKey(key);
                int size = GetFileSize(key);

                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    data = new byte[size];
                    await fs.ReadAsync(data, 0, size, token).ConfigureAwait(false);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            return _Deserializer(data);
        }

        /// <summary>
        /// Examine an item at the specified index asynchronously without removing it.
        /// </summary>
        /// <param name="index">Zero-based index of the item.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with the item at the specified index.</returns>
        public async Task<T> PeekAtAsync(int index, CancellationToken token = default)
        {
            byte[] data = await GetBytesAsync(index, token).ConfigureAwait(false);
            return _Deserializer(data);
        }

        /// <summary>
        /// Attempts to dequeue an item from the queue.
        /// </summary>
        /// <param name="result">When this method returns, contains the item if successful; otherwise, the default value.</param>
        /// <returns>true if an item was successfully dequeued; otherwise, false.</returns>
        public bool TryDequeue(out T result)
        {
            try
            {
                result = Dequeue();
                return !EqualityComparer<T>.Default.Equals(result, default);
            }
            catch
            {
                result = default;
                return false;
            }
        }

        /// <summary>
        /// Attempts to peek at an item in the queue without removing it.
        /// </summary>
        /// <param name="result">When this method returns, contains the item if successful; otherwise, the default value.</param>
        /// <returns>true if an item was successfully retrieved; otherwise, false.</returns>
        public bool TryPeek(out T result)
        {
            try
            {
                result = Peek();
                return !EqualityComparer<T>.Default.Equals(result, default);
            }
            catch
            {
                result = default;
                return false;
            }
        }

        /// <summary>
        /// Attempts to get an item at the specified index without removing it.
        /// </summary>
        /// <param name="index">Zero-based index of the item.</param>
        /// <param name="result">When this method returns, contains the item if successful; otherwise, the default value.</param>
        /// <returns>true if an item was successfully retrieved; otherwise, false.</returns>
        public bool TryPeekAt(int index, out T result)
        {
            try
            {
                result = PeekAt(index);
                return !EqualityComparer<T>.Default.Equals(result, default);
            }
            catch
            {
                result = default;
                return false;
            }
        }

        /// <summary>
        /// Asynchronously attempts to dequeue an item from the queue.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with a tuple indicating success and the dequeued item.</returns>
        public async Task<(bool success, T result)> TryDequeueAsync(CancellationToken token = default)
        {
            try
            {
                var result = await DequeueAsync(token).ConfigureAwait(false);
                return (!EqualityComparer<T>.Default.Equals(result, default), result);
            }
            catch
            {
                return (false, default);
            }
        }

        /// <summary>
        /// Asynchronously attempts to peek at an item in the queue without removing it.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with a tuple indicating success and the peeked item.</returns>
        public async Task<(bool success, T result)> TryPeekAsync(CancellationToken token = default)
        {
            try
            {
                var result = await PeekAsync(token).ConfigureAwait(false);
                return (!EqualityComparer<T>.Default.Equals(result, default), result);
            }
            catch
            {
                return (false, default);
            }
        }

        /// <summary>
        /// Asynchronously attempts to get an item at the specified index without removing it.
        /// </summary>
        /// <param name="index">Zero-based index of the item.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with a tuple indicating success and the item at the specified index.</returns>
        public async Task<(bool success, T result)> TryPeekAtAsync(int index, CancellationToken token = default)
        {
            try
            {
                var result = await PeekAtAsync(index, token).ConfigureAwait(false);
                return (!EqualityComparer<T>.Default.Equals(result, default), result);
            }
            catch
            {
                return (false, default);
            }
        }

        /// <summary>
        /// Determines whether the queue contains a specific key.
        /// </summary>
        /// <param name="key">The key to locate in the queue.</param>
        /// <returns>true if the key is found in the queue; otherwise, false.</returns>
        public bool Contains(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            return KeyExists(key);
        }

        /// <summary>
        /// Determines whether the queue contains an element at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index to check.</param>
        /// <returns>true if an element exists at the specified index; otherwise, false.</returns>
        public bool ContainsIndex(int index)
        {
            if (index < 0) return false;
            return GetKeyByIndex(index) != null;
        }

        /// <summary>
        /// Remove a specific entry from the queue.
        /// </summary>
        /// <param name="key">Key.</param>
        public void Remove(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            string actualKey = GetKey(key);

            _Semaphore.Wait();

            try
            {
                if (File.Exists(actualKey))
                {
                    File.Delete(actualKey);
                    RemoveFromIndex(key);
                }
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Remove an entry at the specified index.
        /// </summary>
        /// <param name="index">Zero-based index.</param>
        public void RemoveAt(int index)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            string key = GetKeyByIndex(index);
            if (String.IsNullOrEmpty(key)) throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            Remove(key);
        }

        /// <summary>
        /// Destructively empty the queue. This will delete all data files.
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
        /// Returns an enumerator that iterates through the queue in index order.
        /// </summary>
        /// <returns>An enumerator for the queue.</returns>
        public IEnumerator<T> GetEnumerator()
        {
            // Get all indices in order and iterate through them
            Dictionary<string, int> indexMap = GetIndexMap();
            List<KeyValuePair<string, int>> orderedItems = indexMap
                .OrderBy(kvp => kvp.Value)
                .ToList();

            foreach (var item in orderedItems)
            {
                yield return _Deserializer(GetBytes(item.Key));
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the queue.
        /// </summary>
        /// <returns>An enumerator for the queue.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Copies the queue to an array, starting at a particular array index.
        /// </summary>
        /// <param name="array">The one-dimensional array that is the destination of the elements copied from the queue.</param>
        /// <param name="arrayIndex">The zero-based index in array at which copying begins.</param>
        public void CopyTo(T[] array, int arrayIndex)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex));
            if (array.Length - arrayIndex < Count) throw new ArgumentException("Destination array is too small.");

            _Semaphore.Wait();

            try
            {
                var indexMap = GetIndexMap();
                var orderedItems = indexMap.OrderBy(kvp => kvp.Value).ToList();

                int i = arrayIndex;
                foreach (var item in orderedItems)
                {
                    byte[] data = File.ReadAllBytes(GetKey(item.Key));
                    array[i++] = _Deserializer(data);
                }
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Converts the queue to an array.
        /// </summary>
        /// <returns>A new array containing copies of the elements of the queue.</returns>
        public T[] ToArray()
        {
            _Semaphore.Wait();
            try
            {
                var indexMap = GetIndexMap();
                var orderedItems = indexMap.OrderBy(kvp => kvp.Value).ToList();

                T[] result = new T[orderedItems.Count];
                for (int i = 0; i < orderedItems.Count; i++)
                {
                    byte[] data = GetBytes(orderedItems[i].Key);
                    result[i] = _Deserializer(data);
                }
                return result;
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Returns all keys in the queue, ordered by index.
        /// </summary>
        /// <returns>List of keys.</returns>
        public List<string> GetKeys()
        {
            Dictionary<string, int> indexMap = GetIndexMap();
            return indexMap.OrderBy(kvp => kvp.Value).Select(kvp => kvp.Key).ToList();
        }

        /// <summary>
        /// Returns all keys in the queue asynchronously, ordered by index.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task with the list of keys.</returns>
        public async Task<List<string>> GetKeysAsync(CancellationToken token = default)
        {
            await _Semaphore.WaitAsync(token);
            try
            {
                Dictionary<string, int> indexMap = GetIndexMap();
                return indexMap.OrderBy(kvp => kvp.Value).Select(kvp => kvp.Key).ToList();
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
                        indexMap[line.Value.Key] = line.Value.Value;
                    }
                }
            }

            return indexMap;
        }

        private int GetLastIndex()
        {
            Dictionary<string, int> indexMap = GetIndexMap();
            if (indexMap.Count == 0) return -1;
            return indexMap.Values.Max();
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
            Dictionary<string, int> indexMap = GetIndexMap();
            foreach (var kvp in indexMap)
            {
                if (kvp.Value == index) return kvp.Key;
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

        #endregion
    }
}