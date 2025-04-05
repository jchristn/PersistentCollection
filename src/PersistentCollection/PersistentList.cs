using System.Drawing;
using System.IO;
using System.Threading;
using System;

namespace PersistentCollection
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Persistent list. List entries are backed on disk.
    /// Provides index-based access to data items.
    /// </summary>
    public class PersistentList<T> : IList<T>, IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Number of entries in the list.
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
        /// Number of bytes in the list.
        /// </summary>
        public long Length
        {
            get
            {
                _Semaphore.Wait();

                try
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
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        /// <summary>
        /// Gets the maximum number of elements that the list can contain.
        /// </summary>
        /// <remarks>Returns int.MaxValue since PersistentList is only limited by disk space.</remarks>
        public int Capacity
        {
            get { return int.MaxValue; } // PersistentList is only limited by disk space
            set { /* No-op since we don't use internal capacity */ }
        }

        /// <summary>
        /// Event handler for when data is added to the list.
        /// </summary>
        public EventHandler<string> DataAdded { get; set; }

        /// <summary>
        /// Event handler for when data is removed from the list.
        /// </summary>
        public EventHandler<string> DataRemoved { get; set; }

        /// <summary>
        /// Event handler for when data is updated in the list.
        /// </summary>
        public EventHandler<string> DataUpdated { get; set; }

        /// <summary>
        /// Event handler for when an exception is raised.
        /// </summary>
        public EventHandler<Exception> ExceptionEncountered { get; set; }

        /// <summary>
        /// Event handler for when the list is cleared.
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
        /// Whether the list is read-only. Always returns false.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Gets or sets the element at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the element to get or set.</param>
        /// <returns>The element at the specified index.</returns>
        public T this[int index]
        {
            get
            {
                if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

                _Semaphore.Wait();

                try
                {
                    string key = GetKeyByIndex(index);
                    if (string.IsNullOrEmpty(key))
                        throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

                    // Get the file data directly without calling Get(key) which would try to acquire the semaphore again
                    string actualKey = GetKey(key);
                    int size = GetFileSize(key);

                    byte[] data = null;
                    using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                    {
                        data = new byte[size];
                        fs.Read(data, 0, size);
                    }

                    return DeserializeItem(data);
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
            set
            {
                if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

                _Semaphore.Wait();

                try
                {
                    // Count method uses semaphore
                    int count = 0;
                    string[] files = Directory.GetFiles(_Directory, "*", SearchOption.TopDirectoryOnly);
                    if (files != null)
                    {
                        files = files.Where(f => !Path.GetFileName(f).Equals(_IndexFile)).ToArray();
                        count = files.Length;
                    }

                    if (index >= count)
                        throw new ArgumentOutOfRangeException(nameof(index));

                    string key = GetKeyByIndex(index);
                    if (string.IsNullOrEmpty(key))
                        throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

                    byte[] serialized = SerializeItem(value);

                    // Update directly instead of calling Update(key, serialized)
                    string actualKey = GetKey(key);
                    using (FileStream fs = new FileStream(actualKey, FileMode.Truncate, FileAccess.Write))
                    {
                        fs.Write(serialized, 0, serialized.Length);
                    }

                    DataUpdated?.Invoke(this, key);
                }
                finally
                {
                    _Semaphore.Release();
                }
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
        private JsonSerializerOptions _JsonOptions;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate.
        /// </summary>
        /// <param name="directory">Directory.</param>
        /// <param name="clearOnDispose">Clear the list's contents on dispose. This will delete saved data.</param>
        public PersistentList(string directory, bool clearOnDispose = false)
        {
            if (String.IsNullOrEmpty(directory)) throw new ArgumentNullException(nameof(directory));

            // Normalize directory path
            _Directory = directory.Replace("\\", "/");
            if (!_Directory.EndsWith("/")) _Directory += "/";

            // Create directory if it doesn't exist
            if (!Directory.Exists(_Directory))
                Directory.CreateDirectory(_Directory);

            _DirectoryInfo = new DirectoryInfo(_Directory);
            _ClearOnDispose = clearOnDispose;

            // Ensure the index file exists
            string indexPath = GetKey(_IndexFile);
            if (!File.Exists(indexPath))
            {
                // Create empty index file
                File.WriteAllBytes(indexPath, Array.Empty<byte>());
            }

            _JsonOptions = new JsonSerializerOptions
            {
                WriteIndented = false
            };
        }

        #endregion

        #region IList<T> Implementation

        /// <summary>
        /// Determines the index of a specific item in the list.
        /// </summary>
        /// <param name="item">The object to locate in the list.</param>
        /// <returns>The index of the item if found; otherwise, -1.</returns>
        public int IndexOf(T item)
        {
            for (int i = 0; i < Count; i++)
            {
                T current = this[i];
                if (EqualityComparer<T>.Default.Equals(current, item))
                    return i;
            }
            return -1;
        }

        /// <summary>
        /// Inserts an item at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index at which item should be inserted.</param>
        /// <param name="item">The object to insert into the list.</param>
        public void Insert(int index, T item)
        {
            if (index < 0 || index > Count)
                throw new ArgumentOutOfRangeException(nameof(index));

            _Semaphore.Wait();
            try
            {
                // Create a new key for the item
                string newKey = Guid.NewGuid().ToString();
                byte[] serialized = SerializeItem(item);

                // Save the serialized item
                using (FileStream fs = new FileStream(GetKey(newKey), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    fs.Write(serialized, 0, serialized.Length);
                }

                // Get current indices from the file
                Dictionary<string, int> indexMap = GetIndexMap();
                List<KeyValuePair<string, int>> orderedIndices = indexMap
                    .OrderBy(kvp => kvp.Value)
                    .ToList();

                // Shift indices for items at or after the insertion point
                List<string> updatedLines = new List<string>();
                foreach (var kvp in orderedIndices)
                {
                    int newIndex = kvp.Value >= index ? kvp.Value + 1 : kvp.Value;
                    updatedLines.Add(kvp.Key + " " + newIndex.ToString());
                }

                // Add the new item at the specified index
                updatedLines.Add(newKey + " " + index.ToString());

                // Write all indices back to file
                File.WriteAllLines(GetKey(_IndexFile), updatedLines);

                DataAdded?.Invoke(this, newKey);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Adds an item to the list.
        /// </summary>
        /// <param name="item">The object to add to the list.</param>
        public void Add(T item)
        {
            byte[] serialized = SerializeItem(item);
            Add(serialized);
        }

        /// <summary>
        /// Adds an item to the list with a specified expiration.
        /// </summary>
        /// <param name="item">The object to add to the list.</param>
        /// <param name="expiration">When the item should expire.</param>
        /// <returns>The key of the added item.</returns>
        public string Add(T item, DateTime? expiration)
        {
            byte[] serialized = SerializeItem(item);
            return Add(serialized);
        }

        /// <summary>
        /// Determines whether the list contains a specific item.
        /// </summary>
        /// <param name="item">The object to locate in the list.</param>
        /// <returns>true if the item is found; otherwise, false.</returns>
        public bool Contains(T item)
        {
            return IndexOf(item) >= 0;
        }

        /// <summary>
        /// Copies the entire list to a compatible one-dimensional array, starting at the specified index of the target array.
        /// </summary>
        /// <param name="array">The one-dimensional array that is the destination of the elements copied from list.</param>
        /// <param name="arrayIndex">The zero-based index in array at which copying begins.</param>
        public void CopyTo(T[] array, int arrayIndex)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(arrayIndex));
            if (Count > array.Length - arrayIndex)
                throw new ArgumentException("Not enough space in target array.");

            for (int i = 0; i < Count; i++)
            {
                array[arrayIndex + i] = this[i];
            }
        }

        /// <summary>
        /// Removes the first occurrence of a specific object from the list.
        /// </summary>
        /// <param name="item">The object to remove from the list.</param>
        /// <returns>true if item was successfully removed; otherwise, false.</returns>
        public bool Remove(T item)
        {
            int index = IndexOf(item);
            if (index < 0)
                return false;

            RemoveAt(index);
            return true;
        }

        /// <summary>
        /// Returns an enumerator that iterates through the list.
        /// </summary>
        /// <returns>An enumerator for the list.</returns>
        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return this[i];
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the list.
        /// </summary>
        /// <returns>An enumerator for the list.</returns>
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
                Directory.Delete(_Directory);
            }

            _Directory = null;
            _DirectoryInfo = null;
            _Semaphore = null;
            _IndexFile = null;
        }

        /// <summary>
        /// Add data to the list.
        /// </summary>
        /// <param name="data">Data.</param>
        /// <returns>Key.</returns>
        public string Add(string data)
        {
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return Add(Encoding.UTF8.GetBytes(data));
        }

        /// <summary>
        /// Add data to the list.
        /// </summary>
        /// <param name="data">Data.</param>
        /// <returns>Key.</returns>
        public string Add(byte[] data)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));

            string key = Guid.NewGuid().ToString();

            _Semaphore.Wait();

            try
            {
                using (FileStream fs = new FileStream(GetKey(key), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    fs.Write(data, 0, data.Length);
                }

                int index = GetLastIndex() + 1;
                AddToIndex(key, index);
            }
            finally
            {
                _Semaphore.Release();
            }

            DataAdded?.Invoke(this, key);

            return key;
        }

        /// <summary>
        /// Add data to the list asynchronously.
        /// </summary>
        /// <param name="data">Data.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Key.</returns>
        public async Task<string> AddAsync(string data, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            return await AddAsync(Encoding.UTF8.GetBytes(data), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Add data to the list asynchronously.
        /// </summary>
        /// <param name="data">Data.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Key.</returns>
        public async Task<string> AddAsync(byte[] data, CancellationToken token = default)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));

            string key = Guid.NewGuid().ToString();

            await _Semaphore.WaitAsync(token);

            try
            {
                token.ThrowIfCancellationRequested();

                // Write the data first
                using (FileStream fs = new FileStream(GetKey(key), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    await fs.WriteAsync(data, 0, data.Length, token).ConfigureAwait(false);
                }

                // Then update the index
                int index = GetLastIndex() + 1;
                AddToIndex(key, index);
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

                    // Remove from index by using a direct update to the index file
                    // (instead of calling non-existent RemoveFromIndex)
                    lock (_IndexFileLock)
                    {
                        string filename = GetKey(_IndexFile);
                        List<string> updatedLines = new List<string>();

                        if (File.Exists(filename))
                        {
                            string[] lines = File.ReadAllLines(filename);
                            foreach (string line in lines)
                            {
                                string[] parts = line.Split(new char[] { ' ' }, 2);
                                if (parts.Length == 2 && !parts[0].Equals(key))
                                {
                                    updatedLines.Add(line);
                                }
                            }

                            File.WriteAllLines(filename, updatedLines);
                        }
                    }
                }
                catch
                {
                    // Ignore cleanup errors
                }
                throw; // Re-throw the cancellation exception
            }
            catch (Exception ex)
            {
                // Cleanup on other exceptions
                try
                {
                    string actualKey = GetKey(key);
                    if (File.Exists(actualKey))
                    {
                        File.Delete(actualKey);
                    }

                    // Also remove from index if it was added
                    lock (_IndexFileLock)
                    {
                        string filename = GetKey(_IndexFile);
                        List<string> updatedLines = new List<string>();

                        if (File.Exists(filename))
                        {
                            string[] lines = File.ReadAllLines(filename);
                            foreach (string line in lines)
                            {
                                string[] parts = line.Split(new char[] { ' ' }, 2);
                                if (parts.Length == 2 && !parts[0].Equals(key))
                                {
                                    updatedLines.Add(line);
                                }
                            }

                            File.WriteAllLines(filename, updatedLines);
                        }
                    }
                }
                catch
                {
                    // Ignore cleanup errors
                }

                ExceptionEncountered?.Invoke(this, ex);
                throw;
            }
            finally
            {
                _Semaphore.Release();
            }

            DataAdded?.Invoke(this, key);

            return key;
        }

        /// <summary>
        /// Add an item to the list asynchronously.
        /// </summary>
        /// <param name="item">The item to add.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The key of the added item.</returns>
        public async Task<string> AddAsync(T item, CancellationToken token = default)
        {
            byte[] serialized = SerializeItem(item);
            return await AddAsync(serialized, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Update existing data in the list.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="data">New data.</param>
        public void Update(string key, string data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            Update(key, Encoding.UTF8.GetBytes(data));
        }

        /// <summary>
        /// Update existing data in the list.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="data">New data.</param>
        public void Update(string key, byte[] data)
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
        /// Update existing data in the list asynchronously.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="data">New data.</param>
        /// <param name="token">Cancellation token.</param>
        public async Task UpdateAsync(string key, string data, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            await UpdateAsync(key, Encoding.UTF8.GetBytes(data), token).ConfigureAwait(false);
        }

        /// <summary>
        /// Update existing data in the list asynchronously.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="data">New data.</param>
        /// <param name="token">Cancellation token.</param>
        public async Task UpdateAsync(string key, byte[] data, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (data == null) throw new ArgumentNullException(nameof(data));

            if (!KeyExists(key)) throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

            await _Semaphore.WaitAsync(token);

            try
            {
                token.ThrowIfCancellationRequested();

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
        /// Update an existing item in the list by its key.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="item">The new value of the item.</param>
        public void Update(string key, T item)
        {
            byte[] serialized = SerializeItem(item);
            Update(key, serialized);
        }

        /// <summary>
        /// Update an existing item in the list by its key asynchronously.
        /// </summary>
        /// <param name="key">The key of the item to update.</param>
        /// <param name="item">The new value of the item.</param>
        /// <param name="token">Cancellation token.</param>
        public async Task UpdateAsync(string key, T item, CancellationToken token = default)
        {
            byte[] serialized = SerializeItem(item);
            await UpdateAsync(key, serialized, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieve data from the list by index.
        /// </summary>
        /// <param name="index">Zero-based index.</param>
        /// <returns>Data.</returns>
        public byte[] Get(int index)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            _Semaphore.Wait();
            try
            {
                string key = GetKeyByIndex(index);
                if (String.IsNullOrEmpty(key))
                    throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

                // Get the file data directly
                string actualKey = GetKey(key);
                int size = GetFileSize(key);

                byte[] data = new byte[size];
                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    fs.Read(data, 0, size);
                }

                return data;
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Retrieve data from the list asynchronously by index.
        /// </summary>
        /// <param name="index">Zero-based index.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Data.</returns>
        public async Task<byte[]> GetAsync(int index, CancellationToken token = default)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            // Get the key outside the semaphore
            string key = null;

            await _Semaphore.WaitAsync(token);
            try
            {
                token.ThrowIfCancellationRequested();
                key = GetKeyByIndex(index);
                if (String.IsNullOrEmpty(key))
                    throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");
            }
            finally
            {
                _Semaphore.Release();
            }

            // Then use the key to get the data
            return await GetAsync(key, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieve data from the list by key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>Data.</returns>
        public byte[] Get(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (!KeyExists(key)) throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

            byte[] ret = null;
            string actualKey = GetKey(key);
            int size = GetFileSize(key);

            _Semaphore.Wait();

            try
            {
                using (FileStream fs = new FileStream(actualKey, FileMode.Open, FileAccess.Read))
                {
                    ret = new byte[size];
                    fs.Read(ret, 0, size);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            return ret;
        }

        /// <summary>
        /// Retrieve data from the list asynchronously by key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Data.</returns>
        public async Task<byte[]> GetAsync(string key, CancellationToken token = default)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            // Check if key exists before acquiring the semaphore
            if (!KeyExists(key)) throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

            byte[] ret = null;
            string actualKey = GetKey(key);
            int size = GetFileSize(key);

            await _Semaphore.WaitAsync(token);

            try
            {
                token.ThrowIfCancellationRequested();

                // Read the file
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
        /// Get an item from the list by its key.
        /// </summary>
        /// <param name="key">The key of the item to get.</param>
        /// <returns>The item with the specified key.</returns>
        public T GetByKey(string key)
        {
            byte[] data = Get(key);
            return DeserializeItem(data);
        }

        /// <summary>
        /// Get an item from the list by its key asynchronously.
        /// </summary>
        /// <param name="key">The key of the item to get.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The item with the specified key.</returns>
        public async Task<T> GetByKeyAsync(string key, CancellationToken token = default)
        {
            byte[] data = await GetAsync(key, token).ConfigureAwait(false);
            return DeserializeItem(data);
        }

        /// <summary>
        /// Remove a specific entry from the list by index.
        /// </summary>
        /// <param name="index">Zero-based index.</param>
        public void RemoveAt(int index)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            _Semaphore.Wait();
            try
            {
                string key = GetKeyByIndex(index);
                if (String.IsNullOrEmpty(key))
                    throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

                string actualKey = GetKey(key);

                // Get the current index map
                Dictionary<string, int> indexMap = GetIndexMap();

                // Remove the file
                if (File.Exists(actualKey))
                {
                    File.Delete(actualKey);
                }

                // Update all indices
                List<string> updatedLines = new List<string>();
                foreach (var kvp in indexMap)
                {
                    if (kvp.Key.Equals(key)) continue; // Skip the removed key

                    // Decrement the index for all items that come after the removed item
                    int newIndex = kvp.Value > index ? kvp.Value - 1 : kvp.Value;
                    updatedLines.Add(kvp.Key + " " + newIndex.ToString());
                }

                // Write updated indices back to the file atomically
                File.WriteAllLines(GetKey(_IndexFile), updatedLines);

                DataRemoved?.Invoke(this, key);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Remove a specific entry from the list by key.
        /// </summary>
        /// <param name="key">Key.</param>
        public void Remove(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            _Semaphore.Wait();
            try
            {
                string actualKey = GetKey(key);

                // Get the index map to find the index of the key
                Dictionary<string, int> indexMap = GetIndexMap();

                if (!indexMap.TryGetValue(key, out int removedIndex))
                {
                    // Key doesn't exist in the index, nothing to remove
                    return;
                }

                // Remove the file
                if (File.Exists(actualKey))
                {
                    File.Delete(actualKey);
                }

                // Update all indices
                List<string> updatedLines = new List<string>();
                foreach (var kvp in indexMap)
                {
                    if (kvp.Key.Equals(key)) continue; // Skip the removed key

                    // Decrement the index for all items that come after the removed item
                    int newIndex = kvp.Value > removedIndex ? kvp.Value - 1 : kvp.Value;
                    updatedLines.Add(kvp.Key + " " + newIndex.ToString());
                }

                // Write updated indices back to the file atomically
                File.WriteAllLines(GetKey(_IndexFile), updatedLines);

                DataRemoved?.Invoke(this, key);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Retrieve all keys in the list.
        /// </summary>
        /// <returns>List of keys.</returns>
        public List<string> GetKeys()
        {
            Dictionary<string, int> indexMap = GetIndexMap();
            return indexMap.OrderBy(kvp => kvp.Value).Select(kvp => kvp.Key).ToList();
        }

        /// <summary>
        /// Retrieve all keys asynchronously in the list.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>List of keys.</returns>
        public async Task<List<string>> GetKeysAsync(CancellationToken token = default)
        {
            await _Semaphore.WaitAsync(token);

            try
            {
                token.ThrowIfCancellationRequested();

                Dictionary<string, int> indexMap = GetIndexMap();
                return indexMap.OrderBy(kvp => kvp.Value).Select(kvp => kvp.Key).ToList();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Adds a collection of items to the end of the list.
        /// </summary>
        /// <param name="collection">The collection whose elements should be added to the end of the list.</param>
        public void AddRange(IEnumerable<T> collection)
        {
            if (collection == null)
                throw new ArgumentNullException(nameof(collection));

            foreach (var item in collection)
            {
                Add(item);
            }
        }

        /// <summary>
        /// Adds a collection of items to the end of the list asynchronously.
        /// </summary>
        /// <param name="collection">The collection whose elements should be added to the end of the list.</param>
        /// <param name="token">Cancellation token.</param>
        public async Task AddRangeAsync(IEnumerable<T> collection, CancellationToken token = default)
        {
            if (collection == null)
                throw new ArgumentNullException(nameof(collection));

            // Convert collection to a list to avoid multiple enumerations
            List<T> items = new List<T>(collection);
            if (items.Count == 0)
                return; // Nothing to do

            // Add items one by one to avoid complex locking that might lead to deadlocks
            foreach (var item in items)
            {
                token.ThrowIfCancellationRequested();
                await AddAsync(item, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns a read-only wrapper for the current list.
        /// </summary>
        /// <returns>A read-only wrapper around the current list.</returns>
        public System.Collections.ObjectModel.ReadOnlyCollection<T> AsReadOnly()
        {
            return new System.Collections.ObjectModel.ReadOnlyCollection<T>(ToList());
        }

        /// <summary>
        /// Searches the entire sorted list for an element using the specified comparer and returns the zero-based index of the element.
        /// </summary>
        /// <param name="item">The object to locate. The value can be null for reference types.</param>
        /// <param name="comparer">The comparer implementation to use when comparing elements.</param>
        /// <returns>The zero-based index of item in the sorted list, if item is found; otherwise, a negative number.</returns>
        public int BinarySearch(T item, IComparer<T> comparer = null)
        {
            return BinarySearch(0, Count, item, comparer);
        }

        /// <summary>
        /// Searches a range of elements in the sorted list for an element using the specified comparer and returns the zero-based index of the element.
        /// </summary>
        /// <param name="index">The zero-based starting index of the range to search.</param>
        /// <param name="count">The length of the range to search.</param>
        /// <param name="item">The object to locate. The value can be null for reference types.</param>
        /// <param name="comparer">The comparer implementation to use when comparing elements.</param>
        /// <returns>The zero-based index of item in the sorted list, if item is found; otherwise, a negative number.</returns>
        public int BinarySearch(int index, int count, T item, IComparer<T> comparer = null)
        {
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (index + count > Count)
                throw new ArgumentException("Index and count do not denote a valid range in the list.");

            comparer = comparer ?? Comparer<T>.Default;

            // Retrieve all items in the range for binary search
            List<T> items = new List<T>(count);
            for (int i = 0; i < count; i++)
            {
                items.Add(this[index + i]);
            }

            return items.BinarySearch(item, comparer);
        }

        /// <summary>
        /// Removes all elements that match the conditions defined by the specified predicate.
        /// </summary>
        /// <param name="match">The predicate delegate that defines the conditions of the elements to remove.</param>
        /// <returns>The number of elements removed from the list.</returns>
        public int RemoveAll(Predicate<T> match)
        {
            if (match == null)
                throw new ArgumentNullException(nameof(match));

            int count = 0;
            for (int i = Count - 1; i >= 0; i--)
            {
                if (match(this[i]))
                {
                    RemoveAt(i);
                    count++;
                }
            }

            return count;
        }

        /// <summary>
        /// Determines whether every element in the list matches the conditions defined by the specified predicate.
        /// </summary>
        /// <param name="match">The predicate delegate that defines the conditions to check against the elements.</param>
        /// <returns>true if every element in the list matches the conditions defined by the specified predicate; otherwise, false.</returns>
        public bool TrueForAll(Predicate<T> match)
        {
            if (match == null)
                throw new ArgumentNullException(nameof(match));

            for (int i = 0; i < Count; i++)
            {
                if (!match(this[i]))
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Reverses the order of the elements in the entire list.
        /// </summary>
        public void Reverse()
        {
            Reverse(0, Count);
        }

        /// <summary>
        /// Reverses the order of the elements in the specified range.
        /// </summary>
        /// <param name="index">The zero-based starting index of the range to reverse.</param>
        /// <param name="count">The number of elements in the range to reverse.</param>
        public void Reverse(int index, int count)
        {
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (index + count > Count)
                throw new ArgumentException("Index and count do not denote a valid range in the list.");

            int start = index;
            int end = index + count - 1;

            while (start < end)
            {
                T temp = this[start];
                this[start] = this[end];
                this[end] = temp;

                start++;
                end--;
            }
        }

        /// <summary>
        /// Sorts the elements in the entire list using the default comparer.
        /// </summary>
        public void Sort()
        {
            Sort(0, Count, null);
        }

        /// <summary>
        /// Sorts the elements in the entire list using the specified comparer.
        /// </summary>
        /// <param name="comparer">The comparer implementation to use when comparing elements.</param>
        public void Sort(IComparer<T> comparer)
        {
            Sort(0, Count, comparer);
        }

        /// <summary>
        /// Sorts the elements in a range of elements in the list using the specified comparer.
        /// </summary>
        /// <param name="index">The zero-based starting index of the range to sort.</param>
        /// <param name="count">The length of the range to sort.</param>
        /// <param name="comparer">The comparer implementation to use when comparing elements.</param>
        public void Sort(int index, int count, IComparer<T> comparer)
        {
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (index + count > Count)
                throw new ArgumentException("Index and count do not denote a valid range in the list.");

            // Get a list of all items in the range
            List<T> items = new List<T>(count);
            for (int i = 0; i < count; i++)
            {
                items.Add(this[index + i]);
            }

            // Sort the list
            items.Sort(comparer);

            // Replace the items in the original list
            for (int i = 0; i < count; i++)
            {
                this[index + i] = items[i];
            }
        }

        /// <summary>
        /// Sorts the elements in the entire list using the specified comparison.
        /// </summary>
        /// <param name="comparison">The comparison to use when comparing elements.</param>
        public void Sort(Comparison<T> comparison)
        {
            if (comparison == null)
                throw new ArgumentNullException(nameof(comparison));

            // Get all items
            List<T> items = ToList();

            // Sort the list using the comparison
            items.Sort(comparison);

            // Replace all items
            for (int i = 0; i < items.Count; i++)
            {
                this[i] = items[i];
            }
        }

        /// <summary>
        /// Performs the specified action on each element of the list.
        /// </summary>
        /// <param name="action">The action delegate to perform on each element of the list.</param>
        public void ForEach(Action<T> action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            for (int i = 0; i < Count; i++)
            {
                action(this[i]);
            }
        }

        /// <summary>
        /// Searches for an element that matches the conditions defined by the specified predicate, 
        /// and returns the first occurrence within the entire list.
        /// </summary>
        /// <param name="match">The predicate delegate that defines the conditions of the element to search for.</param>
        /// <returns>The first element that matches the conditions, or default(T) if not found.</returns>
        public T Find(Predicate<T> match)
        {
            if (match == null)
                throw new ArgumentNullException(nameof(match));

            for (int i = 0; i < Count; i++)
            {
                T item = this[i];
                if (match(item))
                    return item;
            }

            return default;
        }

        /// <summary>
        /// Searches for an element that matches the conditions defined by the specified predicate, 
        /// and returns the zero-based index of the first occurrence within the entire list.
        /// </summary>
        /// <param name="match">The predicate delegate that defines the conditions of the element to search for.</param>
        /// <returns>The zero-based index of the first element that matches the conditions, or -1 if not found.</returns>
        public int FindIndex(Predicate<T> match)
        {
            return FindIndex(0, Count, match);
        }

        /// <summary>
        /// Searches for an element that matches the conditions defined by the specified predicate, 
        /// and returns the zero-based index of the first occurrence within the range of elements 
        /// that extends from the specified index to the end of the list.
        /// </summary>
        /// <param name="startIndex">The zero-based starting index of the search.</param>
        /// <param name="match">The predicate delegate that defines the conditions of the element to search for.</param>
        /// <returns>The zero-based index of the first element that matches the conditions, or -1 if not found.</returns>
        public int FindIndex(int startIndex, Predicate<T> match)
        {
            return FindIndex(startIndex, Count - startIndex, match);
        }

        /// <summary>
        /// Searches for an element that matches the conditions defined by the specified predicate, 
        /// and returns the zero-based index of the first occurrence within the range of elements 
        /// that starts at the specified index and contains the specified number of elements.
        /// </summary>
        /// <param name="startIndex">The zero-based starting index of the search.</param>
        /// <param name="count">The number of elements in the section to search.</param>
        /// <param name="match">The predicate delegate that defines the conditions of the element to search for.</param>
        /// <returns>The zero-based index of the first element that matches the conditions, or -1 if not found.</returns>
        public int FindIndex(int startIndex, int count, Predicate<T> match)
        {
            if (startIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (startIndex + count > Count)
                throw new ArgumentException("StartIndex and count do not denote a valid range in the list.");
            if (match == null)
                throw new ArgumentNullException(nameof(match));

            int endIndex = startIndex + count;
            for (int i = startIndex; i < endIndex; i++)
            {
                if (match(this[i]))
                    return i;
            }

            return -1;
        }

        /// <summary>
        /// Searches for an element that matches the conditions defined by the specified predicate, 
        /// and returns the last occurrence within the entire list.
        /// </summary>
        /// <param name="match">The predicate delegate that defines the conditions of the element to search for.</param>
        /// <returns>The last element that matches the conditions, or default(T) if not found.</returns>
        public T FindLast(Predicate<T> match)
        {
            if (match == null)
                throw new ArgumentNullException(nameof(match));

            for (int i = Count - 1; i >= 0; i--)
            {
                T item = this[i];
                if (match(item))
                    return item;
            }

            return default;
        }

        /// <summary>
        /// Searches for an element that matches the conditions defined by the specified predicate, 
        /// and returns the zero-based index of the last occurrence within the entire list.
        /// </summary>
        /// <param name="match">The predicate delegate that defines the conditions of the element to search for.</param>
        /// <returns>The zero-based index of the last element that matches the conditions, or -1 if not found.</returns>
        public int FindLastIndex(Predicate<T> match)
        {
            return FindLastIndex(Count - 1, Count, match);
        }

        /// <summary>
        /// Searches for an element that matches the conditions defined by the specified predicate, 
        /// and returns the zero-based index of the last occurrence within the range of elements 
        /// that extends from the first element to the specified index.
        /// </summary>
        /// <param name="startIndex">The zero-based starting index of the backward search.</param>
        /// <param name="match">The predicate delegate that defines the conditions of the element to search for.</param>
        /// <returns>The zero-based index of the last element that matches the conditions, or -1 if not found.</returns>
        public int FindLastIndex(int startIndex, Predicate<T> match)
        {
            return FindLastIndex(startIndex, startIndex + 1, match);
        }

        /// <summary>
        /// Searches for an element that matches the conditions defined by the specified predicate, 
        /// and returns the zero-based index of the last occurrence within the range of elements 
        /// that contains the specified number of elements and ends at the specified index.
        /// </summary>
        /// <param name="startIndex">The zero-based starting index of the backward search.</param>
        /// <param name="count">The number of elements in the section to search.</param>
        /// <param name="match">The predicate delegate that defines the conditions of the element to search for.</param>
        /// <returns>The zero-based index of the last element that matches the conditions, or -1 if not found.</returns>
        public int FindLastIndex(int startIndex, int count, Predicate<T> match)
        {
            if (Count == 0)
            {
                if (startIndex != -1)
                    throw new ArgumentOutOfRangeException(nameof(startIndex));
            }
            else
            {
                if (startIndex >= Count)
                    throw new ArgumentOutOfRangeException(nameof(startIndex));
            }

            if (count < 0 || startIndex - count + 1 < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (match == null)
                throw new ArgumentNullException(nameof(match));

            int endIndex = startIndex - count + 1;
            for (int i = startIndex; i >= endIndex; i--)
            {
                if (match(this[i]))
                    return i;
            }

            return -1;
        }

        /// <summary>
        /// Retrieves all the elements that match the conditions defined by the specified predicate.
        /// </summary>
        /// <param name="match">The predicate delegate that defines the conditions of the elements to search for.</param>
        /// <returns>A list containing all the elements that match the conditions.</returns>
        public List<T> FindAll(Predicate<T> match)
        {
            if (match == null)
                throw new ArgumentNullException(nameof(match));

            List<T> result = new List<T>();
            for (int i = 0; i < Count; i++)
            {
                T item = this[i];
                if (match(item))
                    result.Add(item);
            }

            return result;
        }

        /// <summary>
        /// Determines whether the list contains elements that match the conditions defined by the specified predicate.
        /// </summary>
        /// <param name="match">The predicate delegate that defines the conditions of the elements to search for.</param>
        /// <returns>true if the list contains one or more elements that match the conditions; otherwise, false.</returns>
        public bool Exists(Predicate<T> match)
        {
            return FindIndex(match) != -1;
        }

        /// <summary>
        /// Converts the elements in the current list to another type, and returns a list containing the converted elements.
        /// </summary>
        /// <typeparam name="TOutput">The type of the elements of the target list.</typeparam>
        /// <param name="converter">A converter delegate that converts each element from one type to another type.</param>
        /// <returns>A list of the target type containing the converted elements.</returns>
        public List<TOutput> ConvertAll<TOutput>(Converter<T, TOutput> converter)
        {
            if (converter == null)
                throw new ArgumentNullException(nameof(converter));

            List<TOutput> result = new List<TOutput>(Count);
            for (int i = 0; i < Count; i++)
            {
                result.Add(converter(this[i]));
            }

            return result;
        }

        /// <summary>
        /// Inserts the elements of a collection into the list at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index at which the new elements should be inserted.</param>
        /// <param name="collection">The collection whose elements should be inserted into the list.</param>
        public void InsertRange(int index, IEnumerable<T> collection)
        {
            if (index < 0 || index > Count)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (collection == null)
                throw new ArgumentNullException(nameof(collection));

            List<T> itemsToInsert = new List<T>(collection);
            if (itemsToInsert.Count == 0)
                return; // Nothing to insert

            _Semaphore.Wait();
            try
            {
                // Get all existing keys and indices
                Dictionary<string, int> indexMap = GetIndexMap();
                List<KeyValuePair<string, int>> orderedIndices = indexMap
                    .OrderBy(kvp => kvp.Value)
                    .ToList();

                // Create new keys and save all items to insert
                List<string> newKeys = new List<string>();
                foreach (var item in itemsToInsert)
                {
                    string newKey = Guid.NewGuid().ToString();
                    newKeys.Add(newKey);

                    byte[] serialized = SerializeItem(item);
                    using (FileStream fs = new FileStream(GetKey(newKey), FileMode.OpenOrCreate, FileAccess.Write))
                    {
                        fs.Write(serialized, 0, serialized.Length);
                    }
                }

                // Update indices for all existing items
                List<string> updatedLines = new List<string>();
                foreach (var kvp in orderedIndices)
                {
                    int newIndex = kvp.Value >= index ? kvp.Value + itemsToInsert.Count : kvp.Value;
                    updatedLines.Add(kvp.Key + " " + newIndex.ToString());
                }

                // Add indices for all new items
                for (int i = 0; i < newKeys.Count; i++)
                {
                    updatedLines.Add(newKeys[i] + " " + (index + i).ToString());
                }

                // Write all indices back to file
                File.WriteAllLines(GetKey(_IndexFile), updatedLines);

                // Raise events for all added items
                foreach (var newKey in newKeys)
                {
                    DataAdded?.Invoke(this, newKey);
                }
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Removes a range of elements from the list.
        /// </summary>
        /// <param name="index">The zero-based starting index of the range of elements to remove.</param>
        /// <param name="count">The number of elements to remove.</param>
        public void RemoveRange(int index, int count)
        {
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (index + count > Count)
                throw new ArgumentException("Index and count do not denote a valid range in the list.");

            for (int i = index + count - 1; i >= index; i--)
            {
                RemoveAt(i);
            }
        }

        /// <summary>
        /// Returns a view of a range of elements in the source list.
        /// </summary>
        /// <param name="index">The zero-based starting position in the source list.</param>
        /// <param name="count">The number of elements in the range.</param>
        /// <returns>A view of a range of elements in the source list.</returns>
        public List<T> GetRange(int index, int count)
        {
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (index + count > Count)
                throw new ArgumentException("Index and count do not denote a valid range in the list.");

            List<T> range = new List<T>(count);
            for (int i = 0; i < count; i++)
            {
                range.Add(this[index + i]);
            }

            return range;
        }

        /// <summary>
        /// Ensures that the size of the list can accommodate the specified number of elements.
        /// </summary>
        /// <param name="capacity">The minimum size to ensure.</param>
        /// <remarks>This is a no-op in PersistentList as it doesn't use internal capacity like regular List.</remarks>
        public void EnsureCapacity(int capacity)
        {
            // No-op for PersistentList since it doesn't use internal capacity
        }

        /// <summary>
        /// Destructively empty the list. This will delete all of the data files in the directory.
        /// </summary>
        public void Clear()
        {
            _Semaphore.Wait();

            try
            {
                // First get all files to delete
                List<string> filesToDelete = new List<string>();
                foreach (FileInfo file in _DirectoryInfo.GetFiles())
                {
                    if (!file.Name.Equals(_IndexFile)) // Don't delete the index file, just clear it
                    {
                        filesToDelete.Add(file.FullName);
                    }
                }

                // Delete all data files
                foreach (string file in filesToDelete)
                {
                    File.Delete(file);
                }

                // Clear index file
                File.WriteAllText(GetKey(_IndexFile), string.Empty);

                // Remove subdirectories if any
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
        /// Get a copy of this list.
        /// </summary>
        /// <returns>A copy of the list.</returns>
        public List<T> ToList()
        {
            List<T> list = new List<T>(Count);
            for (int i = 0; i < Count; i++)
            {
                list.Add(this[i]);
            }
            return list;
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
                        if (string.IsNullOrWhiteSpace(lines[i])) continue;

                        KeyValuePair<string, int>? line = ParseIndexLine(i, lines[i]);
                        if (line == null) continue;
                        indexMap[line.Value.Key] = line.Value.Value;
                    }
                }
            }

            return indexMap;
        }

        private async Task<Dictionary<string, int>> GetIndexMapAsync(CancellationToken token = default)
        {
            Dictionary<string, int> indexMap = new Dictionary<string, int>();
            string filename = GetKey(_IndexFile);

            token.ThrowIfCancellationRequested();

            if (!File.Exists(filename))
                File.WriteAllBytes(filename, Array.Empty<byte>());

            // Read file asynchronously using FileStream instead of File.ReadAllLinesAsync
            List<string> lines = new List<string>();
            using (FileStream fs = new FileStream(filename, FileMode.Open, FileAccess.Read))
            using (StreamReader reader = new StreamReader(fs, Encoding.UTF8))
            {
                string line;
                while ((line = await reader.ReadLineAsync().ConfigureAwait(false)) != null)
                {
                    lines.Add(line);
                }
            }

            if (lines.Count > 0)
            {
                for (int i = 0; i < lines.Count; i++)
                {
                    if (string.IsNullOrWhiteSpace(lines[i])) continue;

                    KeyValuePair<string, int>? parsedLine = ParseIndexLine(i, lines[i]);
                    if (parsedLine == null) continue;
                    indexMap[parsedLine.Value.Key] = parsedLine.Value.Value;
                }
            }

            return indexMap;
        }

        private async Task SaveIndexMapAsync(List<KeyValuePair<string, int>> indexEntries, CancellationToken token = default)
        {
            string filename = GetKey(_IndexFile);
            List<string> lines = new List<string>();

            foreach (var kvp in indexEntries)
            {
                if (!string.IsNullOrEmpty(kvp.Key))
                {
                    lines.Add(kvp.Key + " " + kvp.Value.ToString());
                }
            }

            token.ThrowIfCancellationRequested();

            try
            {
                // Use atomic file operation to prevent corruption
                string tempFile = filename + ".tmp";
                await WriteAllLinesAsync(tempFile, lines, token);

                if (File.Exists(filename))
                    File.Delete(filename);

                File.Move(tempFile, filename);
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException)
                    throw;

                ExceptionEncountered?.Invoke(this, ex);

                // Fallback to direct write if the atomic approach fails
                await WriteAllLinesAsync(filename, lines, token);
            }
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

        private async Task WriteAllLinesAsync(string path, IEnumerable<string> lines, CancellationToken token = default)
        {
            using (FileStream fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None,
                                               bufferSize: 4096, useAsync: true))
            using (StreamWriter writer = new StreamWriter(fs, Encoding.UTF8))
            {
                foreach (string line in lines)
                {
                    token.ThrowIfCancellationRequested();
                    await writer.WriteLineAsync(line).ConfigureAwait(false);
                }
                await writer.FlushAsync().ConfigureAwait(false);
            }
        }

        private async Task<string[]> ReadAllLinesAsync(string path, CancellationToken token = default)
        {
            List<string> lines = new List<string>();

            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
                                               bufferSize: 4096, useAsync: true))
            using (StreamReader reader = new StreamReader(fs, Encoding.UTF8))
            {
                string line;
                while ((line = await reader.ReadLineAsync().ConfigureAwait(false)) != null)
                {
                    token.ThrowIfCancellationRequested();
                    lines.Add(line);
                }
            }

            return lines.ToArray();
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

        private byte[] SerializeItem(T item)
        {
            // Special handling for string type to maintain backward compatibility
            if (typeof(T) == typeof(string))
            {
                string strValue = item as string;
                return Encoding.UTF8.GetBytes(strValue ?? string.Empty);
            }

            // For other types, use JSON serialization
            return JsonSerializer.SerializeToUtf8Bytes(item, _JsonOptions);
        }

        private T DeserializeItem(byte[] data)
        {
            // Special handling for string type to maintain backward compatibility
            if (typeof(T) == typeof(string))
            {
                string strValue = Encoding.UTF8.GetString(data);
                return (T)(object)strValue;
            }

            // For other types, use JSON deserialization
            return JsonSerializer.Deserialize<T>(data, _JsonOptions);
        }

        #endregion
    }
}
