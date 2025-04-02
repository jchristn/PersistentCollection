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

            _Directory = directory;

            InitializeDirectory();

            _ClearOnDispose = clearOnDispose;

            if (!File.Exists(GetKey(_IndexFile)))
                File.WriteAllBytes(GetKey(_IndexFile), Array.Empty<byte>());

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

            // Need to shift all items after the insertion point
            List<T> items = new List<T>();
            for (int i = index; i < Count; i++)
            {
                items.Add(this[i]);
            }

            // Remove all items from index to end
            for (int i = Count - 1; i >= index; i--)
            {
                RemoveAt(i);
            }

            // Insert the new item
            Add(item);

            // Add back the shifted items
            foreach (var shiftedItem in items)
            {
                Add(shiftedItem);
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

            await _Semaphore.WaitAsync();

            try
            {
                using (FileStream fs = new FileStream(GetKey(key), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    await fs.WriteAsync(data, 0, data.Length, token).ConfigureAwait(false);
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

            await _Semaphore.WaitAsync();

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

            string key = GetKeyByIndex(index);
            if (String.IsNullOrEmpty(key)) throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

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
            if (!KeyExists(key)) throw new KeyNotFoundException("The specified key '" + key + "' does not exist.");

            byte[] ret = null;
            string actualKey = GetKey(key);
            int size = GetFileSize(key);

            await _Semaphore.WaitAsync();

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

            string key = GetKeyByIndex(index);
            if (String.IsNullOrEmpty(key)) throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");

            Remove(key);
        }

        /// <summary>
        /// Remove a specific entry from the list by key.
        /// </summary>
        /// <param name="key">Key.</param>
        public void Remove(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            string actualKey = GetKey(key);
            int removedIndex = -1;

            _Semaphore.Wait();

            try
            {
                // Get the index of the key we're removing
                Dictionary<string, int> indexMap = GetIndexMap();
                if (indexMap.TryGetValue(key, out removedIndex))
                {
                    // Remove the file
                    if (File.Exists(actualKey))
                    {
                        File.Delete(actualKey);
                    }

                    // Update all indexes
                    List<string> updatedLines = new List<string>();
                    foreach (var kvp in indexMap)
                    {
                        if (kvp.Key.Equals(key)) continue; // Skip the removed key

                        // Decrement the index for all items that come after the removed item
                        int newIndex = kvp.Value > removedIndex ? kvp.Value - 1 : kvp.Value;
                        updatedLines.Add(kvp.Key + " " + newIndex.ToString());
                    }

                    // Write updated indexes back to the file
                    File.WriteAllLines(GetKey(_IndexFile), updatedLines);
                }
            }
            finally
            {
                _Semaphore.Release();
            }

            DataRemoved?.Invoke(this, key);
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

            foreach (var item in collection)
            {
                await AddAsync(item, token).ConfigureAwait(false);
                if (token.IsCancellationRequested) break;
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

            // Need to shift all items after the insertion point
            List<T> itemsToShift = new List<T>();
            for (int i = index; i < Count; i++)
            {
                itemsToShift.Add(this[i]);
            }

            // Remove all items from index to end
            for (int i = Count - 1; i >= index; i--)
            {
                RemoveAt(i);
            }

            // Insert the new items
            foreach (var item in itemsToInsert)
            {
                Add(item);
            }

            // Add back the shifted items
            foreach (var item in itemsToShift)
            {
                Add(item);
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
