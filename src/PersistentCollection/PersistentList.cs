namespace PersistentCollection
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Text.Json;
    using System.Threading;

    /// <summary>
    /// Persistent list. List entries are backed on disk.
    /// Provides index-based access to data items.
    /// </summary>
    public class PersistentList<T> : IList<T>, IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Boolean indicating if the object is thread-safe.
        /// </summary>
        public bool IsSynchronized
        {
            get
            {
                return true;
            }
        }

        /// <summary>
        /// Object to use in synchronization across threads.
        /// </summary>
        public object SyncRoot
        {
            get
            {
                return this;
            }
        }

        /// <summary>
        /// Number of entries waiting in the list.
        /// </summary>
        public int Count => ((ICollection<T>)this).Count;

        /// <summary>
        /// Boolean indicating if the list is read-only.  This will always be false.
        /// </summary>
        public bool IsReadOnly => ((ICollection<T>)this).IsReadOnly;

        /// <summary>
        /// Retrieve by index.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value at index.</returns>
        public T this[int index]
        {
            get => ((IList<T>)this)[index];
            set => ((IList<T>)this)[index] = value;
        }

        /// <summary>
        /// Retrieve index of an item.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>Index.</returns>
        public int IndexOf(T item) => ((IList<T>)this).IndexOf(item);

        /// <summary>
        /// Insert an item at a specific index.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="item">Item.</param>
        public void Insert(int index, T item) => ((IList<T>)this).Insert(index, item);

        /// <summary>
        /// Remove an item at a specific index.
        /// </summary>
        /// <param name="index">Index.</param>
        public void RemoveAt(int index) => ((IList<T>)this).RemoveAt(index);

        /// <summary>
        /// Add an item.
        /// </summary>
        /// <param name="item">Item.</param>
        public void Add(T item) => ((ICollection<T>)this).Add(item);

        /// <summary>
        /// Clear the list.
        /// </summary>
        public void Clear() => ((ICollection<T>)this).Clear();

        /// <summary>
        /// Check if the list contains an item.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if exists.</returns>
        public bool Contains(T item) => ((ICollection<T>)this).Contains(item);

        /// <summary>
        /// Copy the list to an array.
        /// </summary>
        /// <param name="array">Array.</param>
        /// <param name="arrayIndex">Array index.</param>
        public void CopyTo(T[] array, int arrayIndex) => ((ICollection<T>)this).CopyTo(array, arrayIndex);

        /// <summary>
        /// Remove an item.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if removed.</returns>
        public bool Remove(T item) => ((ICollection<T>)this).Remove(item);

        /// <summary>
        /// Get enumerator.
        /// </summary>
        /// <returns>Enumerator.</returns>
        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)this).GetEnumerator();

        #endregion

        #region Private-Members

        private string _PersistenceFile = null;
        private List<T> _List = new List<T>();

        private readonly object _PersistenceFileLock = new object();
        private SemaphoreSlim _Semaphore = new SemaphoreSlim(1, 1);

        private bool _Disposed = false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Persistent list.
        /// </summary>
        /// <param name="persistenceFile">Persistence file.</param>
        public PersistentList(string persistenceFile = "./list.idx")
        {
            if (String.IsNullOrEmpty(persistenceFile)) throw new ArgumentNullException(nameof(persistenceFile));

            _PersistenceFile = persistenceFile;

            if (File.Exists(_PersistenceFile)) PopulateFromFile();
        }

        #endregion

        #region IList-Interface-Implementation

        int ICollection<T>.Count
        {
            get
            {
                try
                {
                    _Semaphore.Wait();
                    return _List.Count;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        bool ICollection<T>.IsReadOnly
        {
            get
            {
                return false;
            }
        }

        T IList<T>.this[int index]
        {
            get
            {
                try
                {
                    _Semaphore.Wait();
                    return _List[index];
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
            set
            {
                try
                {
                    _Semaphore.Wait();
                    _List[index] = value;
                    RewritePersistenceFile();
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        int IList<T>.IndexOf(T item)
        {
            try
            {
                _Semaphore.Wait();
                return _List.IndexOf(item);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        void IList<T>.Insert(int index, T item)
        {
            try
            {
                _Semaphore.Wait();
                _List.Insert(index, item);
                RewritePersistenceFile();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        void IList<T>.RemoveAt(int index)
        {
            try
            {
                _Semaphore.Wait();
                _List.RemoveAt(index);
                RewritePersistenceFile();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        void ICollection<T>.Add(T item)
        {
            try
            {
                _Semaphore.Wait();
                _List.Add(item);
                RewritePersistenceFile();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        void ICollection<T>.Clear()
        {
            try
            {
                _Semaphore.Wait();
                _List.Clear();
                ClearPersistenceFile();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        bool ICollection<T>.Contains(T item)
        {
            try
            {
                _Semaphore.Wait();
                return _List.Contains(item);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        void ICollection<T>.CopyTo(T[] array, int arrayIndex)
        {
            try
            {
                _Semaphore.Wait();
                _List.CopyTo(array, arrayIndex);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        bool ICollection<T>.Remove(T item)
        {
            try
            {
                _Semaphore.Wait();

                int index = _List.IndexOf(item);
                if (index >= 0)
                {
                    _List.RemoveAt(index);
                    RewritePersistenceFile();
                    return true;
                }

                return false;
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            List<T> snapshot;

            try
            {
                _Semaphore.Wait();
                snapshot = new List<T>(_List);
            }
            finally
            {
                _Semaphore.Release();
            }

            return snapshot.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<T>)this).GetEnumerator();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Dispose.
        /// </summary>
        /// <param name="disposing">Disposing.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_Disposed)
            {
                if (disposing)
                {
                    if (_List != null) _List.Clear();
                    _Semaphore?.Dispose();
                }

                _List = null;
                _Semaphore = null;
                _Disposed = true;
            }
        }

        /// <summary>
        /// Dispose.
        /// </summary>
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Private-Methods

        private void PopulateFromFile()
        {
            lock (_PersistenceFileLock)
            {
                if (_List == null) _List = new List<T>();
                else _List.Clear();

                using (StreamReader reader = new StreamReader(_PersistenceFile))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        // 1 { "foo": "bar" }
                        // 2 
                        // 3 { "hello": "world" }
                        if (String.IsNullOrWhiteSpace(line)) continue;

                        int spaceIndex = line.IndexOf(' ');
                        if (spaceIndex == -1)
                        {
                            if (int.TryParse(line, out int id))
                            {
                                while (_List.Count <= id) _List.Add(default(T)); // Ensure list has enough capacity
                                _List[id] = default(T);
                            }
                            continue;
                        }
                        else
                        {
                            if (!int.TryParse(line.Substring(0, spaceIndex), out int id))
                                continue;

                            string json = line.Substring(spaceIndex + 1).Trim();

                            // Handle empty JSON part
                            if (string.IsNullOrWhiteSpace(json))
                            {
                                while (_List.Count <= id)
                                {
                                    _List.Add(default(T));
                                }
                                _List[id] = default(T);
                                continue;
                            }

                            T data = JsonSerializer.Deserialize<T>(json);
                            while (_List.Count <= id) _List.Add(default(T)); // Ensure list has enough capacity
                            _List[id] = data;
                        }
                    }
                }
            }
        }

        private void RewritePersistenceFile()
        {
            lock (_PersistenceFileLock)
            {
                string dir = Path.GetDirectoryName(_PersistenceFile);
                if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

                using (StreamWriter writer = new StreamWriter(_PersistenceFile, false))
                {
                    for (int i = 0; i < _List.Count; i++)
                    {
                        T item = _List[i];
                        writer.Write($"{i} ");
                        if (item == null || EqualityComparer<T>.Default.Equals(item, default(T)))
                        {
                            writer.WriteLine();
                        }
                        else
                        {
                            string json = JsonSerializer.Serialize(item);
                            writer.WriteLine(json);
                        }
                    }
                }
            }
        }

        private void ClearPersistenceFile()
        {
            lock (_PersistenceFileLock)
            {
                using (StreamWriter writer = new StreamWriter(_PersistenceFile, false))
                {
                    // write nothing
                }
            }
        }

        #endregion
    }
}

