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

        private const string _CollectionType = PersistentCollectionTelemetry.CollectionTypeList;

        private string _PersistenceFile = null;
        private string _FileName = null;
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
            _FileName = Path.GetFileName(persistenceFile);

            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "open", "PersistentList open"))
            {
                try
                {
                    if (File.Exists(_PersistenceFile)) PopulateFromFile();
                    scope?.SetItemCount(_List.Count);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }

            CollectionTelemetry.AddInstance(_CollectionType, 1);
            CollectionTelemetry.AddItems(_CollectionType, _List.Count);
        }

        #endregion

        #region IList-Interface-Implementation

        int ICollection<T>.Count
        {
            get
            {
                using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "count", null))
                {
                    try
                    {
                        _Semaphore.Wait();
                        scope?.LockAcquired();
                        return _List.Count;
                    }
                    catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                    {
                        throw;
                    }
                    finally
                    {
                        _Semaphore.Release();
                    }
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
                using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "get", null))
                {
                    try
                    {
                        _Semaphore.Wait();
                        scope?.LockAcquired();
                        return _List[index];
                    }
                    catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                    {
                        throw;
                    }
                    finally
                    {
                        _Semaphore.Release();
                    }
                }
            }
            set
            {
                using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "set", "PersistentList set"))
                {
                    try
                    {
                        _Semaphore.Wait();
                        scope?.LockAcquired();
                        _List[index] = value;
                        RewritePersistenceFile();
                    }
                    catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                    {
                        throw;
                    }
                    finally
                    {
                        _Semaphore.Release();
                    }
                }
            }
        }

        int IList<T>.IndexOf(T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "index_of", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    return _List.IndexOf(item);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        void IList<T>.Insert(int index, T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "insert", "PersistentList insert"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    _List.Insert(index, item);
                    CollectionTelemetry.AddItems(_CollectionType, 1);
                    RewritePersistenceFile();
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        void IList<T>.RemoveAt(int index)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "remove_at", "PersistentList remove_at"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    _List.RemoveAt(index);
                    CollectionTelemetry.AddItems(_CollectionType, -1);
                    RewritePersistenceFile();
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        void ICollection<T>.Add(T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "add", "PersistentList add"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    _List.Add(item);
                    CollectionTelemetry.AddItems(_CollectionType, 1);
                    RewritePersistenceFile();
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        void ICollection<T>.Clear()
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "clear", "PersistentList clear"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    CollectionTelemetry.AddItems(_CollectionType, -_List.Count);
                    _List.Clear();
                    ClearPersistenceFile();
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        bool ICollection<T>.Contains(T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "contains", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    return _List.Contains(item);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        void ICollection<T>.CopyTo(T[] array, int arrayIndex)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "copy_to", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    _List.CopyTo(array, arrayIndex);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        bool ICollection<T>.Remove(T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "remove", "PersistentList remove"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();

                    int index = _List.IndexOf(item);
                    if (index >= 0)
                    {
                        _List.RemoveAt(index);
                        CollectionTelemetry.AddItems(_CollectionType, -1);
                        RewritePersistenceFile();
                        return true;
                    }

                    scope?.MarkMiss();
                    return false;
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
                finally
                {
                    _Semaphore.Release();
                }
            }
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            List<T> snapshot;

            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "enumerate", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    snapshot = new List<T>(_List);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
                finally
                {
                    _Semaphore.Release();
                }
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
                    CollectionTelemetry.AddInstance(_CollectionType, -1);
                    if (_List != null)
                    {
                        CollectionTelemetry.AddItems(_CollectionType, -_List.Count);
                        _List.Clear();
                    }
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
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceRead, "PersistentList persistence.read", _FileName))
            {
                try
                {
                    lock (_PersistenceFileLock)
                    {
                        if (_List == null) _List = new List<T>();
                        else _List.Clear();

                        if (File.Exists(_PersistenceFile))
                        {
                            string json = File.ReadAllText(_PersistenceFile);
                            scope?.SetBytes(Encoding.UTF8.GetByteCount(json));
                            _List = new List<T>(JsonSerializer.Deserialize<List<T>>(json));
                        }
                        else
                        {
                            _List = new List<T>();
                        }
                    }

                    scope?.SetItemCount(_List.Count);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }
        }

        private void RewritePersistenceFile()
        {
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceWrite, "PersistentList persistence.write", _FileName))
            {
                try
                {
                    lock (_PersistenceFileLock)
                    {
                        string dir = Path.GetDirectoryName(_PersistenceFile);
                        if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

                        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(_List);
                        File.WriteAllBytes(_PersistenceFile, bytes);

                        scope?.SetBytes(bytes.Length);
                        scope?.SetItemCount(_List.Count);
                    }
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }
        }

        private void ClearPersistenceFile()
        {
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceTruncate, "PersistentList persistence.truncate", _FileName))
            {
                try
                {
                    lock (_PersistenceFileLock)
                    {
                        using (StreamWriter writer = new StreamWriter(_PersistenceFile, false))
                        {
                            // write nothing
                        }
                    }
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }
        }

        #endregion
    }
}

