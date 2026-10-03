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
    /// Persistent dictionary. Dictionary entries are backed on disk.
    /// Provides key-based access to data items.
    /// </summary>
    public class PersistentDictionary<TKey, TValue> : IDictionary<TKey, TValue>, IDisposable
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
        /// Number of entries in the dictionary.
        /// </summary>
        public int Count => ((ICollection<KeyValuePair<TKey, TValue>>)this).Count;

        /// <summary>
        /// Boolean indicating if the dictionary is read-only. This will always be false.
        /// </summary>
        public bool IsReadOnly => ((ICollection<KeyValuePair<TKey, TValue>>)this).IsReadOnly;

        /// <summary>
        /// Collection of keys.
        /// </summary>
        public ICollection<TKey> Keys => ((IDictionary<TKey, TValue>)this).Keys;

        /// <summary>
        /// Collection of values.
        /// </summary>
        public ICollection<TValue> Values => ((IDictionary<TKey, TValue>)this).Values;

        /// <summary>
        /// Retrieve by key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>Value associated with key.</returns>
        public TValue this[TKey key]
        {
            get => ((IDictionary<TKey, TValue>)this)[key];
            set => ((IDictionary<TKey, TValue>)this)[key] = value;
        }

        /// <summary>
        /// Add a key-value pair.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="value">Value.</param>
        public void Add(TKey key, TValue value) => ((IDictionary<TKey, TValue>)this).Add(key, value);

        /// <summary>
        /// Add a key-value pair.
        /// </summary>
        /// <param name="item">Key-value pair.</param>
        public void Add(KeyValuePair<TKey, TValue> item) => ((ICollection<KeyValuePair<TKey, TValue>>)this).Add(item);

        /// <summary>
        /// Clear the dictionary.
        /// </summary>
        public void Clear() => ((ICollection<KeyValuePair<TKey, TValue>>)this).Clear();

        /// <summary>
        /// Check if the dictionary contains a key-value pair.
        /// </summary>
        /// <param name="item">Key-value pair.</param>
        /// <returns>True if exists.</returns>
        public bool Contains(KeyValuePair<TKey, TValue> item) => ((ICollection<KeyValuePair<TKey, TValue>>)this).Contains(item);

        /// <summary>
        /// Check if the dictionary contains a key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>True if exists.</returns>
        public bool ContainsKey(TKey key) => ((IDictionary<TKey, TValue>)this).ContainsKey(key);

        /// <summary>
        /// Copy the dictionary to an array.
        /// </summary>
        /// <param name="array">Array.</param>
        /// <param name="arrayIndex">Array index.</param>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) => ((ICollection<KeyValuePair<TKey, TValue>>)this).CopyTo(array, arrayIndex);

        /// <summary>
        /// Remove a key-value pair.
        /// </summary>
        /// <param name="item">Key-value pair.</param>
        /// <returns>True if removed.</returns>
        public bool Remove(KeyValuePair<TKey, TValue> item) => ((ICollection<KeyValuePair<TKey, TValue>>)this).Remove(item);

        /// <summary>
        /// Remove a key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>True if removed.</returns>
        public bool Remove(TKey key) => ((IDictionary<TKey, TValue>)this).Remove(key);

        /// <summary>
        /// Try to get a value by key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="value">Value.</param>
        /// <returns>True if successful.</returns>
        public bool TryGetValue(TKey key, out TValue value) => ((IDictionary<TKey, TValue>)this).TryGetValue(key, out value);

        /// <summary>
        /// Get enumerator.
        /// </summary>
        /// <returns>Enumerator.</returns>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => ((IEnumerable<KeyValuePair<TKey, TValue>>)this).GetEnumerator();

        #endregion

        #region Private-Members

        private const string _CollectionType = PersistentCollectionTelemetry.CollectionTypeDictionary;

        private string _PersistenceFile = null;
        private string _FileName = null;
        private Dictionary<TKey, TValue> _Dictionary = new Dictionary<TKey, TValue>();

        private readonly object _PersistenceFileLock = new object();
        private SemaphoreSlim _Semaphore = new SemaphoreSlim(1, 1);

        private bool _Disposed = false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Persistent dictionary.
        /// </summary>
        /// <param name="persistenceFile">Persistence file.</param>
        public PersistentDictionary(string persistenceFile = "./dictionary.idx")
        {
            if (String.IsNullOrEmpty(persistenceFile)) throw new ArgumentNullException(nameof(persistenceFile));

            _PersistenceFile = persistenceFile;
            _FileName = Path.GetFileName(persistenceFile);

            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "open", "PersistentDictionary open"))
            {
                try
                {
                    if (File.Exists(_PersistenceFile)) PopulateFromFile();
                    scope?.SetItemCount(_Dictionary.Count);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }

            CollectionTelemetry.AddInstance(_CollectionType, 1);
            CollectionTelemetry.AddItems(_CollectionType, _Dictionary.Count);
        }

        #endregion

        #region IDictionary-Interface-Implementation

        int ICollection<KeyValuePair<TKey, TValue>>.Count
        {
            get
            {
                using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "count", null))
                {
                    try
                    {
                        _Semaphore.Wait();
                        scope?.LockAcquired();
                        return _Dictionary.Count;
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

        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly
        {
            get
            {
                return false;
            }
        }

        ICollection<TKey> IDictionary<TKey, TValue>.Keys
        {
            get
            {
                using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "keys", null))
                {
                    try
                    {
                        _Semaphore.Wait();
                        scope?.LockAcquired();
                        return _Dictionary.Keys.ToList();
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

        ICollection<TValue> IDictionary<TKey, TValue>.Values
        {
            get
            {
                using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "values", null))
                {
                    try
                    {
                        _Semaphore.Wait();
                        scope?.LockAcquired();
                        return _Dictionary.Values.ToList();
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

        TValue IDictionary<TKey, TValue>.this[TKey key]
        {
            get
            {
                using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "get", null))
                {
                    try
                    {
                        _Semaphore.Wait();
                        scope?.LockAcquired();
                        return _Dictionary[key];
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
                using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "set", "PersistentDictionary set"))
                {
                    try
                    {
                        _Semaphore.Wait();
                        scope?.LockAcquired();
                        bool existed = _Dictionary.ContainsKey(key);
                        _Dictionary[key] = value;
                        if (!existed) CollectionTelemetry.AddItems(_CollectionType, 1);
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

        void IDictionary<TKey, TValue>.Add(TKey key, TValue value)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "add", "PersistentDictionary add"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    _Dictionary.Add(key, value);
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

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "add", "PersistentDictionary add"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    _Dictionary.Add(item.Key, item.Value);
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

        void ICollection<KeyValuePair<TKey, TValue>>.Clear()
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "clear", "PersistentDictionary clear"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    CollectionTelemetry.AddItems(_CollectionType, -_Dictionary.Count);
                    _Dictionary.Clear();
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

        bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "contains", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    return _Dictionary.ContainsKey(item.Key) &&
                           EqualityComparer<TValue>.Default.Equals(_Dictionary[item.Key], item.Value);
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

        bool IDictionary<TKey, TValue>.ContainsKey(TKey key)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "contains_key", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    return _Dictionary.ContainsKey(key);
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

        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "copy_to", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    foreach (KeyValuePair<TKey, TValue> kvp in _Dictionary)
                    {
                        array[arrayIndex++] = kvp;
                    }
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

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "remove", "PersistentDictionary remove"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();

                    if (!_Dictionary.TryGetValue(item.Key, out TValue value) ||
                        !EqualityComparer<TValue>.Default.Equals(value, item.Value))
                    {
                        scope?.MarkMiss();
                        return false;
                    }

                    bool result = _Dictionary.Remove(item.Key);
                    if (result)
                    {
                        CollectionTelemetry.AddItems(_CollectionType, -1);
                        RewritePersistenceFile();
                    }
                    else
                    {
                        scope?.MarkMiss();
                    }

                    return result;
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

        bool IDictionary<TKey, TValue>.Remove(TKey key)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "remove", "PersistentDictionary remove"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    bool result = _Dictionary.Remove(key);
                    if (result)
                    {
                        CollectionTelemetry.AddItems(_CollectionType, -1);
                        RewritePersistenceFile();
                    }
                    else
                    {
                        scope?.MarkMiss();
                    }

                    return result;
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

        bool IDictionary<TKey, TValue>.TryGetValue(TKey key, out TValue value)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "try_get_value", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    bool found = _Dictionary.TryGetValue(key, out value);
                    if (!found) scope?.MarkMiss();
                    return found;
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

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            Dictionary<TKey, TValue> snapshot;

            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "enumerate", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    snapshot = new Dictionary<TKey, TValue>(_Dictionary);
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
            return ((IEnumerable<KeyValuePair<TKey, TValue>>)this).GetEnumerator();
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
                    if (_Dictionary != null)
                    {
                        CollectionTelemetry.AddItems(_CollectionType, -_Dictionary.Count);
                        _Dictionary.Clear();
                    }
                    _Semaphore?.Dispose();
                }

                _Dictionary = null;
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
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceRead, "PersistentDictionary persistence.read", _FileName))
            {
                try
                {
                    lock (_PersistenceFileLock)
                    {
                        if (_Dictionary == null) _Dictionary = new Dictionary<TKey, TValue>();
                        else _Dictionary.Clear();

                        if (File.Exists(_PersistenceFile))
                        {
                            string json = File.ReadAllText(_PersistenceFile);
                            scope?.SetBytes(Encoding.UTF8.GetByteCount(json));
                            _Dictionary = new Dictionary<TKey, TValue>(JsonSerializer.Deserialize<Dictionary<TKey, TValue>>(json));
                        }
                        else
                        {
                            _Dictionary = new Dictionary<TKey, TValue>();
                        }
                    }

                    scope?.SetItemCount(_Dictionary.Count);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }
        }

        private void RewritePersistenceFile()
        {
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceWrite, "PersistentDictionary persistence.write", _FileName))
            {
                try
                {
                    lock (_PersistenceFileLock)
                    {
                        string dir = Path.GetDirectoryName(_PersistenceFile);
                        if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

                        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(_Dictionary);
                        File.WriteAllBytes(_PersistenceFile, bytes);

                        scope?.SetBytes(bytes.Length);
                        scope?.SetItemCount(_Dictionary.Count);
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
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceTruncate, "PersistentDictionary persistence.truncate", _FileName))
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