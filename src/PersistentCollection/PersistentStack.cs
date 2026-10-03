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
    using System.Threading.Tasks;

    /// <summary>
    /// Persistent stack.  Queued entries are backed on disk.
    /// Data is popped from the stack in a last-in-first-out manner.
    /// </summary>
    public class PersistentStack<T> : IEnumerable<T>, IEnumerable, ICollection, IReadOnlyCollection<T>, IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Number of entries waiting in the stack.
        /// </summary>
        public int Count
        {
            get
            {
                using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "count", null))
                {
                    try
                    {
                        _Semaphore.Wait();
                        scope?.LockAcquired();
                        return _Stack.Count;
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

        #endregion

        #region Private-Members

        private const string _CollectionType = PersistentCollectionTelemetry.CollectionTypeStack;

        private string _PersistenceFile = null;
        private string _FileName = null;
        private Stack<T> _Stack = new Stack<T>();

        private readonly object _PersistenceFileLock = new object();
        private SemaphoreSlim _Semaphore = new SemaphoreSlim(1, 1);

        private bool _Disposed = false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Persistent stack.
        /// </summary>
        /// <param name="persistenceFile">Persistence file.</param>
        public PersistentStack(string persistenceFile = "./stack.idx")
        {
            if (String.IsNullOrEmpty(persistenceFile)) throw new ArgumentNullException(nameof(persistenceFile));

            _PersistenceFile = persistenceFile;
            _FileName = Path.GetFileName(persistenceFile);

            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "open", "PersistentStack open"))
            {
                try
                {
                    if (File.Exists(_PersistenceFile)) PopulateFromFile();
                    scope?.SetItemCount(_Stack.Count);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }

            CollectionTelemetry.AddInstance(_CollectionType, 1);
            CollectionTelemetry.AddItems(_CollectionType, _Stack.Count);
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
                    if (_Stack != null)
                    {
                        CollectionTelemetry.AddItems(_CollectionType, -_Stack.Count);
                        _Stack.Clear();
                    }
                    _Semaphore?.Dispose();
                }

                _Stack = null;
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

        /// <summary>
        /// Get enumerator.
        /// </summary>
        /// <returns>Enumerator.</returns>
        public IEnumerator<T> GetEnumerator()
        {
            List<T> snapshot;

            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "enumerate", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    snapshot = new List<T>(_Stack);
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

        /// <summary>
        /// Get enumerator.
        /// </summary>
        /// <returns>Enumerator.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Clear the stack.
        /// </summary>
        public void Clear()
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "clear", "PersistentStack clear"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    CollectionTelemetry.AddItems(_CollectionType, -_Stack.Count);
                    _Stack.Clear();
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

        /// <summary>
        /// Check if the stack contains an item.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if exists.</returns>
        public bool Contains(T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "contains", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    return _Stack.Contains(item);
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

        /// <summary>
        /// Peek at the top of the stack without removing from the stack.
        /// </summary>
        /// <returns>Item.</returns>
        public T Peek()
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "peek", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    T item = _Stack.Peek();
                    return item;
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

        /// <summary>
        /// Pop an item from the stack.
        /// </summary>
        /// <returns>Item.</returns>
        public T Pop()
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "pop", "PersistentStack pop"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    T item = _Stack.Pop();
                    CollectionTelemetry.AddItems(_CollectionType, -1);
                    RewritePersistenceFile();
                    return item;
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

        /// <summary>
        /// Push an item to the stack.
        /// </summary>
        /// <param name="item">Item.</param>
        public void Push(T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "push", "PersistentStack push"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    _Stack.Push(item);
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

        /// <summary>
        /// Create an array from the stack.
        /// </summary>
        /// <returns>Array.</returns>
        public T[] ToArray()
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "to_array", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    return _Stack.ToArray();
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

        /// <summary>
        /// Copy queue contents to an array.
        /// </summary>
        /// <param name="array">Array.</param>
        /// <param name="arrayIndex">Array index.</param>
        public void CopyTo(T[] array, int arrayIndex)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "copy_to", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    _Stack.CopyTo(array, arrayIndex);
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

        /// <summary>
        /// Copy the queue to an array.
        /// </summary>
        /// <param name="array">Array.</param>
        /// <param name="arrayIndex">Array index.</param>
        public void CopyTo(Array array, int arrayIndex)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "copy_to", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    ((ICollection)_Stack).CopyTo(array, arrayIndex);
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

        /// <summary>
        /// Try to peek.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if successful.</returns>
        public bool TryPeek(out T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "try_peek", null))
            {
                bool acquired = false;

                try
                {
                    _Semaphore.Wait();
                    acquired = true;
                    scope?.LockAcquired();

                    if (_Stack.Count < 1)
                    {
                        scope?.MarkMiss();
                        item = default(T);
                        return false;
                    }

                    item = _Stack.Peek();
                    return true;
                }
                catch (Exception ex)
                {
                    scope?.MarkFailed(ex);
                    item = default(T);
                    return false;
                }
                finally
                {
                    if (acquired) _Semaphore.Release();
                }
            }
        }

        /// <summary>
        /// Try to pop.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if successful.</returns>
        public bool TryPop(out T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "try_pop", "PersistentStack try_pop"))
            {
                bool acquired = false;

                try
                {
                    _Semaphore.Wait();
                    acquired = true;
                    scope?.LockAcquired();

                    if (_Stack.Count < 1)
                    {
                        scope?.MarkMiss();
                        item = default(T);
                        return false;
                    }

                    item = _Stack.Pop();
                    CollectionTelemetry.AddItems(_CollectionType, -1);
                    RewritePersistenceFile();
                    return true;
                }
                catch (Exception ex)
                {
                    scope?.MarkFailed(ex);
                    item = default(T);
                    return false;
                }
                finally
                {
                    if (acquired) _Semaphore.Release();
                }
            }
        }

        #endregion

        #region Private-Methods

        private void PopulateFromFile()
        {
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceRead, "PersistentStack persistence.read", _FileName))
            {
                try
                {
                    lock (_PersistenceFileLock)
                    {
                        if (_Stack == null) _Stack = new Stack<T>();
                        else _Stack.Clear();

                        if (File.Exists(_PersistenceFile))
                        {
                            string json = File.ReadAllText(_PersistenceFile);
                            scope?.SetBytes(Encoding.UTF8.GetByteCount(json));
                            List<T> temp = JsonSerializer.Deserialize<List<T>>(json);
                            if (temp != null && temp.Count > 0)
                            {
                                temp.Reverse();
                                foreach (T item in temp) _Stack.Push(item);
                            }
                        }
                        else
                        {
                            _Stack = new Stack<T>();
                        }
                    }

                    scope?.SetItemCount(_Stack.Count);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }
        }

        private void RewritePersistenceFile()
        {
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceWrite, "PersistentStack persistence.write", _FileName))
            {
                try
                {
                    lock (_PersistenceFileLock)
                    {
                        string dir = Path.GetDirectoryName(_PersistenceFile);
                        if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

                        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(_Stack);
                        File.WriteAllBytes(_PersistenceFile, bytes);

                        scope?.SetBytes(bytes.Length);
                        scope?.SetItemCount(_Stack.Count);
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
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceTruncate, "PersistentStack persistence.truncate", _FileName))
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
