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

    /// <summary>
    /// Persistent generic queue. Queued entries are backed on disk.
    /// Data is dequeued from the queue in a first-in-first-out manner.
    /// </summary>
    /// <typeparam name="T">Type of elements in the queue</typeparam>
    public class PersistentQueue<T> : IEnumerable<T>, IEnumerable, ICollection, IReadOnlyCollection<T>, IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Number of entries waiting in the queue.
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
                        return _Queue.Count;
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

        private const string _CollectionType = PersistentCollectionTelemetry.CollectionTypeQueue;

        private string _PersistenceFile = null;
        private string _FileName = null;
        private Queue<T> _Queue = new Queue<T>();

        private readonly object _PersistenceFileLock = new object();
        private SemaphoreSlim _Semaphore = new SemaphoreSlim(1, 1);

        private bool _Disposed = false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Persistent queue.
        /// </summary>
        /// <param name="persistenceFile">Persistence file.</param>
        public PersistentQueue(string persistenceFile = "./queue.idx")
        {
            if (String.IsNullOrEmpty(persistenceFile)) throw new ArgumentNullException(nameof(persistenceFile));

            _PersistenceFile = persistenceFile;
            _FileName = Path.GetFileName(persistenceFile);

            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "open", "PersistentQueue open"))
            {
                try
                {
                    if (File.Exists(_PersistenceFile)) PopulateFromFile();
                    scope?.SetItemCount(_Queue.Count);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }

            CollectionTelemetry.AddInstance(_CollectionType, 1);
            CollectionTelemetry.AddItems(_CollectionType, _Queue.Count);
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
                    if (_Queue != null)
                    {
                        CollectionTelemetry.AddItems(_CollectionType, -_Queue.Count);
                        _Queue.Clear();
                    }
                    _Semaphore?.Dispose();
                }

                _Queue = null;
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
            Queue<T> snapshot;

            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "enumerate", null))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    snapshot = new Queue<T>(_Queue);
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
        /// Copy the queue to an array.
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
                    _Queue.CopyTo(array, arrayIndex);
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
                    ((ICollection)_Queue).CopyTo(array, arrayIndex);
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
        /// Clear the queue.
        /// </summary>
        public void Clear()
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "clear", "PersistentQueue clear"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    CollectionTelemetry.AddItems(_CollectionType, -_Queue.Count);
                    _Queue.Clear();
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
        /// Check if the queue contains an item.
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
                    return _Queue.Contains(item);
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
        /// Dequeue an item from the queue.
        /// </summary>
        /// <returns>Item.</returns>
        public T Dequeue()
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "dequeue", "PersistentQueue dequeue"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    T item = _Queue.Dequeue();
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
        /// Enqueue an item to the stack.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <exception cref="NotImplementedException"></exception>
        public void Enqueue(T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "enqueue", "PersistentQueue enqueue"))
            {
                try
                {
                    _Semaphore.Wait();
                    scope?.LockAcquired();
                    _Queue.Enqueue(item);
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
        /// Peek at the next item to dequeue.
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
                    T item = _Queue.Peek();
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
        /// Create an array from the queue.
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
                    return _Queue.ToArray();
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

                    if (_Queue.Count < 1)
                    {
                        scope?.MarkMiss();
                        item = default(T);
                        return false;
                    }

                    item = _Queue.Peek();
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
        /// Try to dequeue.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if successful.</returns>
        public bool TryDequeue(out T item)
        {
            using (TelemetryScope scope = TelemetryScope.Operation(_CollectionType, "try_dequeue", "PersistentQueue try_dequeue"))
            {
                bool acquired = false;

                try
                {
                    _Semaphore.Wait();
                    acquired = true;
                    scope?.LockAcquired();

                    if (_Queue.Count < 1)
                    {
                        scope?.MarkMiss();
                        item = default(T);
                        return false;
                    }

                    item = _Queue.Dequeue();
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
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceRead, "PersistentQueue persistence.read", _FileName))
            {
                try
                {
                    lock (_PersistenceFileLock)
                    {
                        if (_Queue == null) _Queue = new Queue<T>();
                        else _Queue.Clear();

                        if (File.Exists(_PersistenceFile))
                        {
                            string json = File.ReadAllText(_PersistenceFile);
                            scope?.SetBytes(Encoding.UTF8.GetByteCount(json));
                            _Queue = new Queue<T>(JsonSerializer.Deserialize<List<T>>(json));
                        }
                        else
                        {
                            _Queue = new Queue<T>();
                        }
                    }

                    scope?.SetItemCount(_Queue.Count);
                }
                catch (Exception ex) when (TelemetryScope.Fail(scope, ex))
                {
                    throw;
                }
            }
        }

        private void RewritePersistenceFile()
        {
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceWrite, "PersistentQueue persistence.write", _FileName))
            {
                try
                {
                    lock (_PersistenceFileLock)
                    {
                        string dir = Path.GetDirectoryName(_PersistenceFile);
                        if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

                        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(_Queue);
                        File.WriteAllBytes(_PersistenceFile, bytes);

                        scope?.SetBytes(bytes.Length);
                        scope?.SetItemCount(_Queue.Count);
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
            using (TelemetryScope scope = TelemetryScope.Persistence(_CollectionType, PersistentCollectionTelemetry.PersistenceTruncate, "PersistentQueue persistence.truncate", _FileName))
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
