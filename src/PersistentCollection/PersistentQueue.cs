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
                try
                {
                    _Semaphore.Wait();
                    return _Queue.Count;
                }
                finally
                {
                    _Semaphore.Release();
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

        private string _PersistenceFile = null;
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

            if (File.Exists(_PersistenceFile)) PopulateFromFile();
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
                    if (_Queue != null) _Queue.Clear();
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

            try
            {
                _Semaphore.Wait();
                snapshot = new Queue<T>(_Queue);
            }
            finally
            {
                _Semaphore.Release();
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
            try
            {
                _Semaphore.Wait();
                _Queue.CopyTo(array, arrayIndex);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Copy the queue to an array.
        /// </summary>
        /// <param name="array">Array.</param>
        /// <param name="arrayIndex">Array index.</param>
        public void CopyTo(Array array, int arrayIndex)
        {
            try
            {
                _Semaphore.Wait();
                ((ICollection)_Queue).CopyTo(array, arrayIndex);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Clear the queue.
        /// </summary>
        public void Clear()
        {
            try
            {
                _Semaphore.Wait();
                _Queue.Clear();
                ClearPersistenceFile();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Check if the queue contains an item.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if exists.</returns>
        public bool Contains(T item)
        {
            try
            {
                _Semaphore.Wait();
                return _Queue.Contains(item);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Dequeue an item from the queue.
        /// </summary>
        /// <returns>Item.</returns>
        public T Dequeue()
        {
            try
            {
                _Semaphore.Wait();
                T item = _Queue.Dequeue();
                RewritePersistenceFile();
                return item;
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Enqueue an item to the stack.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <exception cref="NotImplementedException"></exception>
        public void Enqueue(T item)
        {
            try
            {
                _Semaphore.Wait();
                _Queue.Enqueue(item);
                RewritePersistenceFile();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Peek at the next item to dequeue.
        /// </summary>
        /// <returns>Item.</returns>
        public T Peek()
        {
            try
            {
                _Semaphore.Wait();
                T item = _Queue.Peek();
                return item;
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Create an array from the queue.
        /// </summary>
        /// <returns>Array.</returns>
        public T[] ToArray()
        {
            try
            {
                _Semaphore.Wait();
                return _Queue.ToArray();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Try to peek.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if successful.</returns>
        public bool TryPeek(out T item)
        {
            try
            {
                item = Peek();
                return true;
            }
            catch (Exception)
            {
                item = default(T);
                return false;
            }
        }

        /// <summary>
        /// Try to dequeue.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if successful.</returns>
        public bool TryDequeue(out T item)
        {
            try
            {
                item = Dequeue();
                return true;
            }
            catch (Exception)
            {
                item = default(T);
                return false;
            }
        }

        #endregion

        #region Private-Methods

        private void PopulateFromFile()
        {
            lock (_PersistenceFileLock)
            {
                if (_Queue == null) _Queue = new Queue<T>();
                else _Queue.Clear();

                if (File.Exists(_PersistenceFile))
                {
                    _Queue = new Queue<T>(JsonSerializer.Deserialize<List<T>>(File.ReadAllText(_PersistenceFile)));
                }
                else
                {
                    _Queue = new Queue<T>();
                }
            }
        }

        private void RewritePersistenceFile()
        {
            lock (_PersistenceFileLock)
            {
                string dir = Path.GetDirectoryName(_PersistenceFile);
                if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

                File.WriteAllText(_PersistenceFile, JsonSerializer.Serialize(_Queue));
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
