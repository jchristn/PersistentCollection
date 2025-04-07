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
                try
                {
                    _Semaphore.Wait();
                    return _Stack.Count;
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
                    if (_Stack != null) _Stack.Clear();
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

            try
            {
                _Semaphore.Wait();
                snapshot = new List<T>(_Stack);
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
        /// Clear the stack.
        /// </summary>
        public void Clear()
        {
            try
            {
                _Semaphore.Wait();
                _Stack.Clear();
                ClearPersistenceFile();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Check if the stack contains an item.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if exists.</returns>
        public bool Contains(T item)
        {
            try
            {
                _Semaphore.Wait();
                return _Stack.Contains(item);
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Peek at the top of the stack without removing from the stack.
        /// </summary>
        /// <returns>Item.</returns>
        public T Peek()
        {
            try
            {
                _Semaphore.Wait();
                T item = _Stack.Peek();
                return item;
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Pop an item from the stack.
        /// </summary>
        /// <returns>Item.</returns>
        public T Pop()
        {
            try
            {
                _Semaphore.Wait();
                T item = _Stack.Pop();
                RewritePersistenceFile();
                return item;
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Push an item to the stack.
        /// </summary>
        /// <param name="item">Item.</param>
        public void Push(T item)
        {
            try
            {
                _Semaphore.Wait();
                _Stack.Push(item);
                RewritePersistenceFile();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Create an array from the stack.
        /// </summary>
        /// <returns>Array.</returns>
        public T[] ToArray()
        {
            try
            {
                _Semaphore.Wait();
                return _Stack.ToArray();
            }
            finally
            {
                _Semaphore.Release();
            }
        }

        /// <summary>
        /// Copy queue contents to an array.
        /// </summary>
        /// <param name="array">Array.</param>
        /// <param name="arrayIndex">Array index.</param>
        public void CopyTo(T[] array, int arrayIndex)
        {
            try
            {
                _Semaphore.Wait();
                _Stack.CopyTo(array, arrayIndex);
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
                ((ICollection)_Stack).CopyTo(array, arrayIndex);
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
        /// Try to pop.
        /// </summary>
        /// <param name="item">Item.</param>
        /// <returns>True if successful.</returns>
        public bool TryPop(out T item)
        {
            try
            {
                item = Pop();
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
                if (_Stack == null) _Stack = new Stack<T>();
                else _Stack.Clear();

                if (File.Exists(_PersistenceFile))
                {
                    List<T> temp = JsonSerializer.Deserialize<List<T>>(File.ReadAllText(_PersistenceFile));
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
        }

        private void RewritePersistenceFile()
        {
            lock (_PersistenceFileLock)
            {
                string dir = Path.GetDirectoryName(_PersistenceFile);
                if (!String.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

                File.WriteAllText(_PersistenceFile, JsonSerializer.Serialize(_Stack));
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
