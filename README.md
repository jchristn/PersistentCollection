![alt tag](https://github.com/jchristn/PersistentCollection/blob/main/src/PersistentCollection/Assets/icon.png?raw=true)

# PersistentCollection

[![NuGet Version](https://img.shields.io/nuget/v/PersistentCollection.svg?style=flat)](https://www.nuget.org/packages/PersistentCollection/) [![NuGet](https://img.shields.io/nuget/dt/PersistentCollection.svg)](https://www.nuget.org/packages/PersistentCollection) 

Lightweight, persistent, thread-safe, disk-based collection classes written in C# for queue, stack, and list.  All classes leverage a temporary directory for storage to enable persistence across instantiations of the object or restarts of the software.

**IMPORTANT**:
- To provide persistence, the internal data structure is persisted to disk *in full* any time a change is made
- Thus, this library is NOT APPROPRIATE for large amounts of data or a large number of records
- It is recommended that these classes be used sparingly, when record counts are less than 1000 and total size is less than 10MB

## New in v2.0.x

- Remove expiration
- Migrate existing implementation to `PersistentList`, `PersistentQueue`, and `PersistentStack`
- Rename package to `PersistentCollection`

## Getting Started

Refer to the ```Test``` project for a working example.

### PersistentList

`PersistentList` mimics the behavior of `System.Collections.Generic.List<T>`.

```csharp
using PersistentCollection;

PersistentList<string> myList = new PersistentList<string>("./list.idx"); 
myList.Add("foo");
myList.Add("bar");
string val = myList.Get(1);
myList.RemoveAt(1);
```

### PersistentQueue

`PersistentQueue` mimics the behavior of `System.Collections.Generic.Queue<T>`.

```csharp
using PersistentCollection;

PersistentQueue<string> myQueue = new PersistentQueue<string>("./queue.idx");
myQueue.Enqueue("foo");
myQueue.Enqueue("bar");
string val = myQueue.Dequeue(); // foo
```

### PersistentStack

`PersistentStack` mimics the behavior of `System.Collections.Generic.Stack<T>`.

```csharp
using PersistentCollection;

PersistentStack<string> myStack = new PersistentStack<string>("./stack.idx");
myStack.Push("foo");
myStack.Push("bar");
string val = myStack.Pop(); // bar
```

## Version History

Refer to CHANGELOG.md for version history.
