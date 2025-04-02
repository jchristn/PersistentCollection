![alt tag](https://github.com/jchristn/PersistentCollection/blob/main/src/PersistentCollections/Assets/icon.png?raw=true)

# PersistentCollection

[![NuGet Version](https://img.shields.io/nuget/v/PersistentCollection.svg?style=flat)](https://www.nuget.org/packages/PersistentCollection/) [![NuGet](https://img.shields.io/nuget/dt/PersistentCollection.svg)](https://www.nuget.org/packages/PersistentCollection) 

Lightweight, persistent, thread-safe, disk-based collection classes written in C# for queue, stack, and list.  All classes leverage a temporary directory for storage to enable persistence across instantiations of the object or restarts of the software.

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

PersistentList<string> myList = new PersistentList<string>("./temp"); // data directory
myList.Add("foo");
myList.Add("bar");
string val = myList.Get(1);
myList.RemoveAt(1);
```

### PersistentQueue

`PersistentQueue` mimics the behavior of `System.Collections.Generic.Queue<T>`.

```csharp
using PersistentCollection;

PersistentQueue<string> myQueue = new PersistentQueue<string>("./temp"); // data directory
myQueue.Enqueue("foo");
myQueue.Enqueue("bar");
string val = myQueue.Dequeue(); // foo
```

### PersistentStack

`PersistentStack` mimics the behavior of `System.Collections.Generic.Stack<T>`.

```csharp
using PersistentCollection;

PersistentStack<string> myStack = new PersistentStack<string>("./temp"); // data directory
myStack.Push("foo");
myStack.Push("bar");
string val = myStack.Pop(); // bar
```

## Version History

Refer to CHANGELOG.md for version history.
