# Shielded.Gossip

Shielded.Gossip is a distributed key/value store library for .NET Standard with
support for eventually and strongly consistent transactions. It is based on the
[Shielded STM library](https://github.com/jbakic/Shielded) and uses a gossip
protocol for synchronizing between servers.

*The library is still a work in progress. I would be grateful if you report any
issues you encounter while using it.*

It can be used to implement a distributed cache, or any kind of distributed
database. It does not provide storage - it is meant to be combined with an external
system for persistency, like a traditional relational DB.

## Basics

The central class is the GossipBackend. You need to provide it with a message
transport implementation and some configuration. The library has a basic transport
using TCP, in the class TcpTransport, but by implementing ITransport you can use
any communication means you want. It does not require strong guarantees from the
transport. It is enough that a majority of messages make it through in a reasonable
amount of time.

```csharp
var transport = new TcpTransport("Server1",
    new Dictionary<string, IPEndPoint>
    {
        { "Server1", new IPEndPoint(IPAddress.Parse("10.0.0.1"), 2001) },
        { "Server2", new IPEndPoint(IPAddress.Parse("10.0.0.2"), 2001) },
        { "Server3", new IPEndPoint(IPAddress.Parse("10.0.0.3"), 2001) },
    }));
transport.StartListening();
var backend = new GossipBackend(transport, new GossipConfiguration());
```

This gives you a running backend with ID Server1, which will communicate with the
two other servers listed above.

The backend is a key/value store. Keys are strings, and values can be any type which
implements the IMergeable&lt;T&gt; interface, which is needed for resolving conflicts.
You can implement it yourself, or use one of the included wrapper types which
implement it. E.g. if you use an external database and have optimistic concurrency
checks based on an integer column, use the IntVersioned&lt;T&gt; wrapper, which
resolves conflicts by simply picking the value with a higher version. There's also
the Lww&lt;T&gt; wrapper, which pairs a value with the time it was written, and
resolves conflicts using last-write-wins semantics. And the safest option is the
combination of VecVersioned and Multiple wrappers, which add Version Vectors to your
types to make them full-blown CRDTs (more on that below).

Getting and setting values is simple. If you have a type called SomeEntity, and this type
has an int Version property:

```csharp
// the result has properties Value of type SomeEntity, and an int Version.
var wrappedVal = backend.TryGetIntVersioned<SomeEntity>("the key");
// ...
var newVal = new SomeEntity { Id = 1234, Name = "name", Version = 2 };
// Version is an extension method which wraps the SomeEntity into an IntVersioned wrapper
backend.Set("the key", newVal.Version(newVal.Version));
```

Distributing the new data to other servers is done asynchronously, in the background,
and does not block.

The backend supports transactions as well, which are done using ordinary Shielded library
transactions.

```csharp
Shield.InTransaction(() =>
{
    backend.Set("key1", val1);
    backend.Set("key2", val2);
});
```

Such transactions are only locally consistent - you will read the latest consistent
snapshot of the data available locally, and your transaction will be retried if another
thread on the same machine is in conflict with you. The transaction will also be
distributed to other servers as a single package, so no other server will see key2 set
to val2 wthout also seeing the write you made to key1, but that is the only guarantee.
E.g. any previous transaction you did locally might not yet be visible to other servers
even if they can see the effects of this one.

## CRDTs and Version Vectors

CRDTs ([Conflict-free Replicated Data Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type))
are the ideal type of value to use with the eventually consistent backend. They are
mergable in an idempotent, commutative and associative way, which means that the servers
must eventually agree on the same version of the data, regardless of the order or number
of messages they exchange. Currently included are the VersionVector, and a distributed
counter implemented in the CountVector class. VectorBase can be used as a base class to
easily implement vector clock-like CRDT types.

To achieve the same desirable characteristics with a type which is not a CRDT, you can
add the IHasVersionVector interface to it, and use the Multiple&lt;T&gt; wrapper to make
it a CRDT. If you cannot change the type to add the interface to it, you can use the
VecVersioned&lt;T&gt; wrapper which pairs a value of any type with a version vector and
which implements that interface.

[Version vectors](https://en.wikipedia.org/wiki/Version_vector) reliably detect
conflicting edits to the same key, and the Multiple will keep the conflicting versions,
allowing you to resolve conflicts using any strategy that works best for your use case.
Thus, version vectors will never lose any writes even if used in a system that is just
eventually consistent, unlike the simpler IntVersioned and Lww wrappers.

## Removing Keys

As you may already know, this is a tricky subject. Removing data from a gossip based
key/value store generally requires maintaining so-called tombstones. Those are records
of removal, which need to be kept forever, because if a server has been disconnected
from the others for a long time, and the others removed a key, he can revive it when he
reconnects.

The library does support removal, but since it has no persistence, it maintains
tombstones only for one minute (configurable), just to communicate the removal to other
servers.

A key can be removed by using the method Remove:

```csharp
backend.Remove("some key");
```

Another option is to implement the IDeletable interface on a type, and when its
CanDelete property becomes true, it will be deleted. CanDelete should only depend on
the state of the object and nothing else.

What happens if a removed key gets revived later is up to you. If you're handling
KeyMissing events, you can restore the tombstone from an external storage when needed.
Or you can react to Changed events and, when a new item is added which should not be
there, simply remove it again.

## Expiry

All Set method variants of both backends accept an expiry parameter. For example, to
expire a key after 5 seconds:

```csharp
backend.Set("some key", someMergeable, 5000);
```

By writing the same value again with only a different expiry you can extend it, e.g.
if you wish to have sliding expiry. It is also possible to activate it for an already
existing value. But it is not possible to reduce it.

It is important to note that, to avoid issues with server clock synchronization, the
backends do not communicate about the exact time when an item must expire. Rather, they
send each other the information on how many milliseconds a key has left before it
expires. Thus, expiry is not precise.

## The Consistent Backend

If you need strongly consistent transactions as well, use the ConsistentGossipBackend.
If used in ordinary Shielded transactions, it is only eventually consistent, just like
the GossipBackend. But if you use its RunConsistent method, the transaction will be checked
and will succeed only if a majority of servers agree that it was indeed consistent.

```csharp
var consistentBackend = new ConsistentGossipBackend(someTransport, new GossipConfiguration());
// ...
var (success, withdrawn) = await consistentBackend.RunConsistent(() =>
{
    var accountState = consistentBackend.TryGetIntVersioned<AccountState>("account-1234");
    if (accountState.Value == null || accountState.Value.Balance <= 0m)
        return 0m;
    var balance = accountState.Value.Balance;
    var newState = accountState.NextVersion();
    newState.Value.Balance = 0m;
    consistentBackend.Set("account-1234", newState);
    return balance;
});
```

The code above will take all the money from a hypothetical bank account, if the account has
any funds. If another server is trying to perform the same operation, only one of them will
succeed. The variable success will be set to true on both, but one will get withdrawn == 0m.
Success will only be false if we fail to achieve agreement with other servers after a number of
attempts. How many attempts to make can be controlled by an extra argument to RunConsistent,
by default it is 10.

If you wish to synchronize the transactions with an external system, the method Prepare should
be used - it prepares the transaction, and if successful, returns a continuation which you can
use to commit or roll back the transaction later. As long as the continuation is not completed,
the system will block any other consistent transactions accessing the same fields as yours.

*The library needs more work in this regard - there's currently no event raised when a transaction
is received from another server. Presumably, a server who did not initiate a transaction might
also want to do some external work in sync with that transaction, so I plan to add some events
like TransactionPreparing, TransactionCommitting, TransactionFailing... for this purpose.
Also missing is the ability to wait for a number of servers to fully confirm the transaction,
currently you'll get a result as soon as possible, as soon as the transaction has been checked
by enough servers.*

The consistent transactions are implemented using the ordinary, eventually consistent gossip
backend. The transaction state is stored in a CRDT type, and the servers react to changes to it
by making appropriate state transitions, in effect performing a simple 2-phase commit protocol.

The transaction metadata carries information about all the read and written fields, and will
insist that all the fields you read were up to date, and that all your writes have versions
that are strictly greater than the versions on other servers. It is enough for a majority of
servers to confirm (or reject) a transaction, so the system should work in case of partitions,
as long as you're in the partition that contains more than half of the servers.

You should avoid using the same fields from both consistent and non-consistent transaction,
but FYI, if you access the same fields from non-consistent transactions, the non-consistent
transactions will proceed uninterrupted. Consistent transactions can only block each other.
This is maybe weird, but it guarantees that non-consistent ops never block, which is very
useful.

## On Serializing

The library uses the DataContractSerializer, simply to avoid introducing any dependencies.
You will probably want to replace this with a better, binary serializer. Just call the
method Serializer.Use, and pass it an implementation of ISerializer, which is a minimal
interface for a serializer that is capable of serializing and deserializing objects without
being told in advance what type it's dealing with.

*Please note that "a better, binary serializer" certainly does not mean the BinaryFormatter
from the .NET Framework! It uses reflection and it is slow.*

## The Gossip Protocol

The library is based on the article "Epidemic Algorithms for Replicated Database Maintenance" by
Demers et al., 1987. It employs only the direct mail and anti-entropy messages, but the
anti-entropy exchange is incremental, and uses a reverse time index of all updates to the
database, which makes it in effect similar to the "hot rumor" approach, but without the risk
of deactivating a rumor before it gets transmitted to all servers. The article is a great read,
and it explains all of this, accompanied with the results of many simulations and experiments.

Direct mail is launched whenever a write is committed locally and is sent to all known servers
using the ITransport.Broadcast method. Gossip anti-entropy exchanges are done randomly - every
5 seconds (configurable) a server randomly picks a gossip partner, and if they're not already
in a gossip exchange, starts one. Direct mail will by default not be sent to a server who we're
in a gossip exchange with right now, but this is configurable. The backends can also be
configured to start gossip exchanges on every write instead of sending direct mail, or the
direct mail can simply be turned off.

The anti-entropy exchange begins by sending a small package of last changes on the server
(default is 10 key/value pairs, but will vary because we send only whole transactions), and the
hash of the entire database. Upon receiving a gossip message, a server first applies the
changes from the message, merging them with local data. Then it compares the hashes, and if they
match, it replies with an end message. As long as they do not match, the servers will continue
sending replies to each other, transmitting changes to their local DB starting from the most
recent ones and moving back. Every further reply doubles in size, until they start hitting
against the configurable cut-off limit, but NB that the cut-off will be exceeded if you commit
any transactions whose number of affected fields is greater than the cut-off - the backends
only transmit entire transactions. If the servers have not lost connectivity recently, then
they will terminate quickly. If not, they will continue until they are both in sync.

The database hash is a simple XOR of hashes of individual keys and the versions of their values.
This enables it to be incrementaly updated, with no need to iterate over all the keys.
