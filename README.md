# Shielded.Gossip

Shielded.Gossip is a distributed key/value store library for .NET Standard with
support for eventually and strongly consistent transactions. It is based on the
[Shielded STM library](https://github.com/jbakic/Shielded) and uses a gossip
protocol for synchronizing between servers.

*The library is still a work in progress, but should be ready for use. I would be
grateful if you report any issues you encounter while using it.*

It can be used to easily implement a distributed cache, or any kind of distributed
database. It does not provide storage, it is meant to be combined with an external
system for persistency, like a traditional relational DB. If the external storage
supports consistent transactions, you will only need the eventually consistent
features of this library.

## Basics

The central class is the GossipBackend. You need to provide it with a message
transport implementation and some configuration. The library has a basic transport
using TCP, in the class TcpTransport, but by implementing ITransport you can use
any communication means you want. It does not require any strong guarantees from the
transport, since gossip-based communication is very resilient. It is enough that
a majority of messages will make it through in some reasonable amount of time.

```csharp
var transport = new TcpTransport("Server1", new IPEndPoint(IPAddress.Parse("10.0.0.1"), 2001),
    // using ShieldedDict allows us to later safely change transport.Servers if needed, but it's not mandatory.
    Shield.InTransaction(() => new ShieldedDict<string, IPEndPoint>
    {
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
It is ideal to use the library with CRDTs -
[Conflict-free Replicated Data Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type),
specifically the state-based convergent variant. Such types have a merge operation which
is idempotent, commutative and associative, which basically means that different servers
will eventually reach agreement on the correct value regardless of the number of conflicting
edits, or the number and order of messages they exchange.

However, you do not need to implement or use CRDTs. The library has wrapper types which
implement the IMergeable interface, and you can choose the appropriate one based on the
required semantics. E.g. if you use an external database and have optimistic concurrency
checks based on an integer column, you can use the simple IntVersioned&lt;T&gt; wrapper,
which resolves conflicts by simply picking the value with a higher version. There's also
the Lww&lt;T&gt; wrapper, which pairs a value with the time it was written, and resolves
conflicts using last-write-wins semantics, and perhaps the strongest is the combination
of VecVersioned and Multiple wrappers, which add Version Vectors to your types to make
them proper CRDTs.

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
and does not block the backend in any way. If other servers changed the same key at the
same time, those changes will be merged with yours eventually.

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
E.g. any previous transaction you did locally might not yet be visible to other servers even
if they can see the effects of this one. And most importantly, no changes on other servers
will conflict with yours. They will just eventually be merged together.

## CRDTs and Version Vectors

As mentioned previously, CRDTs are the ideal type of value to use with the eventually consistent
backend. They are mergable in an idempotent, commutative and associative way, which means that
the servers must eventually agree on the same version of the data, regardless of the order or
number of messages they exchange. Currently included are the VersionVector, and a distributed
counter implemented in the CountVector class. VectorBase can be used as a base class to easily
implement vector clock-like CRDT types.

To achieve the same desirable characteristics with a type which is not a CRDT, you can simply
add the IHasVersionVector interface to it, and use the Multiple&lt;T&gt; wrapper to make it a
CRDT. If you cannot change the type to add the interface to it, you can use the
VecVersioned&lt;T&gt; wrapper which pairs a value of any type with a version vector and which
implements that interface.

[Version vectors](https://en.wikipedia.org/wiki/Version_vector) reliably detect conflicting edits
to the same key, and the Multiple will keep the conflicting versions, allowing you to resolve
conflicts using any strategy that works best for your use case. Thus, version vectors will
never lose any writes even if used in a system that is just eventually consistent, unlike the
simpler IntVersioned and Lww wrappers.

## Removing Keys

As you may already know, this is a tricky subject. Removing data from a gossip based key/value
store generally requires maintaining so-called tombstones. Those are records of removal, which
need to be kept forever, because if a server has been disconnected from the others for a long
time, and the others removed a key, he can revive it when he reconnects.

The library does support removal, but since it has no persistence, it does not maintain any
tombstones. There are two ways in which a key can be removed.

One way is by implementing the IDeletable interface on a data type. The idea is that certain
types can reach a state where it becomes safe to delete them. Once in a version which is safe
to delete, it will indicate this by the CanDelete property being true. The backend will keep
it around for some time (default is one minute) to make sure the new removable state is
communicated to other servers, and then clean it up from it's internal storage. It is
important that CanDelete be determined solely by the version and state of the object itself,
so that all servers that have the same value will see the same bool in this property.

For example, the state of a consistent transaction uses this method to get itself cleaned up
once not needed any more. This is perfectly safe, because if an old transaction is revived
later, it will simply fail - most notably, because its changes have already been applied,
so it cannot check out OK again, but there is some more complexity involved. In any case,
once its failure is determined, it will reliably again become deletable.

Another way to remove is to use the method Remove:

```csharp
backend.Remove("some key");
```

Internally, the value under the key keeps the same version, but is marked as deleted. This is
regarded as a higher version, and will be accepted by other servers. However, what happens if
the key gets revived later is now fully up to you. You should probably react to Changed
events, and check in some external tombstone storage whether any newly arriving item may be
kept, and immediately remove it again if not.

## Expiry

All Set method variants of both backends accept an extra parameter with which you can ask for
an item to expire from the storage. For example, to expire a key after 5 seconds:

```csharp
backend.Set("some key", someMergeable, 5000);
```

By writing the same value again with only a different expiry you can extend it, in case you
wish to have sliding expiry. It is not however possible to reduce the expiry, for that you
must write a new, higher version of the data. This is due to the nature of the gossip
implementation - when merging two values which are otherwise equal, the backend simply takes
the max of the expiry values. An exception is if one side did not have expiry, in which case
the result of the merge will have the expiry of the other.

It is important to note that, to avoid issues with server clock synchronization, the backends
do not communicate about the exact time when an item must expire. Rather, they send each other
the information on how many milliseconds a key has left before it expires. Since this number
does not change during transmission of a message, then every time they exchange information
about a key, its expiry can get extended. It is therefore not precise.

## The Consistent Backend

If you need strongly consistent transactions as well, use the ConsistentGossipBackend.
If used in ordinary Shielded transactions, it is only eventually consistent, just like
the GossipBackend. But if you use its RunConsistent method, the transaction will be checked
and will succeed only if a majority of servers agree that it was indeed consistent.

```csharp
var consistentBackend = new ConsistentGossipBackend(someTransport, new GossipConfiguration());
// ...
var (success, withdrawn) = consistentBackend.RunConsistent(() =>
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
like TransactionPreparing, TransactionCommitting, TransactionFailing... for this purpose.*

The consistent transactions are implemented using the ordinary, eventually consistent gossip
backend. The transaction state is stored in a CRDT type, and the servers react to changes to it
by making appropriate state transitions, in effect performing a simple 2-phase commit protocol.

The transaction metadata carries information about all the read and written fields, and will
insist that all the fields you read were up to date, and that all your writes have versions
that are strictly greater than the versions on other servers. It is enough for a majority of
servers to confirm (or reject) a transaction, so the system should work in case of partitions,
as long as you're in the partition that contains more than half of the servers.

NB that calls to RunConsistent will return successfully as soon as the local server declares
success - it will not wait for other servers to raise their state to success as well. This
may change in the future, by adding an option to wait for a number of servers to confirm
success too, which could be important when combining consistent transactions with any external
systems.

You should avoid using the same fields from both consistent and non-consistent transaction,
but FYI, if you access the same fields from non-consistent transactions, the non-consistent transactions
will proceed uninterrupted. Thus, when the other servers confirm your transaction, they guarantee
only that at the point in time when they checked, your transaction was OK, and that no other consistent
transaction can change the affected fields until you decide to commit or roll back. This
behavior ensures that the non-consistent ops never block, which is a useful quality.

## The Gossip Protocol

The library is based on the article "Epidemic Algorithms for Replicated Database Maintenance" by
Demers et al., 1987. It employs only the direct mail and anti-entropy messages, but the
anti-entropy exchange is incremental, and uses a reverse time index of all updates to the
database, which makes it in effect similar to the "hot rumor" approach, but without the risk
of deactivating a rumor before it gets transmitted to all servers. The article is a great read,
and it explains all of this, accompanied with the results of many simulations and experiments.
