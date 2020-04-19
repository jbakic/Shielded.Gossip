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

What happens if a removed key gets revived later is up to you. You can react to Changed
events and, when a new item is added which should not be there, simply remove it again.

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
attempts.

The consistent transactions are implemented using the ordinary, eventually consistent gossip
backend. The transaction state is stored in a CRDT type, and the servers react to changes to it
by making appropriate state transitions, following a simplified Paxos-like protocol.

*It is capable of surviving partitions, and most of the time the majority partition will be able
to continue executing transactions. However, right now it is rather simple and there are cases
where a majority partition could get blocked where a proper Paxos implementation would not.
This is still a work in progress. More details on the implementation are given below.*

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

## The Gossip Protocol Implementation

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

## The Consistent Protocol Implementation

Consistent transactions follow a simple Paxos-like protocol, with special consideration for the
fact that it is running over a gossip network.

When a transaction is started, the initiator will check it locally and if it is OK and does not
conflict with any currently running transaction, it will vote Promised on it and write it into
the underlying gossip store to distribute it to other servers. Every server that receives the
transaction will check it and vote Promised or Rejected depending on the result. When a server
is Promised on a transaction, and it witnesses a majority of Promised votes, it raises its state
to Accepted. When a majority of Accepted votes is witnessed by any server, it commits the
transaction's writes into the gossip store, and we're done. If a majority of Rejected votes is
seen, the transaction fails. The initiator may then make a new attempt, reading newer data, but
this will have a new unique ID and will be handled like any other, independent transaction.

Every transaction is assigned a ballot number, which comes from a
[Lamport clock](https://en.wikipedia.org/wiki/Lamport_timestamps) kept by the servers. Every time
a server wishes to move its state forward on a transaction, it finds all active transactions
that are in conflict with it, and checks the following:

* If there is a conflicting transaction with a higher ballot number, and we are Promised or
Accepted on it, we will not move forward with this one.

* If there is a conflicting transaction with a lower ballot number, and we are in the
Accepted state on it, we will not move forward with this one, unless it already has at least one
other Accepted vote.

So, a promise can switch to a higher ballot competitor, but once we have accepted a transaction,
we cannot accept any conflicting one, even if it has a higher ballot. We should stay in this
state and wait until a majority is formed for one or the other transaction. However, if a higher
ballot transaction has managed to reach at least one Accepted vote, then we know for sure that
our transaction can never commit! A majority of servers must have voted Promised on that other
transaction for it to get the Accepted vote, so at least some of those who voted Promised for
ours have switched to the new one and will not vote for us any more (see first rule above).

It's important to note a gossip-specific point here - we cannot reject lower ballot transactions
straight away! A transaction may be revived after a long time, by a server that was disconnected,
and after other servers have forgotten about that transaction. To guarantee that this will not lead
to such a zombie transaction being committed, servers only vote Rejected once it is absolutely certain
that it can never commit. That means, we will wait until another transaction successfully writes into
the field, preventing others from passing the consistency check.

This is why the servers do not simply keep a record of just one transaction for every field, as you
might expect if you know how Paxos works. Instead they keep track of all currently active transactions.
If a higher-ballot transaction ends up failing (perhaps an even higher-ballot competitor committed and
killed it), the servers might yet commit its lower-ballot competitor.

All in all, this approach means that the initiator is rather irrelevant. Transactions take on a life
of their own when in the network, and even if the initiator gets disconnected, the others can easily
continue and resolve the transaction on their own, without requiring repeated proposal rounds to resolve
conflicts.

In case of a partition, when two transactions are in conflict and have already been accepted by at
least some servers, then the one with the higher ballot will win and be committed. Likewise, if both
are just promised. But if one lower ballot transaction has been accepted by some, while others have
promised on a higher ballot one, and neither group can reach majority without the other, then the
system will be blocked until the partition heals. Any attempt to switch votes here would either cause
consistency issues or blockages in other situations.

This is where we do need those further rounds of proposing from Paxos. The proposer in a next round
would, since it's a new ballot, win over all the reachable servers' promises and then select to go
forward with the accepted value. I plan to similarly bump up lower ballot transactions in such
situations. If a server has accepted a transaction, and that transaction does not reach resolution
after a certain time, the server will start a new voting round for it, with a new ballot number.
If the partition then suddenly heals, and we see that the old voting round had a resolution, fine,
we commit or reject depending on that vote. But if not, the new voting round should win over to this
transaction those servers who were previously promised on a newer competitor, and thus resolve this
block.

I believe that with that improvement the algorithm should become equivalent to full-blown Paxos.
But, that will significantly increase its complexity, and in any case, to prove such a big claim a lot
of work will be needed, particularly since this is not a simple, direct implementation of Paxos. But
I hope I've convinced you that it is well suited for a gossip-based system. It resolves many types
of conflict efficiently and without centralized coordination. Even now, in this simple version, it is
better than the two-phase commit used here previously, which depended much more on the initiator
(AKA the coordinator in descriptions of 2PC), and which would definitely block if the initiator were
to end up in a minority partition.

If you see any problems here, please let me know! Open an issue in the issue tracker here and we can
discuss it.
