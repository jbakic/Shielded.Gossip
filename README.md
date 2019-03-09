# Shielded.Gossip

Shielded.Gossip is a Distributed Transactional Memory for .NET Standard.
It comes with a Gossip Protocol-based key/value store with support for both eventually
and strongly consistent transactions. It is based on the
[Shielded STM library](https://github.com/jbakic/Shielded).

If you only need eventually consistent transactions, use the GossipBackend class.
Changes are done inside ordinary Shielded transactions, and they get propagated
automatically to other servers.

If you need both eventually and strongly consistent transactions, use the ConsistentGossipBackend.
If used in ordinary Shielded transactions, it is only eventually consistent, but if
you use its RunConsistent method, the transaction will be checked and will succeed only
if a majority of servers agree that it was indeed consistent. Its method Prepare enables
you to synchronize a consistent transaction with another transactional system - it
prepares the transaction, and if successful, returns a continuation which you can use to
commit or roll back the transaction later.

The library is a **work in progress**. Some features are still missing, and it needs more testing.

It depends on an implementation of the ITransport interface to send and receive messages.
Included in the library is a simple TCP-based transport in the class TcpTransport, but you
can implement your own to use any library/protocol you prefer.

## CRDTs

All the values the eventually consistent gossip backend works with should be CRDTs -
[Conflict-free Replicated Data Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type).
Using CRDTs provides great guarantees - CRDTs are mergable in an idempotent, commutative and
associative way, which means that the servers must eventually agree on the same version of the
data, regardless of the order or number of messages they exchange. Currently included are
the VersionVector, and a distributed counter implemented in the CountVector class. VectorBase
can be used as a base class to easily implement vector clock-like CRDT types.

If you wish to use any other types, which are not CRDTs, you can add the IHasVersionVector
interface to them, and use the Multiple&lt;T&gt; wrapper to make them CRDTs. You can also
use the VecVersioned&lt;T&gt; wrapper to add a vector clock to any type you cannot modify. The
backends have helper methods to make this easier.

[Version vectors](https://en.wikipedia.org/wiki/Version_vector) reliably detect conflicting edits
to the same key, and the Multiple keeps the conflicting versions, allowing you to resolve
conflicts using any strategy that works best for your use case. Of course, if you only use
consistent transactions on a field, you won't have to deal with that.

The library also has Lww&lt;T&gt;, for last-write-wins semantics, and IntVersioned&lt;T&gt;, for a
simple int-versioned field. These two are fake CRDTs - they implement IMergeable, but their merge
operations do not meet the CRDT criteria. They are not safe if used in eventually consistent
transactions. They're meant to be used with fields which you only change through consistent
transactions, or fields whose safety is guaranteed by another system, like a database with
optimistic concurrency checks on update. However, the Lww wrapper can be used in non-consistent
transactions too, in cases where you simply don't care about consistency of a field, and
last-write-wins is good enough.

## Consistency

The consistent transactions are implemented using the ordinary, eventually consistent gossip
backend. The transaction state is stored in a CRDT type, and the servers react to changes to it
by making appropriate state transitions.

The transaction metadata carries information about all the read and written fields, and will
insist that all the fields you read were up to date, and that all your writes have versions
that are strictly greater than the versions on other servers. It is enough for a majority of
servers to confirm (or reject) a transaction, so the system should work in case of partitions,
as long as you're in the partition that contains more than half of the servers.

You should avoid using the same fields from both consistent and non-consistent transaction,
but FYI, if you access the same fields from non-consistent transactions, the non-consistent transactions
will proceed uninterrupted. Thus, when the other servers confirm your transaction, they guarantee
only that at the point in time when they checked, your transaction was OK, and that no other consistent
transaction can change the affected fields until you decide to commit or roll back. This
behavior ensures that the non-consistent ops never block, which is a useful quality.

## The protocol

The library is based on the article "Epidemic Algorithms for Replicated Database Maintenance" by
Demers et al., 1987. It employs only the direct mail and anti-entropy messages, but the
anti-entropy exchange is incremental, and uses a reverse time index of all updates to the
database, which makes it in effect similar to the "hot rumor" approach, but without the risk
of deactivating a rumor before it gets transmitted to all servers. The article is a great read,
and it explains all of this, accompanied with the results of many simulations and experiments.
