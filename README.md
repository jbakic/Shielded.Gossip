# Shielded.Gossip

Shielded.Gossip is a Distributed Transactional Memory implementation for .NET Standard.
It comes with a Gossip Protocol-based implementation of a key/value store which supports
both eventually and strongly consistent transactions. It can be extended with additional,
custom backends. It is based on the [Shielded STM library](https://github.com/jbakic/Shielded).
A new version of Shielded is included here, in the namespace Shielded.Standard, which
supports .NET Standard, but does not include the proxy class generation feature.

You can use it to implement a distributed cache, combinable with an external data store. The
external store can guarantee consistency, or you can use the consistent gossip backend to
ensure certain operations are consistent.

It depends on an implementation of the ITransport interface to send and receive messages.
Included in the library is a simple TCP-based transport in the class TcpTransport. You are
more than welcome to replace it with any other communication means better suited to your
project.

All the values the gossip backend works with must be CRDTs -
[Conflict-free Replicated Data Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type).
If you are wish to use any other types, which are not CRDTs, you can add the IHasVectorClock
interface to them, and use the Multiple&lt;T&gt; wrapper to make them CRDTs. You can also
use the Vc&lt;T&gt; wrapper to add a vector clock to any type you cannot modify. The backends
have helper methods to make this easier.

[Vector clocks](https://en.wikipedia.org/wiki/Vector_clock) reliably detect conflicting edits
to the same key, and the Multiple keeps the conflicting versions, allowing you to resolve the
conflict using any strategy that works best for your data type. Of course, if you only use
consistent transactions on a field, you won't have to deal with that.

Using CRDTs provides great guarantees - CRDTs are mergable in an idempotent, commutative and
associative way, which means that the servers must eventually agree on the same version of the
data regardless of the order or number of messages they exchange. Currently included are
the VectorClock, and a distributed counter implemented in the CountVector class. VectorBase
can be used as a base class to easily implement many CRDT types.

The library is still a work in progress. I hope it may be useful already, but it would benefit from
some extra features, and more real-life usage and testing.

## The protocol

The library is based on the article "Epidemic Algorithms for Replicated Database Maintenance" by
Demers et al., 1987. It employs only the direct mail and anti-entropy messages, but the
anti-entropy exchange is incremental, and uses a reverse time index of all updates to the
database, which makes it in effect similar to the "hot rumor" approach, but without the risk
of deactivating a rumor before it gets transmitted to all servers. The article is a great read,
and it explains all of this, accompanied with the results of many simulations and experiments.

## Consistency

The consistent transactions are implemented using the ordinary, eventually consistent gossip
backend. The transaction state is stored in a CRDT type, and the servers react to changes to it
by making appropriate state transitions.

The transactions only contain information on written fields. This makes them more efficient,
but means they suffer from the Write Skew issue - if you only read from a field, we will not
check on other servers whether that field has the same value there which you saw locally. If
you need such a guarantee, you can make a write into that field, and it will then be checked
as well.

An important thing to note is that the only thing a transaction checks is the outcome of
comparing the version of your data item, and whatever it finds on each server. If the outcome
locally was Greater, which means your version of the data was strictly higher than the existing
data, we will then insist that the result is Greater on all machines. If however your Set call
returned Conflict, which means conflicting changes have already occurred elsewhere, then the
transaction will transmit and apply that field too, but without checking it in any way.
(Calls to Set which return Less or Equal do not actually change anything, and are never
included in the transaction.) By checking the responses of your Set and SetVersion calls,
you can control which level of protection is employed.
