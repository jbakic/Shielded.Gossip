# Shielded.Gossip

This is a work in progress.

Shielded.Gossip is a Distributed Transactional Memory implementation for .NET Standard.
It comes with a Gossip Protocol implementation of a simple key/value store which supports
both eventually and strongly consistent transactions. It can be extended with additional,
custom backends. It is based on the [Shielded STM library](https://github.com/jbakic/Shielded),
which provides transactional memory in one process, but which does not support .NET Standard.
A new version of Shielded is included here, Shielded.Standard, which supports .NET Standard but
does not have proxy class generation.
