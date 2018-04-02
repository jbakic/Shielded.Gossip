namespace Shielded.Gossip
{
    public interface IMergeable<in TIn, out TOut>
    {
        TOut MergeWith(TIn other);
        VectorRelationship VectorCompare(TIn other);
        //ulong VersionHash { get; }
    }
}