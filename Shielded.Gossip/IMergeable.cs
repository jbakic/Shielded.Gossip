namespace Shielded.Gossip
{
    public interface IHasVersionHash
    {
        VersionHash GetVersionHash();
    }

    public interface IMergeable<in TIn, out TOut> : IHasVersionHash
        where TOut : IMergeable<TIn, TOut>
    {
        TOut MergeWith(TIn other);
        VectorRelationship VectorCompare(TIn other);
    }
}