namespace Shielded.Gossip
{
    public interface IHasVersionHash
    {
        VersionHash GetVersionHash();
    }

    public interface IMergeable<in TIn, out TOut> : IHasVersionHash
    {
        TOut MergeWith(TIn other);
        VectorRelationship VectorCompare(TIn other);
    }
}