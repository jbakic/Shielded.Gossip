namespace Shielded.Gossip
{
    public interface IMergeable<in TIn, out TOut>
    {
        TOut MergeWith(TIn other);
    }

    public interface IMergeable<in TIn> : IMergeable<TIn, object>
    {
    }
}