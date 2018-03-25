namespace Shielded.Gossip
{
    public interface IMergeable<in TIn, out TOut>
    {
        TOut Wrap();
        TOut MergeWith(TIn other);
    }
}