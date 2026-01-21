package collocation.mapreduce;

public class CollocationResult implements Comparable<CollocationResult> {
    public String decade;
    public double llr;
    public String words;

    public CollocationResult(String decade, double llr, String words) {
        this.decade = decade;
        this.llr = llr;
        this.words = words;
    }

    @Override
    public int compareTo(CollocationResult o) {
        // MinHeap needs smallest LLR at root to evict it
        return Double.compare(this.llr, o.llr);
    }
}
