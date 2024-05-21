package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int buckets;
    private final int min;
    private final int max;
    private int[] histogram;
    private final int bucket_size;
    private int numTuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.histogram = new int[buckets];
        if ((max - min + 1) % buckets == 0) {
            this.bucket_size = (max - min + 1) / buckets;
        }
        else {
            this.bucket_size = (max - min + 1) / buckets + 1;
        }
        this.numTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bin = (v - min) / bucket_size;
        histogram[bin]++;
        numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int bin = (v - min) / bucket_size;
        int bin_left = min + bin * bucket_size;
        int bin_right = min + (bin + 1) * bucket_size;
        double selectivity = 0;

        switch (op) {
            case EQUALS:
                if (bin < 0 || bin >= buckets) {
                    selectivity = 0;
                }
                else {
                    selectivity = ((double) histogram[bin] / bucket_size) / numTuples;
                }
                break;

            case GREATER_THAN:
                if (bin < 0) {
                    selectivity = 1;
                }
                else if (bin >= buckets) {
                    selectivity = 0;
                }
                else {
                    selectivity += ((double) (bin_right - v - 1) / bucket_size) * ((double) histogram[bin] / numTuples);
                    for (int i = bin + 1; i < buckets; ++i) {
                        selectivity += (double) histogram[i] / numTuples;
                    }
                }
                break;

            case LESS_THAN:
                if (bin < 0) {
                    selectivity = 0;
                }
                else if (bin >= buckets) {
                    selectivity = 1;
                }
                else {
                    selectivity += ((double) (v - bin_left) / bucket_size) * ((double) histogram[bin] / numTuples);
                    for (int i = 0; i < bin; ++i) {
                        selectivity += (double) histogram[i] / numTuples;
                    }
                }
                break;

            case LESS_THAN_OR_EQ:
                if (bin < 0) {
                    selectivity = 0;
                }
                else if (bin >= buckets) {
                    selectivity = 1;
                }
                else {
                    selectivity += ((double) (v - bin_left + 1) / bucket_size) * ((double) histogram[bin] / numTuples);
                    for (int i = 0; i < bin; ++i) {
                        selectivity += (double) histogram[i] / numTuples;
                    }
                }
                break;

            case GREATER_THAN_OR_EQ:
                if (bin < 0) {
                    selectivity = 1;
                }
                else if (bin >= buckets) {
                    selectivity = 0;
                }
                else {
                    selectivity += ((double) (bin_right - v) / bucket_size) * ((double) histogram[bin] / numTuples);
                    for (int i = bin + 1; i < buckets; ++i) {
                        selectivity += (double) histogram[i] / numTuples;
                    }
                }
                break;

            case LIKE:
                break;

            case NOT_EQUALS:
                if (bin < 0 || bin >= buckets) {
                    selectivity = 1;
                }
                else {
                    selectivity = 1 - ((double) histogram[bin] / bucket_size) / numTuples;
                }
        }
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
