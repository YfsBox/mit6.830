package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.lang.reflect.Array;
import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
//这个类用来表示一个直方图
public class IntHistogram {

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
    private int bucketsNum_;
    private int min_;
    private int max_;
    private Integer[] hightArray_;
    private int nTups_;

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        bucketsNum_ = buckets;
        min_ = min;
        max_ = max;

        nTups_ = 0;

        hightArray_ = new Integer[bucketsNum_];
        Arrays.fill(hightArray_,0);
    }

    private int getGap() {
        return max_ - min_ + 1;
    }

    private double getWidth() {
        return (max_ * 1.0 - min_ * 1.0 + 1) / bucketsNum_;
    }

    private int getIndex(int value) {
        if (value <= min_) {
            return 0;
        }
        if (value >= max_) {
            return bucketsNum_ - 1;
        }
        double width = getWidth();
        int index = (int) ((value * 1.0  - min_) / width);
        return index;
    }

    private double getOffset(int value) {
        double width = getWidth();
        int index = getIndex(value);
        double offset = value - (min_ + index * width);
        return offset;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < min_ || v > max_) {
            return;
        }
        hightArray_[getIndex(v)] += 1;
        nTups_ += 1;
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


    private double getLessSum(int v) {
        if (v <= min_) {
            return 0;
        }
        if (v > max_) {
            return 1.0;
        }

        double sum = 0,width = getWidth(),offset = getOffset(v);
        int index = getIndex(v);
        for(int i = 0; i < index; i ++) {
            sum += (hightArray_[i] / (double) nTups_);
        }
        double cnt = hightArray_[index]/ (double) nTups_ / width;

        sum += cnt * offset;
        return sum;
    }

    public double estimateSelectivity(Predicate.Op op, int v) {
        String opStr = op.toString();
        double sum = 0;
    	// some code goes here
        if (opStr.equals("=")) {
            sum = getLessSum(v + 1) - getLessSum(v);
        } else if (opStr.equals("<")) {
            sum = getLessSum(v);
        } else if (opStr.equals(">")) {
            sum = 1.0 - getLessSum(v + 1);
        } else if (opStr.equals("<=")) {
            sum = getLessSum(v + 1);
        } else if (opStr.equals(">=")) {
            sum = 1.0 - getLessSum(v);
        } else if (opStr.equals("<>")) {
            sum = 1.0 - getLessSum(v + 1) + getLessSum(v);
        }
        return sum;
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
        double sum = 0.0;
        for (int i = 0 ; i < bucketsNum_; i ++) {
            sum += hightArray_[i];
        }
        return sum / nTups_;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        /*return String.format("IntHistgram(buckets=%d, min=%d, max=%d",
                buckets.length, min, max);*/
        return null;
    }
}
