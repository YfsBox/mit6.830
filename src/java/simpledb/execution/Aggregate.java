package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    private OpIterator child_;
    private Aggregator aggregator_;
    private int gfield_;
    private int afield_;
    private Aggregator.Op aop_;
    private TupleDesc desc_;
    private OpIterator opIt_;

    private void MergeToAggregator() throws IllegalArgumentException,DbException,TransactionAbortedException{
        while (child_.hasNext()) {
            Tuple tup = child_.next();
            aggregator_.mergeTupleIntoGroup(tup);
        }
    }

    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop)  {
        // some code goes here
        gfield_ = gfield;
        afield_ = afield;
        aop_ = aop;
        child_ = child;
        TupleDesc desc = child_.getTupleDesc();
        Type gfieldType = null;
        Type afieldType = desc.getFieldType(afield);
        if (gfield != -1) {
            gfieldType = desc.getFieldType(gfield);
        }
        if (afieldType.equals(Type.INT_TYPE)) {
            aggregator_  = new IntegerAggregator(gfield,gfieldType,afield,aop);
        } else {
            aggregator_ = new StringAggregator(gfield,gfieldType,afield,aop);
        }
        String[] names;
        Type[] types;
        if (gfield != -1) {
            names = new String[2];
            types = new Type[2];

            names[0] = desc.getFieldName(gfield);
            types[0] = gfieldType;
            names[1] = null;
            types[1] = afieldType;
        } else {
            names = new String[1];
            types = new Type[1];

            names[0] = null;
            types[0] = afieldType;
        }

        desc_ = new TupleDesc(types,names);
        opIt_ = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield_;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        //return ;
        return child_.getTupleDesc().getFieldName(gfield_);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield_;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child_.getTupleDesc().getFieldName(afield_);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop_;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child_.open();
        if (opIt_ == null) {
            MergeToAggregator();
            opIt_ = aggregator_.iterator();
        }
        opIt_.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
        if (opIt_.hasNext()) {
            return opIt_.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return desc_;
    }

    public void close() {
        // some code goes here
        opIt_.close();
        child_.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] opit = new OpIterator[1];
        opit[0] = child_;
        return opit;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child_ = children[0];
    }

}
