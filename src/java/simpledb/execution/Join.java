package simpledb.execution;

import simpledb.optimizer.JoinOptimizer;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    //child1,2指的是两个关系,两个关系中只有符合某个条件的元组才是可返回的Tuple
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    private JoinPredicate joinPredicate_;
    private OpIterator child1_;
    private OpIterator child2_;
    private Tuple currTuple1_;
    private Tuple currTuple2_;

    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        joinPredicate_ = p;
        child1_ = child1;
        child2_ = child2;
        currTuple1_ = null;
        currTuple1_ = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinPredicate_;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        TupleDesc tuple = child1_.getTupleDesc();
        int index = joinPredicate_.getField1();
        return tuple.getFieldName(index);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        TupleDesc tuple = child2_.getTupleDesc();
        int index = joinPredicate_.getField2();
        return tuple.getFieldName(index);
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1_.getTupleDesc(),child2_.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1_.open();
        child2_.open();
    }

    public void close() {
        // some code goes here
        currTuple1_ = null;
        currTuple2_ = null;

        child1_.close();
        child2_.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        currTuple1_ =null;
        currTuple2_ = null;

        child1_.rewind();
        child2_.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    private Tuple joinTuples(Tuple tuple1,Tuple tuple2) { //根据是否符合某个条件将两者合并
        if (joinPredicate_.filter(tuple1,tuple2)) {
            TupleDesc tupleDesc = TupleDesc.merge(tuple1.getTupleDesc(),tuple2.getTupleDesc());
            Tuple tuple = new Tuple(tupleDesc);

            int fieldNum_1 = tuple1.getTupleDesc().numFields(),fieldNum_2 = tuple2.getTupleDesc().numFields();
            for (int i = 0; i < fieldNum_1;i ++) {
                tuple.setField(i,tuple1.getField(i));
            }
            for (int i = 0; i < fieldNum_2;i ++) {
                tuple.setField(fieldNum_1 + i,tuple2.getField(i));
            }
            return tuple;
        }
        return null;
    }


    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (currTuple1_ != null || child1_.hasNext()) {
            if (currTuple1_ == null) {
                if (child1_.hasNext()) {
                    currTuple1_ = child1_.next();
                } else {
                    return null;
                }
            }
            if (!child2_.hasNext()) {
                if(child1_.hasNext()){
                    child2_.rewind();
                    currTuple1_ = child1_.next();
                }else{
                    return null;
                }
            }
            while (child2_.hasNext()) {
                currTuple2_ = child2_.next();
                Tuple tuple = joinTuples(currTuple1_,currTuple2_);
                if (tuple != null) {
                    return tuple;
                }
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] opIterators = new OpIterator[2];
        opIterators[0] = child1_;
        opIterators[1] = child2_;
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child1_ = children[0];
        child2_ = children[1];
    }

}
