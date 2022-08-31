package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.io.PrintStream;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId tid_;
    private OpIterator child_;
    private TupleDesc desc_;
    private boolean isNext_;


    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        tid_ = t;
        child_ = child;
        String[] names = new String[1];
        Type[] types = new Type[1];

        names[0] = null;
        types[0] = Type.INT_TYPE;

        desc_ = new TupleDesc(types,names);
        isNext_ = true;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return desc_;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        isNext_ = true;
        child_.open();
        super.open();
    }

    public void close() {
        // some code goes here
        child_.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child_.close();
        child_.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!isNext_) {
            return null;
        }
        int cnt  = 0;
        while (child_.hasNext()) {
            Tuple tuple = child_.next();
            boolean insertOk = true;
            try {
                Database.getBufferPool().deleteTuple(tid_,tuple);
            } catch (Exception e) {
                insertOk = false;
            }
            if (insertOk) {
                cnt += 1;
            }
        }
        Tuple result = new Tuple(desc_);
        IntField field = new IntField(cnt);
        result.setField(0,field);
        isNext_ = false;
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] opIterators = new OpIterator[1];
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child_ = children[0];
    }

}
