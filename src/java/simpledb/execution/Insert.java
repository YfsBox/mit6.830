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

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId tid_;
    private OpIterator child_; //迭代这个child_
    private int tableId_;
    private TupleDesc desc_;
    private boolean isNext_;


    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        tid_ = t;
        child_ = child;
        tableId_ = tableId;
        Type[] types = new Type[1];
        String[] names = new String[1];
        types[0] = Type.INT_TYPE;
        names[0] = null;
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
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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
                Database.getBufferPool().insertTuple(tid_,tableId_,tuple);
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