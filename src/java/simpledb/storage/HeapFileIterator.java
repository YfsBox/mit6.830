package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{

    private HeapFile hpFile_;
    private TransactionId tid_;
    private Iterator<Tuple> tupleIterator_;
    private int currPageNo_;

    public HeapFileIterator(HeapFile file,TransactionId tid) {
        hpFile_ = file;
        tid_ = tid;
        tupleIterator_ = null;
        currPageNo_ = 0;
    }

    private Iterator<Tuple> getTupleIterator(int pgno) throws TransactionAbortedException,DbException {
        HeapPageId pgId = new HeapPageId(hpFile_.getId(),pgno);
        Page page = Database.getBufferPool().getPage(tid_,pgId, Permissions.READ_WRITE); //这个地方应该和bufferpool直接交互
        HeapPage heapPage = (HeapPage) page;
        if (page == null) {
            System.out.println("The heapPage is null");
        }
        return heapPage.iterator();
    }


    @Override
    public void open() throws DbException, TransactionAbortedException {
        currPageNo_ = 0;
        tupleIterator_ = getTupleIterator(currPageNo_);
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (tupleIterator_ != null && tupleIterator_.hasNext()) {
            return true;
        }
        while (currPageNo_ + 1 < hpFile_.numPages()) {
            Iterator<Tuple> nextIt = getTupleIterator(currPageNo_ + 1);
            if (nextIt != null && nextIt.hasNext()) {
                return true;
            }
            currPageNo_ += 1;
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        //return null;
        if(tupleIterator_ == null) {
            throw new NoSuchElementException("The tupleIterator is null from HeapFileIterator");
        }
        if (tupleIterator_.hasNext()) {
            return tupleIterator_.next();
        }
        //考虑进入下一页
        if(currPageNo_ + 1 < hpFile_.numPages()) {
            currPageNo_ ++;
            tupleIterator_ = getTupleIterator(currPageNo_);
            return tupleIterator_.next();
        }
        //已经没有下一页了
        throw new NoSuchElementException("Not have next Page from tupleIterator next");
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        currPageNo_ = 0;
        tupleIterator_ = null;
    }
}
