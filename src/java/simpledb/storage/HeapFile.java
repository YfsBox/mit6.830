package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file_;
    private TupleDesc tupleDesc_;
    private int heapId_;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file_ = f;
        heapId_ = f.getAbsoluteFile().hashCode(); //用来进行唯一的标识
        tupleDesc_ = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file_;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return heapId_;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return tupleDesc_;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) { //从file里面提取出来相应的page
        // some code goes here
        int tableId = pid.getTableId(),pgNo = pid.getPageNumber(),pgsize = BufferPool.getPageSize();
        int fileLen = (int) file_.length();
        //System.out.println(String.format("pid,tableId: %d,pgNo: %d,size: %d,pageSize: %d",tableId,pgNo,fileLen,pgsize));
        if ((pgNo + 1) * pgsize > fileLen) {
            //System.out.println(String.format("Over the length from readPage,%d,%d",(pgNo + 1)*pgsize,fileLen));
            return null;
        }
        try {
            RandomAccessFile raf = new RandomAccessFile(file_,"r");
            int offset = BufferPool.getPageSize() * pgNo;
            raf.seek(offset);
            byte[] data = new byte[pgsize];
            raf.read(data,0,pgsize);
            HeapPageId hpId = new HeapPageId(tableId,pgNo);
            HeapPage heapPage = new HeapPage(hpId,data);
            raf.close();
            return heapPage;
        } catch (Exception e) {
            //System.out.println();
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() { //根据这个文件的大小计算出来即可
        // some code goes here
        int num = (int) Math.floor(file_.length() * 1.0 / BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }
}

