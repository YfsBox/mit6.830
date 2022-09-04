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
    public void writePage(Page page) throws IOException { //写入一个新的page
        // some code goes here
        // not necessary for lab1
        int pageNum = page.getId().getPageNumber();
        RandomAccessFile raf = new RandomAccessFile(file_,"rw");
        int offset = BufferPool.getPageSize() * pageNum;
        raf.seek(offset);
        raf.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() { //根据这个文件的大小计算出来即可
        // some code goes here
        //System.out.println(file_.length());
        int num = (int) Math.floor(file_.length() * 1.0 / BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> list = new ArrayList<Page>();
        HeapPage result = null;
        RecordId recordId = t.getRecordId();
        int pageNum = numPages(),i;
        for (i = 0; i <pageNum; i ++) {
            HeapPageId pageId = new HeapPageId(heapId_,i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                result = page;
                break;
            } else {
                //这种情况,可以直接释放掉
                Database.getBufferPool().unsafeReleasePage(tid,pageId);//将这个锁释放掉
            }
        }
        if (i >= pageNum) { //需要增加新的页,前面的已经写满了,这个地方由于没有经过getPage获得锁就进行了写,所以需要考虑加锁
            byte[] data = new byte[BufferPool.getPageSize()];
            HeapPageId heapPageId = new HeapPageId(heapId_,i);
            HeapPage newPgae = new HeapPage(heapPageId,data);
            newPgae.insertTuple(t);
            writePage(newPgae);
            newPgae = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
            result = newPgae;
        }
        list.add(result);
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        ArrayList<Page> list = new ArrayList<Page>();
        HeapPage result;
        PageId pageid = t.getRecordId().getPageId();
        result = (HeapPage) Database.getBufferPool().getPage(tid,pageid,Permissions.READ_WRITE);
        result.markDirty(true,tid);
        result.deleteTuple(t);

        list.add(result);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }
}

