package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.LockManager;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final int MAX_TRYACQUIRE_NUM = 50000;
    private static final long TIMEOUT = 2000;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    //private ArrayList<Page> pages_;
    private HashMap<Integer,Page> pages_;
    private int maxPageNum_;
    private LinkedList<Integer> fifoQueue_;
    private LockManager lockManager_;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pages_ = new HashMap<Integer,Page>();
        maxPageNum_ = numPages;
        fifoQueue_ = new LinkedList<Integer>();
        lockManager_ = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        //首先寻找有没有符合要求的
        long requestSeq = lockManager_.AddRequest(pid,tid,perm);
        long start = System.currentTimeMillis();
        while (!lockManager_.AcquireLock(pid,tid,perm,requestSeq)){
            long now = System.currentTimeMillis();
            if (now - start > TIMEOUT) {
                throw new TransactionAbortedException();
            }
        }
        int hashcode = pid.hashCode();
        if (pages_.containsKey(hashcode)) {
            Page result = pages_.get(hashcode);
            return result;
        }
        //到了这里说明没有
        int size = pages_.size();
        if (size >= maxPageNum_) {
            //throw new DbException(String.format("The pages.size is %d,add new page error from getPage",size));
            evictPage();
        }
        DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbfile.readPage(pid);
        pages_.put(hashcode,page);
        fifoQueue_.add(hashcode);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager_.ReleaseLock(pid.hashCode(),tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) { //如果是共享锁该怎么判断
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager_.IsHolding(p,tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */


    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        } else {
            try {
                RecoverPages(tid);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        lockManager_.ReleaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     *
     *
     */
    private void updatePagePool(List<Page> pagelist,TransactionId tid) throws DbException{
        int size = pagelist.size();
        for (int i = 0 ; i < size; i ++) {
            HeapPage page = (HeapPage) pagelist.get(i);
            page.markDirty(true,tid);
            int hashcode = page.pid.hashCode(); //注意是要得出pid的hashcode
            if (pages_.containsKey(hashcode)) {
                Page oldPage = pages_.get(hashcode);
                oldPage = page; //更新
            } else {
                //暂且先不考虑超出的情况
                if(pages_.size() >= maxPageNum_)  {
                    evictPage();
                }
                pages_.put(hashcode,page);
            }
        }
    }


    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        HeapFile hpfile = (HeapFile) file;
        List<Page> pagelist = hpfile.insertTuple(tid,t);
        updatePagePool(pagelist,tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        HeapFile hpfile = (HeapFile) file;
        List<Page> pagelist = hpfile.deleteTuple(tid,t);
        updatePagePool(pagelist,tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Integer> keysetIt = fifoQueue_.iterator();
        while (keysetIt.hasNext()) {
            int key = keysetIt.next();
            HeapPage page = (HeapPage) pages_.get(key);
            PageId pageId = page.getId();
            HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(pageId.getTableId());
            if (page.isDirty() != null) { //说明dirty
                file.writePage(page);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        int tableId = pid.getTableId();
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        int hashCode = pid.hashCode();
        if (pages_.containsKey(hashCode)) {
            HeapPage page = (HeapPage) pages_.get(hashCode);
            if (page.isDirty() != null) {
                file.writePage(page);
            }
        } //写入
    }

    private synchronized void RecoverPages(TransactionId tid) throws IOException {
        Iterator<Page> pageIt = pages_.values().iterator();
        while (pageIt.hasNext()) {
            Page page =  pageIt.next();
            if (tid.equals(page.isDirty())) { //只要等于tid就写回
                DbFile file = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                try {
                    int hashcode = page.getId().hashCode();
                    HeapPage hpage = (HeapPage) page.getBeforeImage();
                    file.writePage(hpage);
                    pages_.put(hashcode,hpage);
                } catch (IOException e) {
                    System.out.println("The flush page error in flushForTid");
                    e.printStackTrace();
                }
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<Page> pageIt = pages_.values().iterator();
        while (pageIt.hasNext()) {
            Page page =  pageIt.next();
            if (tid.equals(page.isDirty())) { //只要等于tid就写回
                DbFile file = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                try {
                    file.writePage(page);
                } catch (IOException e) {
                    System.out.println("The flush page error in flushForTid");
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Iterator<Integer> fifoIterator = fifoQueue_.iterator();
        while (fifoIterator.hasNext()) {
            int hash = fifoIterator.next();
            HeapPage page = (HeapPage) pages_.get(hash);
            if (page.isDirty() == null) {
                fifoIterator.remove();
                pages_.remove(hash);
                return;
            }
        }
        throw new DbException("no undirty page to evict");
    }

}
