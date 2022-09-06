package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class LockManager {

    public enum LockType {
        SHARED_TYPE,EXCLUSIVE_TYPE,NULL_TYPE
    }

    public class LockRequest {
        private LockType lockType_;
        private TransactionId tid_;
        private long seqno_;

        public LockRequest(LockType locktype,TransactionId tid,long seqno) {
            lockType_ = locktype;
            tid_ = tid;
            //isWaiting_ = true; //初始化时都处于等待状态
            seqno_ = seqno;
        }

        public LockType getLockType() {
            return lockType_;
        }

        public TransactionId getTid() {
            return tid_;
        }

        public long getSeqno() {
            return seqno_;
        }

    }

    public class LockState {
        private LockType lockType_;
        private TransactionId tid_; //设置读锁对应的只能是null
        private int shardCnt_;

        public LockState() {
            lockType_ = LockType.NULL_TYPE;
            tid_ = null;
        }

        public TransactionId getTid() {
            return tid_;
        }
        public LockType getLockType() {
            return lockType_;
        }

        public void setTid(TransactionId tid) {
            tid_ = tid;
        }
        public void setLockType(LockType lockType) {
            lockType_ = lockType;
        }
        public void setShardCnt(int cnt) {
            shardCnt_ = cnt;
        }
        private void updateState(LockType lockType,TransactionId tid) {
            lockType_ = lockType;
            tid_ = tid;
        }
        public boolean canAcquire(LockType lockType,TransactionId tid) {
            boolean result = false;
            if (lockType_.equals(LockType.NULL_TYPE)) {
                if (lockType.equals(LockType.SHARED_TYPE)) {
                    shardCnt_ = 1;
                } else {
                    shardCnt_ = 0;
                }
                updateState(lockType,tid);
                result = true;
            } else if (lockType_.equals(LockType.SHARED_TYPE)) {
                if (lockType.equals(lockType_)) {
                    if (!tid.equals(tid_)) {
                        shardCnt_ += 1;
                    }
                    updateState(lockType,tid);
                    result = true;
                } else {
                    if (tid.equals(tid_) && shardCnt_ == 1) {
                        shardCnt_ = 0;
                        updateState(lockType,tid);
                        result = true;
                    } else {
                        result = false;
                    }
                }
            } else if (lockType_.equals(LockType.EXCLUSIVE_TYPE)) {
                result = tid_.equals(tid); //这个时候不需要变
            }
            return result;
        }

    }

    private ConcurrentMap<Integer,LinkedList<LockRequest>> lockTable_;
    private ConcurrentMap<Integer,LockState> lockStates_;


    public LockManager() {
        lockTable_ = new ConcurrentHashMap<Integer,LinkedList<LockRequest>>();
        lockStates_ = new ConcurrentHashMap<Integer,LockState>();
    }

    private LockType perm2LockType(Permissions perm) {
        LockType lockType;
        if (perm.equals(Permissions.READ_ONLY)) {
            lockType = LockType.SHARED_TYPE;
        } else {
            lockType = LockType.EXCLUSIVE_TYPE;
        }
        return lockType;
    }

    public synchronized long AddRequest(PageId pageId, TransactionId tid, Permissions perm) { //只可有一个线程访问,达到互斥访问的效果
        int hashcode = pageId.hashCode();

        LockType lockType = perm2LockType(perm);
        if (!lockTable_.containsKey(hashcode)) {
            lockTable_.put(hashcode,new LinkedList<LockRequest>());
            lockStates_.put(hashcode,new LockState());
        }
        Random random = new Random();
        long seqno = random.nextLong();
        lockTable_.get(hashcode).add(new LockRequest(lockType,tid,seqno));
        return seqno;
    }

    public synchronized boolean AcquireLock(PageId pageId,TransactionId tid,Permissions perm,long seqno) {

        int hashcode = pageId.hashCode();
        LockType lockType = perm2LockType(perm);
        LockState state = lockStates_.get(hashcode);

        boolean canAcquire = state.canAcquire(lockType,tid);
        if (!canAcquire) {
            return false;
        } else { //其中包含了null,也就是没有锁占用的情况
            boolean isfound = false;
            Iterator<LockRequest> requestIt = lockTable_.get(hashcode).iterator();
            while (requestIt.hasNext()) {
                LockRequest request = requestIt.next();
                long seq = request.getSeqno();
                if (seq == seqno) {
                    //request.setWaiting_(false); //取消等待
                    isfound = true;
                    break;
                }
            }
            return isfound;
        }
    }

    public synchronized void ReleaseLock(int pageHash,TransactionId tid) {
        int hashcode = pageHash;
        LockType currType = lockStates_.get(hashcode).getLockType();
        TransactionId currTid = lockStates_.get(hashcode).getTid();
        if (lockTable_.containsKey(hashcode)) {
            Iterator<LockRequest> requestIterator = lockTable_.get(hashcode).iterator();
            while (requestIterator.hasNext()) {
                LockRequest request = requestIterator.next();
                if (request.getTid().equals(tid)) {
                    if (currType.equals(LockType.EXCLUSIVE_TYPE) && tid.equals(currTid)) {
                        lockStates_.get(hashcode).setTid(null);
                        lockStates_.get(hashcode).setLockType(LockType.NULL_TYPE);
                    } else if (currType.equals(LockType.SHARED_TYPE)) { //通常情况下,当前为共享锁的话
                        int shardCnt = lockStates_.get(hashcode).shardCnt_;
                        if (shardCnt == 1) {
                            lockStates_.get(hashcode).setTid(null);
                            lockStates_.get(hashcode).setLockType(LockType.NULL_TYPE);
                        } else {
                            lockStates_.get(hashcode).setShardCnt(shardCnt - 1);
                        }
                    }
                    requestIterator.remove();//移除这一项
                }
            }
        }
    }

    public synchronized void ReleaseAllLocks(TransactionId tid) {
        Iterator<Integer> it = lockTable_.keySet().iterator();
        while (it.hasNext()) {
            int hash = it.next();
            ReleaseLock(hash,tid);
        }
    }

    public synchronized boolean IsHolding(PageId pageId,TransactionId tid) {
        int hashcode = pageId.hashCode();
        if (!lockStates_.containsKey(hashcode)) {
            return false;
        }
        return lockStates_.get(hashcode).tid_.equals(tid);
    }

    public synchronized TransactionId getHoldingTid(PageId pageId) {
        int hash = pageId.hashCode();
        return lockStates_.get(hash).tid_;
    }


}
