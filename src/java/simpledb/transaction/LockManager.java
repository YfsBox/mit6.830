package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;


public class LockManager {

    public enum LockType {
        SHARED_TYPE,EXCLUSIVE_TYPE,NULL_TYPE
    }

    public class LockRequest {
        private LockType lockType_;
        private TransactionId tid_;
        private boolean isWaiting_;
        private long seqno_;

        public LockRequest(LockType locktype,TransactionId tid,long seqno) {
            lockType_ = locktype;
            tid_ = tid;
            isWaiting_ = true; //初始化时都处于等待状态
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

        public void setWaiting_(boolean isWaiting) {
            isWaiting_ = isWaiting;
        }
    }

    public class LockState {
        private LockType lockType_;
        private TransactionId tid_; //设置读锁对应的只能是null

        public LockState(LockType locktype,TransactionId tid) {
            lockType_ = locktype;
            tid_ = tid;
        }

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

        public boolean canAcquire(LockType lockType) {
            if (lockType_.equals(LockType.NULL_TYPE)) {
                return true;
            } else if (lockType_.equals(LockType.SHARED_TYPE)) {
                return lockType.equals(LockType.SHARED_TYPE);
            } else if (lockType_.equals(LockType.EXCLUSIVE_TYPE)) {
                return false;
            }
            return false;
        }

    }

    private HashMap<Integer,LinkedList<LockRequest>> lockTable_;
    private HashMap<Integer,LockState> lockStates_;


    public LockManager() {
        lockTable_ = new HashMap<Integer,LinkedList<LockRequest>>();
        lockStates_ = new HashMap<Integer,LockState>();
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
            lockStates_.put(hashcode,new LockState(lockType,tid));
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

        boolean canAcquire = state.canAcquire(lockType);
        if (!canAcquire) {
            return false;
        } else { //其中包含了null,也就是没有锁占用的情况
            state.setLockType(lockType);
            state.setTid(tid);

            Iterator<LockRequest> requestIt = lockTable_.get(hashcode).iterator();
            while (requestIt.hasNext()) {
                LockRequest request = requestIt.next();
                long seq = request.getSeqno();
                if (seq == seqno) {
                    request.setWaiting_(false); //取消等待
                    break;
                }
            }
            return true;
            //找到该请求,借助一个随机生成的序列号可好??
        }
    }

    public synchronized boolean ReleaseLock(PageId pageId,TransactionId tid) {

        return false;
    }

    public synchronized boolean ReleaseAllLocks(TransactionId tid) {

        return false;
    }


}
