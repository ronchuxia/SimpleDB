package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

class Lock {
    private final TransactionId tid;
    private final PageId pid;
    private boolean isExclusive;

    public Lock(TransactionId tid, PageId pid, boolean isExclusive) {
        this.tid = tid;
        this.pid = pid;
        this.isExclusive = isExclusive;
    }

    public boolean isLockExclusive() {
        return isExclusive;
    }

    public TransactionId getTid() {
        return tid;
    }

    public void upgrade() {
        isExclusive = true;
    }
}

public class Locks {
    private final ConcurrentHashMap<TransactionId, ConcurrentHashMap<PageId, Lock>> tidToLock;
    private final ConcurrentHashMap<PageId, ArrayList<Lock>> pidToLock;

    public Locks() {
        tidToLock = new ConcurrentHashMap<>();
        pidToLock = new ConcurrentHashMap<>();
    }

    public synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {
        boolean isExclusive = (perm == Permissions.READ_WRITE);

        if (tid == null) return true;

        if (!tidToLock.containsKey(tid)) {
            tidToLock.put(tid, new ConcurrentHashMap<>());
        }

        if (!pidToLock.containsKey(pid)) {
            pidToLock.put(pid, new ArrayList<>());
        }

        ConcurrentHashMap<PageId, Lock> transactionPidToLock = tidToLock.get(tid);
        ArrayList<Lock> pageLocks = pidToLock.get(pid);

        // if the transaction has a lock on this page
        if (transactionPidToLock.containsKey(pid)) {
            Lock lock = transactionPidToLock.get(pid);
            // if the lock is exclusive, return
            if (lock.isLockExclusive()) {
                return true;
            }
            // if the lock is shared and the required lock is exclusive
            else if (isExclusive) {
                // if there is only one shared lock on this page, upgrade it to be exclusive and return
                if (pageLocks.size() == 1) {
                    lock.upgrade();
                    return true;
                }
                // if there are more than one shared locks on this page, block
                else {
                    return false;
                }
            }
            // if the lock is shared and the required lock is shared, return
            else {
                return true;
            }
        }
        // if the transaction doesn't have a lock on this page
        else {
            // if the required lock is exclusive and there are at least one shared locks on this page, block
            if (isExclusive && !pageLocks.isEmpty()) {
                return false;
            }

            // if the required lock is shared and there is an exclusive lock on this page, block
            if (!isExclusive) {
                for (Lock l: pageLocks) {
                    if (l.isLockExclusive()) {
                        return false;
                    }
                }
            }

            // if the required lock is exclusive and there are no locks on this page
            // or if the required lock is shared and there are no exclusive locks on this page
            // build a new lock and return
            Lock newLock = new Lock(tid, pid, isExclusive);
            transactionPidToLock.put(pid, newLock);
            pageLocks.add(newLock);
            return true;
        }
    }

    public synchronized void unlock(TransactionId tid, PageId pid) {
        if (!tidToLock.containsKey(tid)) return;
        ConcurrentHashMap<PageId, Lock> transactionPidToLock = tidToLock.get(tid);
        if (!pidToLock.containsKey(pid)) return;
        ArrayList<Lock> pageLocks = pidToLock.get(pid);

        Lock lock = transactionPidToLock.get(pid);
        transactionPidToLock.remove(pid);
        pageLocks.remove(lock);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        if (tidToLock.containsKey(tid)) {
            return tidToLock.get(tid).containsKey(pid);
        }
        else {
            return false;
        }
    }

    public Set<PageId> getLockedPages(TransactionId tid) {
        if (!tidToLock.containsKey(tid)) return null;
        return tidToLock.get(tid).keySet();
    }

    public void unlockPages(TransactionId tid) {
        ConcurrentHashMap<PageId, Lock> transactionPidToLock;

        if (!tidToLock.containsKey(tid)) return;
        transactionPidToLock = tidToLock.get(tid);
        for (PageId pid : transactionPidToLock.keySet()) {
            unlock(tid, pid);
        }
        tidToLock.remove(tid);
    }
}
