package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;

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
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final LinkedHashMap<PageId, Page> idToPage;
    private final Locks locks;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.idToPage = new LinkedHashMap<>(numPages, 0.75f, true);
        this.locks = new Locks();
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
    	BufferPool.pageSize = PAGE_SIZE;
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

        // get lock
        long startTime = System.currentTimeMillis();
        while (!locks.lock(tid, pid, perm)) {
            long currentTime = System.currentTimeMillis();
            if(currentTime - startTime > 150) {
                throw new TransactionAbortedException();
            }
        }

        if (idToPage.containsKey(pid)) {
            return idToPage.get(pid);
        }
        else {
            if (idToPage.size() >= numPages) {
                evictPage();
            }
            int tableId = pid.getTableId();
            DbFile file = Database.getCatalog().getDatabaseFile(tableId);
            Page page = file.readPage(pid);
            idToPage.put(pid, page);
            return page;
        }
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        locks.unlock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return locks.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> lockedPages = locks.getLockedPages(tid);

        // no locked pages, return
        if (lockedPages == null) return;

        // flush or discard the pages if they are in the buffer pool
        for (PageId pid : lockedPages) {
            if (!idToPage.containsKey(pid)) continue;
            Page page = idToPage.get(pid);
            if (page.isDirty() == tid) {
                if (commit) {
                    flushPage(pid);
                }
                else {
                    discardPage(pid);
                }
            }
        }

         // release all locks
         locks.unlockPages(tid);
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
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = table.insertTuple(tid, t);
        // Mark the dirty pages as dirty and put them into the buffer
        for (Page dirtyPage : dirtyPages) {
            dirtyPage.markDirty(true, tid);
            idToPage.put(dirtyPage.getId(), dirtyPage);
        }
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
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = table.deleteTuple(tid, t);
        // Mark the dirty pages as dirty and put them into the buffer
        for (Page dirtyPage : dirtyPages) {
            dirtyPage.markDirty(true, tid);
            idToPage.put(dirtyPage.getId(), dirtyPage);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        ArrayList<PageId> pageIds = new ArrayList<>(idToPage.keySet());
        for(PageId pid : pageIds) {
            flushPage(pid);
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
        idToPage.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = idToPage.get(pid);
        if (page.isDirty() != null) {
            // write page to disk file
            int tableId = pid.getTableId();
            DbFile table = Database.getCatalog().getDatabaseFile(tableId);
            table.writePage(page);
            // mark page as not dirty
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // get the LRU page
        for (Map.Entry<PageId, Page> LRU : idToPage.entrySet()) {
            PageId LRUPageId = LRU.getKey();
            Page LRUPage = LRU.getValue();
            if (LRUPage.isDirty() == null) {
                // flush the LRU page
                try {
                    flushPage(LRUPageId);
                }
                catch (IOException e) {
                    throw new DbException("Flush failed");
                }
                // remove the LRU page from the buffer pool
                idToPage.remove(LRUPageId);
                return;
            }
        }
        throw new DbException("All pages are dirty.");

//        Map.Entry<PageId, Page> LRU = idToPage.entrySet().iterator().next();
//        PageId LRUPageId = LRU.getKey();
//        Page LRUPage = LRU.getValue();
//        // flush the LRU page
//        try {
//            flushPage(LRUPageId);
//        }
//        catch (IOException e) {
//            throw new DbException("Flush failed");
//        }
//        // remove the LRU page from the buffer pool
//        idToPage.remove(LRUPageId);
    }
}
