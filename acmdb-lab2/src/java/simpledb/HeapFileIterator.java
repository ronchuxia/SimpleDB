package simpledb;

import java.util.Iterator;

public class HeapFileIterator extends AbstractDbFileIterator {
    private final int tableId;
    private final int numPages;
    private final TransactionId tid;
    private int pageNum;
    private boolean isOpen;
    private Iterator<Tuple> it;

    public HeapFileIterator(int tableId, int numPages, TransactionId tid) {
        this.tableId = tableId;
        this.numPages = numPages;
        this.tid = tid;
        isOpen = false;
    }

    public void open() throws TransactionAbortedException, DbException {
        pageNum = 0;
        isOpen = true;
        HeapPageId pageId = new HeapPageId(tableId, pageNum);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        it = heapPage.iterator();
    }

    public void close() {
        super.close();
        isOpen = false;
    }

    public void rewind() throws TransactionAbortedException, DbException {
        open();
    }

    public Tuple readNext() {
        if (!isOpen) {
            return null;
        }
        else if (it.hasNext()) {
            return it.next();
        }
        else if (pageNum < numPages - 1) {
            try {
                pageNum++;
                HeapPageId pageId = new HeapPageId(tableId, pageNum);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                it = heapPage.iterator();
                return it.next();
            }
            catch (TransactionAbortedException | DbException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            return null;
        }
    }
}
