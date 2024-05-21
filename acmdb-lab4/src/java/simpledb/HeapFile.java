package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.*;
import java.nio.channels.FileChannel;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
        tableId = file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNum = pid.pageNumber();
        int pageSize = BufferPool.getPageSize();

        int offset = pageNum * pageSize;
        byte[] data = new byte[pageSize];

        try {
            FileInputStream pageFile = new FileInputStream(file);
            pageFile.skip(offset);
            pageFile.read(data);
            pageFile.close();
            HeapPageId pageId = new HeapPageId(pid.getTableId(), pageNum);
            return new HeapPage(pageId, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = page.getId();
        int pageNum = pageId.pageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageNum * pageSize;

        byte[] data = page.getPageData();

        try {
            FileOutputStream pageFile = new FileOutputStream(file);
            FileChannel fileChannel = pageFile.getChannel();
            fileChannel.position(offset);
            fileChannel.write(ByteBuffer.wrap(data));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int numPages = (int) file.length() / BufferPool.getPageSize();
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int numPages = (int) file.length() / BufferPool.getPageSize();
        ArrayList<Page> dirtyPages = new ArrayList<>();
        for (int pageNum = 0; pageNum < numPages; ++pageNum) {
            HeapPageId pageId = new HeapPageId(tableId, pageNum);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                dirtyPages.add(page);
                return dirtyPages;
            }
        }
        // No empty file, create a new page and write it to disk
        HeapPageId newPageId = new HeapPageId(tableId, numPages);
        HeapPage newPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        writePage(newPage);
        dirtyPages.add(newPage);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        ArrayList<Page> dirtyPages = new ArrayList<>();
        HeapPage dirtyPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        dirtyPage.deleteTuple(t);
        dirtyPages.add(dirtyPage);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        int numPages = (int) file.length() / BufferPool.getPageSize();
        return new HeapFileIterator(tableId, numPages, tid);
    }
}

