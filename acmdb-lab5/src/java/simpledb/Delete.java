package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private DbIterator child;
    private int numDeletions;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] typeArr = {Type.INT_TYPE};
        return new TupleDesc(typeArr);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        numDeletions = 0;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        called = false;
        child.rewind();
        numDeletions = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (called) return null;

        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().deleteTuple(tid, tuple);
                numDeletions++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        called = true;
        Type[] typeArr = {Type.INT_TYPE};
        Tuple tuple = new Tuple(new TupleDesc(typeArr));
        tuple.setField(0, new IntField(numDeletions));
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = {child};
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
