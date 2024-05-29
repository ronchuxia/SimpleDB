package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final Aggregator aggregator;
    private DbIterator aggregatorIterator;
    private DbIterator child;
    private final int gfield;
    private final int afield;
    private final Aggregator.Op aop;
    private final Type gFieldType;
    private final Type aFieldType;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	    // some code goes here
        this.child = child;
        this.gfield = gfield;
        this.afield = afield;
        this.aop = aop;
        this.aFieldType = child.getTupleDesc().getFieldType(afield);
        if (gfield != Aggregator.NO_GROUPING) {
            this.gFieldType = child.getTupleDesc().getFieldType(gfield);
        }
        else {
            this.gFieldType = null;
        }

        if (aFieldType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, gFieldType, afield, aop);
        }
        else {
            this.aggregator = new StringAggregator(gfield, gFieldType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        // some code goes here
        if (gfield != Aggregator.NO_GROUPING) {
            return child.getTupleDesc().getFieldName(groupField());
        }
        else {
            return null;
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        super.open();
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggregatorIterator = aggregator.iterator();
        aggregatorIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (aggregatorIterator.hasNext()) {
            return aggregatorIterator.next();
        }
        else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // some code goes here
        aggregatorIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] typeArr;
        String[] nameArr;
        if (gfield == Aggregator.NO_GROUPING) {
            typeArr = new Type[]{aFieldType};
            nameArr = new String[]{aop.toString() + " " + aggregateFieldName()};
        } else {
            typeArr = new Type[]{gFieldType, aFieldType};
            nameArr = new String[]{groupFieldName(), aop.toString() + " " + aggregateFieldName()};
        }
        return new TupleDesc(typeArr, nameArr);
    }

    public void close() {
	    // some code goes here
        super.close();
        aggregatorIterator.close();
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
