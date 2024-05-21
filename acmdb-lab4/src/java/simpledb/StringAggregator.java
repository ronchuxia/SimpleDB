package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Op op;
    private HashMap<Field, IntField> groupToValue;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.groupToValue = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field group = null;
        if (gbField != NO_GROUPING) {
            group = tup.getField(gbField);
        }

        if (groupToValue.containsKey(group)) {  // If the group value has been encountered, merge a new tuple into the aggregate
            IntField oldValue = groupToValue.get(group);
            IntField value = new IntField(oldValue.getValue() + 1);
            groupToValue.put(group, value);
        }
        else {  // If the group value has not been encountered, create a new group aggregate result
            IntField value = new IntField(1);
            groupToValue.put(group, value);
        }
    }

    private class StringAggregatorIterator implements DbIterator {
        private final List list = new ArrayList<Tuple>();
        private Iterator iterator;

        @Override
        public TupleDesc getTupleDesc() {
            Type[] typeArr;
            if (gbField == NO_GROUPING) {
                typeArr = new Type[]{Type.INT_TYPE};
            } else {
                typeArr = new Type[]{gbFieldType, Type.INT_TYPE};
            }
            return new TupleDesc(typeArr);
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (gbField == NO_GROUPING) {
                Tuple tuple = new Tuple(getTupleDesc());
                tuple.setField(0, groupToValue.get(null));
                list.add(tuple);
            } else {
                for (HashMap.Entry<Field, IntField> entry : groupToValue.entrySet()) {
                    Tuple tuple = new Tuple(getTupleDesc());
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, entry.getValue());
                    list.add(tuple);
                }
            }
            iterator = list.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return (Tuple) iterator.next();
        }

        @Override
        public void close() {
            list.clear();
            iterator = null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = list.iterator();
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new StringAggregatorIterator();
    }
}
