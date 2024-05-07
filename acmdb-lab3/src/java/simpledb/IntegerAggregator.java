package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Op op;
    private HashMap<Field, IntField> groupToValue;
    private HashMap<Field, IntField> groupToCount;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.groupToValue = new HashMap<>();
        this.groupToCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field group = null;
        if (gbField != NO_GROUPING) {
            group = tup.getField(gbField);
        }

        if (groupToValue.containsKey(group)) {  // If the group value has been encountered, merge a new tuple into the aggregate
            // Modify value
            if (op == Op.COUNT) {
                IntField oldValue = groupToValue.get(group);
                IntField value = new IntField(oldValue.getValue() + 1);
                groupToValue.put(group, value);
            }

            if (op == Op.MAX) {
                IntField oldValue = groupToValue.get(group);
                IntField newValue = (IntField) tup.getField(aField);
                IntField value = new IntField(Math.max(oldValue.getValue(), newValue.getValue()));
                groupToValue.put(group, value);
            }

            if (op == Op.MIN) {
                IntField oldValue = groupToValue.get(group);
                IntField newValue = (IntField) tup.getField(aField);
                IntField value = new IntField(Math.min(oldValue.getValue(), newValue.getValue()));
                groupToValue.put(group, value);
            }

            if (op == Op.SUM || op == Op.AVG) {
                IntField oldValue = groupToValue.get(group);
                IntField newValue = (IntField) tup.getField(aField);
                IntField value = new IntField(oldValue.getValue() + newValue.getValue());
                groupToValue.put(group, value);
            }

            // Modify count
            IntField oldCount = groupToCount.get(group);
            IntField count = new IntField(oldCount.getValue() + 1);
            groupToCount.put(group, count);
        }
        else {  // If the group value has not been encountered, create a new group aggregate result
            IntField value = (IntField) tup.getField(aField);
            IntField count = new IntField(1);
            // Add value
            if (op == Op.COUNT) {
                groupToValue.put(group, count);
            }
            if (op == Op.MAX || op == Op.MIN || op == Op.SUM || op == Op.AVG) {
                groupToValue.put(group, value);
            }
            // Add count
            groupToCount.put(group, count);
        }
    }

    private class IntegerAggregatorIterator implements DbIterator {
        private final List list = new ArrayList<Tuple>();
        private Iterator iterator;

        @Override
        public TupleDesc getTupleDesc() {
            Type[] typeArr;
            if (gbField == NO_GROUPING) {
                typeArr = new Type[]{Type.INT_TYPE};
            }
            else {
                typeArr = new Type[]{gbFieldType, Type.INT_TYPE};
            }
            return new TupleDesc(typeArr);
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (gbField == NO_GROUPING) {
                Tuple tuple = new Tuple(getTupleDesc());
                if (op != Op.AVG) {
                    tuple.setField(0, groupToValue.get(null));
                }
                else {
                    Field field = new IntField(groupToValue.get(null).getValue() / groupToCount.get(null).getValue());
                    tuple.setField(0, field);
                }
                list.add(tuple);
            }
            else {
                for (HashMap.Entry<Field, IntField> entry : groupToValue.entrySet()) {
                    Tuple tuple = new Tuple(getTupleDesc());
                    tuple.setField(0, entry.getKey());
                    if (op != Op.AVG) {
                        tuple.setField(1, entry.getValue());
                    }
                    else {
                        Field field = new IntField(entry.getValue().getValue() / groupToCount.get(entry.getKey()).getValue());
                        tuple.setField(1, field);
                    }
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new IntegerAggregatorIterator();
    }

}
