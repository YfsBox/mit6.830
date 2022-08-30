package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
    private int gbfield_;
    private Type gbfieldtype_;
    private int afield_;
    private Op what_;
    private HashMap<Field,ArrayList<StringField>> GpMap_; //key为gbfield的值,value是afield的值
    private TupleDesc desc_;


    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what)  throws IllegalArgumentException{
        // some code goes here
        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException("the what not COUNT in StringAggregator");
        }
        gbfield_ = gbfield;
        gbfieldtype_ = gbfieldtype;
        afield_ = afield;
        what_ = what;
        desc_ = null;
        GpMap_ = new HashMap<Field,ArrayList<StringField>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    private void setTupleDesc(Tuple tup) {
        if (desc_ == null) {
            TupleDesc tupleDesc = tup.getTupleDesc();
            String[] names;
            Type[] types;
            if (gbfield_ < tupleDesc.numFields() && gbfield_ != -1) {
                names = new String[2];
                types = new Type[2];

                names[0] = tupleDesc.getFieldName(gbfield_);
                types[0] = tupleDesc.getFieldType(gbfield_);
                names[1] = null;
                types[1] = Type.INT_TYPE;
            } else {
                names = new String[1];
                types = new Type[1];

                names[0] = null;
                types[0] = Type.INT_TYPE;
            }
            desc_ = new TupleDesc(types,names);
        }
    }

    public void mergeTupleIntoGroup(Tuple tup) throws NoSuchElementException {
        // some code goes here
        setTupleDesc(tup);

        Field gpfield;
        StringField afield;
        if (gbfield_ != -1) {
            gpfield = tup.getField(gbfield_);
            if (gpfield == null) {
                throw new NoSuchElementException("not have the gbfield in tup from mergeTupleIntoGroup");
            }
        } else {
            gpfield = null;
        }
        afield = (StringField) tup.getField(afield_);

        if (!GpMap_.containsKey(gpfield)) {
            GpMap_.put(gpfield, new ArrayList<StringField>());
        }
        GpMap_.get(gpfield).add(afield);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
        return new StringAggregatorOpIterator(this);
    }



    public class StringAggregatorOpIterator implements OpIterator {
        private StringAggregator aggregator_;
        private Iterator<Field> gvalueIt_;
        private Field currGroup_;

        public StringAggregatorOpIterator(StringAggregator saggregator) {
            aggregator_ = saggregator;
            gvalueIt_ = null;
            currGroup_ = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            gvalueIt_ = aggregator_.GpMap_.keySet().iterator();
            currGroup_ = null;
        }

        @Override
        public void close() {
            currGroup_ = null;
            gvalueIt_ = null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tuple = null;
            tuple = new Tuple(aggregator_.desc_);
            currGroup_ = gvalueIt_.next();
            IntField intfield = new IntField(aggregator_.GpMap_.get(currGroup_).size());

            if (aggregator_.gbfield_ == -1 && aggregator_.GpMap_.containsKey(null)) { //这个时候就只会固定在第一个null为key的里面
                tuple.setField(0,intfield);
            } else {
                tuple.setField(0,currGroup_);
                tuple.setField(1,intfield);
            }
            return tuple;
        }




        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return gvalueIt_.hasNext();
        }
        @Override
        public TupleDesc getTupleDesc() {
            //return null;
            return aggregator_.desc_;
        }
    }

}
