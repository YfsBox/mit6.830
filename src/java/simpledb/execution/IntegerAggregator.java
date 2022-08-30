package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

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

    private int gbfield_;
    private Type gbfieldtype_;
    private int afield_;
    private Op what_;
    private HashMap<Field,ArrayList<IntField>> GpMap_; //key为gbfield的值,value是afield的值
    private TupleDesc desc_;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbfield_ = gbfield;
        gbfieldtype_ = gbfieldtype;
        afield_ = afield;
        what_ = what;
        desc_ = null;
        GpMap_ = new HashMap<Field,ArrayList<IntField>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
                names[1] = tupleDesc.getFieldName(afield_);
                types[1] = tupleDesc.getFieldType(afield_);
            } else {
                names = new String[1];
                types = new Type[1];

                names[0] = tupleDesc.getFieldName(afield_);
                types[0] = tupleDesc.getFieldType(afield_);
            }
            desc_ = new TupleDesc(types,names);
        }
    }
    //当noGroup的时候就是只有一个null,以及对应的一个list.
    //如果不是则每一个feild就对应一个list
    private void updateValue(Field key,IntField value) {
        if (!GpMap_.containsKey(key)) {
            GpMap_.put(key, new ArrayList<IntField>());
        }
        GpMap_.get(key).add(value);
    }

    public void mergeTupleIntoGroup(Tuple tup) throws NoSuchElementException{
        // some code goes here
        setTupleDesc(tup);
        //在下面应该区分是否是-1的情况
        Field gpfield;
        IntField afield;
        if (gbfield_ != -1) {
            gpfield = tup.getField(gbfield_);
            if (gpfield == null) { //关于越界的情况考不考虑??
                throw new NoSuchElementException("not have the gbfield in tup from mergeTupleIntoGroup");
            }
        } else {
            gpfield = null;//这种情况对应的是nogroup的情况
        }
        afield = (IntField) tup.getField(afield_);
        updateValue(gpfield,afield);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
        return new IntegerAggregatorOpIterator(this);
    }


    public class IntegerAggregatorOpIterator implements OpIterator {

        private IntegerAggregator aggregator_;
        private Iterator<IntField> avalueIt_;
        private Iterator<Field> gvalueIt_;
        private Field currGroup_;



        public IntegerAggregatorOpIterator(IntegerAggregator iaggregator) {
            aggregator_ = iaggregator;
            avalueIt_ = null;
            gvalueIt_ = null;
            currGroup_ = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            gvalueIt_ = aggregator_.GpMap_.keySet().iterator();
            currGroup_ = null;
            avalueIt_ = null;
        }

        @Override
        public void close() {
            currGroup_ = null;
            gvalueIt_ = null;
            avalueIt_ = null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }



        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (aggregator_.gbfield_ == -1 && aggregator_.GpMap_.containsKey(null)) { //这个时候就只会固定在第一个null为key的里面
                if (avalueIt_ == null) {
                    avalueIt_ = aggregator_.GpMap_.get(null).iterator();
                }
                IntField intfield = avalueIt_.next(); //有hasnext保证下执行
                Tuple tuple = new Tuple(aggregator_.desc_); //肯定是1个的
                tuple.setField(0,intfield);
                return tuple;
            } else {
                IntField intField;
                Tuple tuple = new Tuple(aggregator_.desc_);
                if (currGroup_ == null && avalueIt_ == null) {
                    currGroup_ = gvalueIt_.next();
                    avalueIt_ = aggregator_.GpMap_.get(currGroup_).iterator();
                    intField = avalueIt_.next();
                } else {
                    if (avalueIt_.hasNext()) {
                        intField = avalueIt_.next();
                    } else {
                        currGroup_ = gvalueIt_.next(); //调用next的前提是hasnext
                        avalueIt_ = aggregator_.GpMap_.get(currGroup_).iterator();
                        intField = avalueIt_.next();
                    }
                }
                tuple.setField(0,currGroup_);
                tuple.setField(1,intField);
                return tuple;
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (aggregator_.gbfield_ == -1) {
                if (avalueIt_ == null) {
                    return aggregator_.GpMap_.containsKey(null);
                } else {
                    return avalueIt_.hasNext();
                }
            } else {
                if (currGroup_ == null && avalueIt_ == null) {
                    return gvalueIt_.hasNext();
                } else {
                    if (avalueIt_.hasNext()) {
                        return true;
                    } else {
                        return gvalueIt_.hasNext();
                    }
                }
            }
        }
        @Override
        public TupleDesc getTupleDesc() {
            //return null;
            return aggregator_.desc_;
        }
    }

}
