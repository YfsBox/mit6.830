package simpledb.storage;

import simpledb.common.Type;

import javax.print.DocFlavor;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable { //表明Tuple元组是可以进行序列化的,在这里只起到一种标识作用,内容没用上。

    private static final long serialVersionUID = 1L;
    private static final int INTFIELD_INIT_VALUE = 0;
    private RecordId recordId_;
    private TupleDesc desc_;
    private ArrayList<Field> fields_;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        int len = td.numFields();
        desc_ = new TupleDesc(td); //借助拷贝构造函数实现深拷贝
        fields_ = new ArrayList<Field>(len);
        for (int i = 0; i < len ; i ++) {
            fields_.add(null); //初始化动态数组
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return desc_;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId_;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId_ = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i >= desc_.numFields()) {
            return;
        }
        fields_.set(i,f); //这里没有实现深拷贝不会不会出问题
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i >= fields_.size()) {
            return null;
        }
        return fields_.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        int len = fields_.size();
        for (int i = 0; i < len; i ++) {
            if (i != len - 1) {
                sb.append(fields_.get(i)).append('\t');
            } else {
                sb.append(fields_.get(i));
            }
        }
        return sb.toString();
        //throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        //return null;
        return fields_.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        desc_ = new TupleDesc(td); //重新的深拷贝
    }
}
