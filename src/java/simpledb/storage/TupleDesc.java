package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
//这个类也就是域描述符
public class TupleDesc implements Serializable { //这个函数中目前来看几乎都是get类型的函数

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable { //其中的内部类,主要用来表示内部类有fildType，name

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType; //final表示的是第一次被初始化就不应该被改变了,类似于java中的const
        //这里的Type是一个enum
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    //上面的这个类主要用来描述一个Item，其中的filedName以及对应的数值类型。

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    //返回的是，这里面有多个field，每一个field都会有一个对应的TDItem，返回这些TDItem的迭代器
    public Iterator<TDItem> iterator() { //属于一个Get型的代码
        // some code goes here
        //return null;
        return ItermList_.iterator();
    }

    private static final long serialVersionUID = 1L;
    private ArrayList<TDItem> ItermList_;
    private int fieldNum_;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) { //构造函数
        // some code goes here
        int len = fieldAr.length;
        for (int i = 0; i < len; i ++) {
            TDItem newItem = new TDItem(typeAr[i],fieldAr[i]);
            ItermList_.add(newItem);//加入新的Item
        }
        fieldNum_ = len;
        PrintItemForTest();

    }
    public void PrintItemForTest() {
        for (TDItem item:ItermList_) {
            System.out.println(item.toString());
        }
    }
    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) { //这个构造函数所创建的没有名字
        // some code goes here
        int len = typeAr.length;
        for (int i = 0; i < len; i ++ ) {
            TDItem newItem = new TDItem(typeAr[i],null);
            ItermList_.add(newItem);
        }
        fieldNum_ = len;
        PrintItemForTest();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() { //返回这个TupleDesc中有多少个fields
        // some code goes here
        return fieldNum_; //返回数量
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        if(i >= fieldNum_) {
            throw new NoSuchElementException("out of the fieldNum in getFieldType");
        } else {
            return ItermList_.get(i).fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        //return null;
        if(i >= fieldNum_) {
            //return null; //也就是越界的情况
            throw new NoSuchElementException("out of the fieldNum in getFieldType");
        } else {
            return ItermList_.get(i).fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException { //这种跟有throws函数的,需要在调用时跟上有关于异常的处理与判断
        // some code goes here
        for (int i = 0; i < fieldNum_;i ++ ) {
            TDItem item = ItermList_.get(i);
            if (item.fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("No Such Element in fieldNameToIndex");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return 0;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) { //合并函数
        // some code goes here
        return null;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
