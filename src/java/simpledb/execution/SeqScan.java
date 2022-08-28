package simpledb.execution;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private DbFile dbFile_;
    private DbFileIterator Iterator_;
    private TransactionId tid_;
    private int tableId_;
    private String tableAlias_;
    private TupleDesc tupleDesc_;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        tid_ = tid;
        tableId_ = tableid;
        tableAlias_ = tableAlias;
        dbFile_ = Database.getCatalog().getDatabaseFile(tableId_);
        Iterator_ = dbFile_.iterator(tid_);

        TupleDesc tmpdesc = Database.getCatalog().getTupleDesc(tableId_);
        Iterator<TupleDesc.TDItem> fieldIterator = tmpdesc.iterator();

        Type[] types = new Type[tmpdesc.numFields()];
        String[] names = new String[tmpdesc.numFields()];

        int index = 0;
        while(fieldIterator.hasNext()) {
            TupleDesc.TDItem item = fieldIterator.next();
            types[index] = item.fieldType;
            names[index] = String.format("%s.%s",tableAlias_,item.fieldName);
            index ++;
        }
        tupleDesc_ = new TupleDesc(types,names);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId_);
        //return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // some code goes here
        return tableAlias_;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        tableId_ = tableid;
        tableAlias_ = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        //DbFileIterator dbFileIt = dbFile_.iterator(tid_);
        Iterator_.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc_;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return Iterator_.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return Iterator_.next();
    }

    public void close() {
        // some code goes here
        Iterator_.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        Iterator_.rewind();
    }
}
