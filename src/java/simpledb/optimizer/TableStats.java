package simpledb.optimizer;

import jdk.nashorn.internal.runtime.regexp.joni.constants.StringType;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>(); //相当于全局的私有的

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    } //根据tablename获取对应的元数据

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableId_;
    private HeapFile tableFile_;
    private TupleDesc tableDesc_;
    private int ioCostPerPage_;
    private int tupleNum_;
    private HashMap<Integer,Field> maxValues_;
    private HashMap<Integer,Field> minValues_;
    private HashMap<Integer,IntHistogram> intHisMap_;
    private HashMap<Integer,StringHistogram> strHisMap_;



    private void UpdateValuesMap(Tuple tuple) {
        for (int k = 0; k < tableDesc_.numFields() ; k ++) {
            Field field = tuple.getField(k);
            if (field.getType().equals(Type.INT_TYPE)) {
                if (field.compare(Predicate.Op.LESS_THAN,minValues_.get(k))) {
                    minValues_.put(k,field);
                }
                if (field.compare(Predicate.Op.GREATER_THAN,maxValues_.get(k))) {
                    maxValues_.put(k,field);
                }
            }
        }
    }

    private void InitHisMap() {
        for (int i = 0 ;i < tableDesc_.numFields(); i ++) {
            Type fieldType = tableDesc_.getFieldType(i);
            if (fieldType.equals(Type.INT_TYPE)) {
                IntField maxField = (IntField) maxValues_.get(i),minField = (IntField) minValues_.get(i);
                IntHistogram ih = new IntHistogram(NUM_HIST_BINS,minField.getValue(),maxField.getValue());
                intHisMap_.put(i,ih);
            } else if (fieldType.equals(Type.STRING_TYPE)) {
                strHisMap_.put(i,new StringHistogram(NUM_HIST_BINS));
            }
        }
    }

    private void AddTupleToHis(Tuple tuple) {
        for (int i = 0; i < tableDesc_.numFields() ; i ++) {
            Field field = tuple.getField(i);
            if (field.getType().equals(Type.INT_TYPE)) {
                IntField intField = (IntField) field;
                intHisMap_.get(i).addValue(intField.getValue());
            } else {
                StringField stringField = (StringField) field;
                strHisMap_.get(i).addValue(stringField.getValue());
            }
        }
    }

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        tupleNum_ = 0;
        tableId_ = tableid;
        minValues_ = new HashMap<Integer,Field>();
        maxValues_ = new HashMap<Integer,Field>();
        intHisMap_ = new HashMap<Integer,IntHistogram>();
        strHisMap_ = new HashMap<Integer,StringHistogram>();

        tableDesc_ = Database.getCatalog().getTupleDesc(tableid); //初始化Map

        for (int i = 0; i < tableDesc_.numFields() ; i ++) {
            Type fieldType = tableDesc_.getFieldType(i);
            if (fieldType.equals(Type.INT_TYPE)) {
                minValues_.put(i,new IntField(Integer.MAX_VALUE));
                maxValues_.put(i,new IntField(Integer.MIN_VALUE));
            } else {
                minValues_.put(i,new StringField(null,Type.STRING_LEN));
                minValues_.put(i,new StringField(null,Type.STRING_LEN));
            }
        }


        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        int pageNum = file.numPages();
        Iterator<Tuple> tupleIt;
        for(int i = 0; i < pageNum; i ++) {
            HeapPageId pageId = new HeapPageId(tableid,i);
            HeapPage page = (HeapPage) file.readPage(pageId);

            tupleIt = page.iterator();
            while (tupleIt.hasNext()) {
                tupleNum_ += 1;
                Tuple tuple = tupleIt.next();
                UpdateValuesMap(tuple);

            }
        }
        InitHisMap();
        ioCostPerPage_ = ioCostPerPage;
        tableFile_ = file;

        for(int i = 0; i < pageNum; i ++) {
            HeapPageId pageId = new HeapPageId(tableid,i);
            HeapPage page = (HeapPage) file.readPage(pageId);

            tupleIt = page.iterator();
            while (tupleIt.hasNext()) {
                Tuple tuple = tupleIt.next();
                AddTupleToHis(tuple);
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        //return 0;
        double pageNum = tableFile_.numPages();
        return 2.0 * pageNum * ioCostPerPage_;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        //return 0;
        return (int) (selectivityFactor * tupleNum_);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType().equals(Type.INT_TYPE)) {
            IntField intField = (IntField) constant;
            IntHistogram ih = intHisMap_.get(field);
            return ih.estimateSelectivity(op,intField.getValue());
        } else {
            StringField stringField = (StringField) constant;
            StringHistogram strh = strHisMap_.get(field);
            return strh.estimateSelectivity(op,stringField.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleNum_;


    }

}
