/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cascading.hbase.helper;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.StringUtils;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
public class TableInputFormat extends TableInputFormatBase implements
        JobConfigurable {
    private final Log LOG = LogFactory.getLog(TableInputFormat.class);

    /**
     * space delimited list of columns
     */
    public static final String COLUMN_LIST = "hbase.mapred.tablecolumns";

    /** Job parameter that specifies the input table. */
    public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";
    /** Base-64 encoded scanner. All other SCAN_ confs are ignored if this is specified.
     * See {@link org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil#convertScanToString(org.apache.hadoop.hbase.client.Scan)} for more details.
     */
    public static final String SCAN = "hbase.mapreduce.scan";
    /** Column Family to Scan */
    public static final String SCAN_COLUMN_FAMILY = "hbase.mapreduce.scan.column.family";
    /** Space delimited list of columns to scan. */
    public static final String SCAN_COLUMNS = "hbase.mapreduce.scan.columns";
    /** The timestamp used to filter columns with a specific timestamp. */
    public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";
    /** The starting timestamp used to filter columns with a specific range of versions. */
    public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";
    /** The ending timestamp used to filter columns with a specific range of versions. */
    public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";
    /** The maximum number of version to return. */
    public static final String SCAN_MAXVERSIONS = "hbase.mapreduce.scan.maxversions";
    /** Set to false to disable server-side caching of blocks for this scan. */
    public static final String SCAN_CACHEBLOCKS = "hbase.mapreduce.scan.cacheblocks";
    /** The number of rows for caching that will be passed to scanners. */
    public static final String SCAN_CACHEDROWS = "hbase.mapreduce.scan.cachedrows";

    public void configure(JobConf job) {
//        Path[] tableNames = FileInputFormat.getInputPaths(job);
//        String colArg = job.get(COLUMN_LIST);
//        String[] colNames = colArg.split(" ");
//        byte [][] m_cols = new byte[colNames.length][];
//        for (int i = 0; i < m_cols.length; i++) {
//            m_cols[i] = Bytes.toBytes(colNames[i]);
//        }
//        setInputColumns(m_cols);
//        try {
//            setHTable(new HTable(HBaseConfiguration.create(job), tableNames[0].getName()));
//        } catch (Exception e) {
//            LOG.error(StringUtils.stringifyException(e));
//        }

        //this.conf = configuration;
        String tableName = job.get(INPUT_TABLE);
        try {
            setHTable(new HTable(new Configuration(job), tableName));
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }

        Scan scan = null;

        if (job.get(SCAN) != null) {
            try {
                scan = convertStringToScan(job.get(SCAN));
            } catch (IOException e) {
                LOG.error("An error occurred.", e);
            }
        } else {
            try {
                scan = new Scan();

                if (job.get(SCAN_COLUMNS) != null) {
                    addColumns(scan, job.get(SCAN_COLUMNS));
                }

                if (job.get(SCAN_COLUMN_FAMILY) != null) {
                    scan.addFamily(Bytes.toBytes(job.get(SCAN_COLUMN_FAMILY)));
                }

                if (job.get(SCAN_TIMESTAMP) != null) {
                    scan.setTimeStamp(Long.parseLong(job.get(SCAN_TIMESTAMP)));
                }

                if (job.get(SCAN_TIMERANGE_START) != null && job.get(SCAN_TIMERANGE_END) != null) {
                    scan.setTimeRange(
                            Long.parseLong(job.get(SCAN_TIMERANGE_START)),
                            Long.parseLong(job.get(SCAN_TIMERANGE_END)));
                }

                if (job.get(SCAN_MAXVERSIONS) != null) {
                    scan.setMaxVersions(Integer.parseInt(job.get(SCAN_MAXVERSIONS)));
                }

                if (job.get(SCAN_CACHEDROWS) != null) {
                    scan.setCaching(Integer.parseInt(job.get(SCAN_CACHEDROWS)));
                }

                // false by default, full table scans generate too much BC churn
                scan.setCacheBlocks((job.getBoolean(SCAN_CACHEBLOCKS, false)));
            } catch (Exception e) {
                LOG.error(StringUtils.stringifyException(e));
            }
        }

        setScan(scan);
    }

    /**
     * Parses a combined family and qualifier and adds either both or just the
     * family in case there is not qualifier. This assumes the older colon
     * divided notation, e.g. "data:contents" or "meta:".
     * <p>
     * Note: It will through an error when the colon is missing.
     *
     * @param familyAndQualifier family and qualifier
     * @return A reference to this instance.
     * @throws IllegalArgumentException When the colon is missing.
     */
    private static void addColumn(Scan scan, byte[] familyAndQualifier) {
        byte [][] fq = KeyValue.parseColumn(familyAndQualifier);
        if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
            scan.addColumn(fq[0], fq[1]);
        } else {
            scan.addFamily(fq[0]);
        }
    }

    /**
     * Adds an array of columns specified using old format, family:qualifier.
     * <p>
     * Overrides previous calls to addFamily for any families in the input.
     *
     * @param columns array of columns, formatted as <pre>family:qualifier</pre>
     */
    public static void addColumns(Scan scan, byte [][] columns) {
        for (byte[] column : columns) {
            addColumn(scan, column);
        }
    }

    /**
     * Convenience method to help parse old style (or rather user entry on the
     * command line) column definitions, e.g. "data:contents mime:". The columns
     * must be space delimited and always have a colon (":") to denote family
     * and qualifier.
     *
     * @param columns  The columns to parse.
     * @return A reference to this instance.
     */
    private static void addColumns(Scan scan, String columns) {
        String[] cols = columns.split(" ");
        for (String col : cols) {
            addColumn(scan, Bytes.toBytes(col));
        }
    }

    public void validateInput(JobConf job) throws IOException {
        // expecting exactly one path
        Path [] tableNames = FileInputFormat.getInputPaths(job);
        if (tableNames == null || tableNames.length > 1) {
            throw new IOException("expecting one table name");
        }

        // connected to table?
        if (getHTable() == null) {
            throw new IOException("could not connect to table '" +
                    tableNames[0].getName() + "'");
        }

        // expecting at least one column
        String colArg = job.get(COLUMN_LIST);
        if (colArg == null || colArg.length() == 0) {
            throw new IOException("expecting at least one column");
        }
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan  The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    public static String convertScanToString(Scan scan) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        scan.write(dos);
        return Base64.encodeBytes(out.toByteArray());
    }

    /**
     * Converts the given Base64 string back into a Scan instance.
     *
     * @param base64  The scan details.
     * @return The newly created Scan instance.
     * @throws IOException When reading the scan instance fails.
     */
    public static Scan convertStringToScan(String base64) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(base64));
        DataInputStream dis = new DataInputStream(bis);
        Scan scan = new Scan();
        scan.readFields(dis);
        return scan;
    }
}
