/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.hbase;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

import cascading.hbase.helper.TableInputFormat;

import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * The HBaseTap class is a {@link Tap} subclass. It is used in conjunction with
 * the {@link HBaseScheme} to allow for the reading and writing
 * of data to and from a HBase cluster.
 */
@SuppressWarnings("serial")
public class HBaseTap extends Tap<JobConf, RecordReader, OutputCollector> {
	/** Field LOG */
	private static final Logger LOG = LoggerFactory.getLogger(HBaseTap.class);

	/** Field SCHEME */
	public static final String SCHEME = "hbase";

	/** Field hBaseAdmin */
	private transient HBaseAdmin hBaseAdmin;

	private final String id = UUID.randomUUID().toString();

	private String tableName;

	private int uniqueId;
	
	/**
	 * Constructor HBaseTap creates a new HBaseTap instance.
	 *
	 * @param tableName
	 *            of type String
	 * @param HBaseFullScheme
	 *            of type HBaseFullScheme
	 */
	public HBaseTap(String tableName, HBaseAbstractScheme HBaseFullScheme) {
		this(tableName, HBaseFullScheme, SinkMode.KEEP);
	}

	/**
	 * Instantiates a new h base tap.
	 *
	 * @param tableName
	 *            the table name
	 * @param HBaseFullScheme
	 *            the h base full scheme
	 * @param uniqueId
	 *            the uniqueId (0 if no id given)
	 */
	public HBaseTap(String tableName, HBaseAbstractScheme HBaseFullScheme,
			int uniqueId) {
		this(tableName, HBaseFullScheme, SinkMode.KEEP, uniqueId);
	}

	/**
	 * Instantiates a new h base tap.
	 *
	 * @param tableName
	 *            the table name
	 * @param HBaseFullScheme
	 *            the h base full scheme
	 * @param sinkMode
	 *            the sink mode
	 */
	public HBaseTap(String tableName, HBaseAbstractScheme HBaseFullScheme,
			SinkMode sinkMode) {
		this(tableName, HBaseFullScheme, sinkMode, 0);
	}

	 /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   * 
   * @param tableName
   *            of type String
   * @param HBaseFullScheme
   *            of type HBaseFullScheme
   * @param sinkMode
   *            of type SinkMode
   * @param uniqueId
   *            the uniqueId (0 if no id given)
   */
  public HBaseTap(String tableName, HBaseAbstractScheme HBaseFullScheme,
      SinkMode sinkMode, int uniqueId) {
    super(HBaseFullScheme, sinkMode);
    this.tableName = tableName;
    this.uniqueId = uniqueId;
  }
	
	
	public Path getPath() {
		return new Path(SCHEME + "://" + tableName.replaceAll(":", "_"));
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
			RecordReader input) throws IOException {
		return new HadoopTupleEntrySchemeIterator(flowProcess, this, input);
	}

	@Override
	public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess,
			OutputCollector output) throws IOException {
		HBaseTapCollector hBaseCollector = new HBaseTapCollector(flowProcess,
				this);
		hBaseCollector.prepare();
		return hBaseCollector;
	}

	private HBaseAdmin getHBaseAdmin(JobConf conf)
			throws MasterNotRunningException, ZooKeeperConnectionException {
		Thread.currentThread().setContextClassLoader(HBaseConfiguration.class.getClassLoader());
		if (hBaseAdmin == null)
			hBaseAdmin = new HBaseAdmin(HBaseConfiguration.create(conf));
		return hBaseAdmin;
	}

    private void obtainToken(JobConf conf) {
        if (User.isHBaseSecurityEnabled(conf)) {
            String user = conf.getUser();
            LOG.info("obtaining HBase token for: {}", user);
            try {
                UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                user = currentUser.getUserName();
                Credentials credentials = conf.getCredentials();
                for(Token t : currentUser.getTokens()) {
                    LOG.debug("Token {} is available", t);
                    //there must be HBASE_AUTH_TOKEN exists, if not bad thing will happen, it's must be generated when job submission.
                    if ("HBASE_AUTH_TOKEN".equalsIgnoreCase(t.getKind().toString())) {
                        credentials.addToken(t.getKind(), t);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Unable to obtain HBase auth token for " + user, e);
            }
        }
    }


	public boolean resourceExists(JobConf conf) throws IOException {
		return getHBaseAdmin(conf).tableExists(tableName);
	}

	public long getModifiedTime(JobConf conf) throws IOException {
		return System.currentTimeMillis(); // currently unable to find last mod
											// time on a table
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
		LOG.debug("sinking to table: {}", tableName);
		// TODO: next 5 lines were added, and commented area was taken out
		// hbase table wasn't being created during tests... wtf?
        obtainToken(conf);
		try {
            createResource(conf);
		} catch (IOException e) {
			throw new RuntimeException(tableName + " does not exist !");
		}

		// // do not delete if initialized from within a task
		// if (isReplace() && conf.get("mapred.task.partition") == null) {
		// try {
		// deleteResource(conf);
		// } catch (IOException e) {
		// throw new RuntimeException("could not delete resource: " + e);
		// }
		// } else if( isUpdate() ) {
		// try {
		// createResource(conf);
		// } catch (IOException e) {
		// throw new RuntimeException(tableName + " does not exist !");
		// }
		// }

		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		super.sinkConfInit(flowProcess, conf);
	}

	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
		LOG.debug("sourcing from table: {}", tableName);
		FileInputFormat.addInputPaths(conf, tableName);
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        obtainToken(conf);
		super.sourceConfInit(flowProcess, conf);
	}

	@Override
	public boolean equals(Object object) {

		if (object == null)
			return false;
		if (this == object)
			return true;
		if (!(object instanceof HBaseTap))
			return false;
		if (!super.equals(object))
			return false;

		HBaseTap tap = (HBaseTap) object;

		if (tableName == null ? tap.tableName != null : !tableName
				.equals(tap.tableName))
			return false;

		return uniqueId == tap.uniqueId;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (tableName == null ? 0 : tableName.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return getPath().toString();
	}

	@Override
	public boolean createResource(JobConf conf) throws IOException {

		HBaseAdmin hBaseAdmin = getHBaseAdmin(conf);

		if (hBaseAdmin.tableExists(tableName)) {
			return true;
		}

		LOG.info("creating hbase table: {}", tableName);

		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

		String[] familyNames = ((HBaseAbstractScheme) getScheme())
				.getFamilyNames();

		for (String familyName : familyNames) {
			tableDescriptor.addFamily(new HColumnDescriptor(familyName));
		}

		hBaseAdmin.createTable(tableDescriptor);

		return true;
	}

	@Override
	public boolean deleteResource(JobConf conf) throws IOException {
		try {
			// eventually keep table meta-data to source table create
			HBaseAdmin hBaseAdmin = getHBaseAdmin(conf);

			if (!hBaseAdmin.tableExists(tableName))
				return true;

			LOG.debug("deleting hbase table: {}", tableName);

			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);

			return true;

		} catch (Exception e) {
		  LOG.error( "error while deleting table {} {}", tableName, e );
			return false;
		}
	}

	@Override
	public String getIdentifier() {
		return id;
	}

}
