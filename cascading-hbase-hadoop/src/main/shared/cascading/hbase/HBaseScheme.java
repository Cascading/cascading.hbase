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

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashSet;

/**
 * The HBaseScheme class is a {@link Scheme} subclass. It is used in conjunction
 * with the {@link HBaseTap} to allow for the reading and writing of data
 * to and from a HBase cluster.
 *
 * @see HBaseTap
 */
@SuppressWarnings("serial")
public class HBaseScheme extends HBaseAbstractScheme
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory
    .getLogger( HBaseScheme.class );

  /** String familyNames */
  private String[] familyNames;
  /** Field valueFields */
  private Fields[] valueFields;
  /** String columns */
  private transient String[] columns;
  /** Field fields */
  private transient byte[][] fields;
  /** byte[] startRow */
  private byte[] startRow;
  /** byte[] stopRow */
  private byte[] stopRow;

  private boolean isFullyQualified = false;

  /**
   * Constructor HBaseScheme creates a new HBaseScheme instance.
   *
   * @param keyFields   of type Fields
   * @param familyName  of type String
   * @param valueFields of type Fields
   */
  public HBaseScheme( Fields keyFields, String familyName, Fields valueFields )
    {
    this( keyFields, new String[]{familyName}, Fields.fields( valueFields ) );
    }

  /**
   * Constructor HBaseScheme creates a new HBaseScheme instance.
   *
   * @param keyFields   of type Fields
   * @param familyNames of type String[]
   * @param valueFields of type Fields[]
   */
  public HBaseScheme( Fields keyFields, String[] familyNames,
                      Fields[] valueFields )
    {
    this.keyField = keyFields;
    // The column Names only holds the family Names.
    this.familyNames = familyNames;
    this.valueFields = valueFields;

    setSourceSink( this.keyField, this.valueFields );

    validate();
    }

  /**
   * Constructor HBaseScheme creates a new HBaseScheme instance.
   *
   * @param keyFields   of type Fields
   * @param familyNames of type String[]
   * @param valueFields of type Fields[]
   * @param startRow of type byte[]
   */
  public HBaseScheme( Fields keyFields, String[] familyNames,
                      Fields[] valueFields, byte[] startRow )
    {
    this.keyField = keyFields;
    // The column Names only holds the family Names.
    this.familyNames = familyNames;
    this.valueFields = valueFields;
    this.startRow = startRow;

    setSourceSink( this.keyField, this.valueFields );

    validate();
    }

  /**
   * Constructor HBaseScheme creates a new HBaseScheme instance.
   *
   * @param keyFields   of type Fields
   * @param familyNames of type String[]
   * @param valueFields of type Fields[]
   * @param startRow of type byte[]
   * @param stopRow of type byte[]
   */
  public HBaseScheme( Fields keyFields, String[] familyNames,
                      Fields[] valueFields, byte[] startRow, byte[] stopRow )
    {
    this.keyField = keyFields;
    // The column Names only holds the family Names.
    this.familyNames = familyNames;
    this.valueFields = valueFields;
    this.startRow = startRow;
    this.stopRow = stopRow;

    setSourceSink( this.keyField, this.valueFields );

    validate();
    }

  /**
   * Method getFamilyNames returns the set of familyNames of this HBaseScheme
   * object.
   *
   * @return the familyNames (type String[]) of this HBaseScheme object.
   */
  public String[] getFamilyNames()
    {
    HashSet<String> familyNameSet = new HashSet<String>();
    for( String familyName : familyNames )
      {
      familyNameSet.add( familyName );
      }
    return familyNameSet.toArray( new String[ 0 ] );
    }

  @Override
  public boolean source( FlowProcess<JobConf> flowProcess,
                         SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {

    Object key = sourceCall.getContext()[ 0 ];
    Object value = sourceCall.getContext()[ 1 ];
    boolean hasNext = sourceCall.getInput().next( key, value );
    if( !hasNext )
      {
      return false;
      }

    Tuple result = sourceGetTuple( key );
    Result row = (Result) value;

    for( int i = 0; i < this.familyNames.length; i++ )
      {
      String familyName = this.familyNames[ i ];
      byte[] familyNameBytes = Bytes.toBytes( familyName );
      Fields fields = this.valueFields[ i ];
      for( int k = 0; k < fields.size(); k++ )
        {
        String fieldName = (String) fields.get( k );

        byte[] fieldNameBytes = Bytes.toBytes( fieldName );
        byte[] cellValue = row.getValue( familyNameBytes, fieldNameBytes );

        Object deserialized = Bytes.toString( cellValue );
        Type type = fields.getType( k );
        if( type instanceof CoercibleType )
          deserialized = ( (CoercibleType<?>) type ).canonical( deserialized );

        result.add( deserialized );
        }
      }
    sourceCall.getIncomingEntry().setTuple( result );

    return true;
    }


  @SuppressWarnings("unchecked")
  @Override
  public void sink( FlowProcess<JobConf> flowProcess,
                    SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {

    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    Put put = sinkGetPut( tupleEntry );

    for( int i = 0; i < valueFields.length; i++ )
      {
      Fields fieldSelector = valueFields[ i ];
      TupleEntry values = tupleEntry.selectEntry( fieldSelector );

      DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();

      for( int j = 0; j < values.getFields().size(); j++ )
        {
        String fieldName = values.getFields().get( j ).toString();
        Type fieldType = values.getFields().getType( j );
        Tuple tuple = values.getTuple();

        Object object = tuple.getObject( j );
        byte[] objectInBytes;

        if( object == null )
          objectInBytes = HConstants.EMPTY_BYTE_ARRAY;

        else if( object instanceof Writable )
          {
          Writable writable = (Writable) object;
          dataOutputBuffer.reset();
          writable.write( dataOutputBuffer );
          objectInBytes = new byte[ dataOutputBuffer.getLength() ];
          System.arraycopy( dataOutputBuffer.getData(), 0,
            objectInBytes, 0, dataOutputBuffer.getLength() );
          }
        else if( fieldType instanceof CoercibleType )
          {
          CoercibleType<?> coercible = (CoercibleType<?>) fieldType;
          objectInBytes = Bytes.toBytes( coercible.coerce( object, String.class ).toString() );
          }
        else
          {
          objectInBytes = Bytes.toBytes( object.toString() );
          }
        put.add( Bytes.toBytes( familyNames[ i ] ),
          Bytes.toBytes( fieldName ), objectInBytes );
        }
      }

    OutputCollector collector = sinkCall.getOutput();
    collector.collect( null, put );
    }

  @Override
  public void sinkConfInit( FlowProcess<JobConf> flowProcess,
                            Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
    setSinkInitFields( conf );
    }

  @Override
  public void sourceConfInit( FlowProcess<JobConf> flowProcess,
                              Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
    // conf.setInputFormatClass(TableInputFormat.class);
    String columns = getColumns();
    setSourceInitFields( conf, columns );
    LOG.debug( "sourcing from columns: {}", columns );

    if (startRow != null)
        conf.set(TableInputFormat.SCAN_ROW_START, Bytes.toHex(startRow));

    if (stopRow != null)
        conf.set(TableInputFormat.SCAN_ROW_STOP, Bytes.toHex(stopRow));

    }

  private String getColumns()
    {
    return Util.join( columns( this.familyNames, this.valueFields ), " " );
    }

  private String[] columns( String[] familyNames, Fields[] fieldsArray )
    {
    if( columns != null )
      return columns;

    int size = 0;

    for( Fields fields : fieldsArray )
      size += fields.size();

    columns = new String[ size ];

    for( int i = 0; i < fieldsArray.length; i++ )
      {
      Fields fields = fieldsArray[ i ];

      for( int j = 0; j < fields.size(); j++ )
        if( isFullyQualified )
          columns[ i + j ] = hbaseColumn( (String) fields.get( j ) );
        else
          columns[ i + j ] = hbaseColumn( familyNames[ i ] )
            + (String) fields.get( j );
      }

    return columns;
    }

  private byte[][] makeBytes( String[] familyNames, Fields[] fieldsArray )
    {
    String[] columns = columns( familyNames, fieldsArray );
    byte[][] bytes = new byte[ columns.length ][];

    for( int i = 0; i < columns.length; i++ )
      bytes[ i ] = Bytes.toBytes( columns[ i ] );

    return bytes;
    }

  private String hbaseColumn( String column )
    {
    if( column.indexOf( ":" ) < 0 )
      return column + ":";
    return column;

    }

  }
