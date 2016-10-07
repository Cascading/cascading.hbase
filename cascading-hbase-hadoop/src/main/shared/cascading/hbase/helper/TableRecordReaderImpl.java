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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.util.StringUtils;


/**
 * Iterate over an HBase table data, return (Text, RowResult) pairs
 */
public class TableRecordReaderImpl
  {
  static final Log LOG = LogFactory.getLog( TableRecordReaderImpl.class );

  private byte[] lastSuccessfulRow;
  private ResultScanner scanner;
  private Scan scan = null;
  private HTable htable;
  static final int MAX_RETRIES = 10;

  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow
   * @throws IOException
   */
  public void restart( byte[] firstRow ) throws IOException
    {
    Scan newScan = new Scan( scan );
    newScan.setStartRow( firstRow );
    this.scanner = this.htable.getScanner( newScan );
    }

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   *
   * @throws IOException
   */
  public void init() throws IOException
    {
    restart( scan.getStartRow() );
    }

  /**
   * @param htable the {@link HTable} to scan.
   */
  public void setHTable( HTable htable )
    {
    this.htable = htable;
    }

  /**
   * Sets the scan defining the actual details like columns etc.
   *
   * @param scan The scan to set.
   */
  public void setScan( Scan scan )
    {
    this.scan = scan;
    }

  public void close()
    {
    this.scanner.close();
    }

  /**
   * @return ImmutableBytesWritable
   * @see org.apache.hadoop.mapred.RecordReader#createKey()
   */
  public ImmutableBytesWritable createKey()
    {
    return new ImmutableBytesWritable();
    }

  /**
   * @return RowResult
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  public Result createValue()
    {
    return new Result();
    }

  public long getPos()
    {
    // This should be the ordinal tuple in the range;
    // not clear how to calculate...
    return 0;
    }

  public float getProgress()
    {
    // Depends on the total number of tuples and getPos
    return 0;
    }

  /**
   * @param key   HStoreKey as input key.
   * @param value MapWritable as input value
   * @return true if there was more data
   * @throws IOException
   */
  public boolean next( ImmutableBytesWritable key, Result value )
    throws IOException
  {
    return next(key, value, 1);
  }

  private boolean next( ImmutableBytesWritable key, Result value, int retryLevel )
            throws IOException
    {
    Result result;
    IOException tryRecovery = null;

    try
      {
        result = this.scanner.next();
        if( result != null && result.size() > 0 )
        {
          key.set(result.getRow());
          lastSuccessfulRow = key.get();
          value.copyFrom(result);
          return true;
        }
        return false;
      }
    catch( DoNotRetryIOException e )
      {
        if ((e.getCause() != null) &&
            (OutOfOrderScannerNextException.class.isAssignableFrom(e.getCause().getClass())) &&
            (retryLevel < MAX_RETRIES))
        {
          /* this is a DoNotRetry, because you should not retry the failing RPC >on the same scanner<; likely, the
            * structure of regions has changed (splits or merges?).
            *
            * Re-attempting on a new scanner, which restart() provides, should be fine. */
          LOG.warn( "recovering from out-of-order exception " + StringUtils.stringifyException( e ) + "(attempt #" + retryLevel+"/" + MAX_RETRIES+")" );
          tryRecovery = e;
        }
      else
        {
          throw e;
        }
      }
    catch( IOException e )
      {
      LOG.debug( "recovering from " + StringUtils.stringifyException( e ) + "(attempt #" + retryLevel+"/" + MAX_RETRIES+")" );
      if( lastSuccessfulRow == null )
        {
        LOG.warn( "We are restarting the first next() invocation," +
          " if your mapper's restarted a few other times like this" +
          " then you should consider killing this job and investigate" +
          " why it's taking so long." );
        }
        tryRecovery = e;
      }

   if (tryRecovery != null)
    {
      if( lastSuccessfulRow == null )
        restart( scan.getStartRow() );
      else
        {
        restart( lastSuccessfulRow );
        this.scanner.next();    // skip presumed already mapped row
        }

      if (retryLevel > MAX_RETRIES)
        {
          LOG.error( "maximum retry count without progress reached, failing I/O " + StringUtils.stringifyException( tryRecovery ));
          throw tryRecovery;
        }
      return next (key, value, retryLevel + 1);
    }



    return false;
    }
  }

