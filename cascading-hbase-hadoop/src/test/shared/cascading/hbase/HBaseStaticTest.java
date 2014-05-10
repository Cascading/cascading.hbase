package cascading.hbase;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

@SuppressWarnings("rawtypes")
public class HBaseStaticTest extends HBaseTestsStaticScheme
  {

  String inputFile = "src/test/shared-resources/data/small.txt";

  @AfterClass
  public static void tearDown() throws IOException
    {
    FileUtils.deleteDirectory( new File( "build/test/output" ) );
    }

  @Test
  public void testHBaseMultiFamily() throws IOException
    {

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, HBaseStaticTest.class );

    deleteTable( configuration, "multitable" );


    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFile );

    Pipe parsePipe = new Pipe( "parse" );
    parsePipe = new Each( parsePipe, new Fields( "line" ), new RegexSplitter(
      new Fields( "num", "lower", "upper" ), " " ) );

    Fields keyFields = new Fields( "num" );
    String[] familyNames = {"left", "right"};
    Fields[] valueFields = new Fields[]{new Fields( "lower" ),
                                        new Fields( "upper" )};
    Tap hBaseTap = new HBaseTap( "multitable", new HBaseScheme( keyFields,
      familyNames, valueFields ), SinkMode.REPLACE );

    FlowConnector flowConnector = createHadoopFlowConnector();
    Flow parseFlow = flowConnector.connect( source, hBaseTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 5 );

    // create flow to read from hbase and save to local file
    Tap sink = new Lfs( new TextLine(), "build/test/output/multifamily",
      SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = flowConnector.connect( hBaseTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 5 );

    }

  @Test
  public void testHBaseRangeScan() throws IOException
  {

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, HBaseStaticTest.class );

    deleteTable( configuration, "multitable" );


    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFile );

    Pipe parsePipe = new Pipe( "parse" );
    parsePipe = new Each( parsePipe, new Fields( "line" ), new RegexSplitter(
            new Fields( "num", "lower", "upper" ), " " ) );

    Fields keyFields = new Fields( "num" );
    String[] familyNames = {"left", "right"};
    Fields[] valueFields = new Fields[]{new Fields( "lower" ),
              new Fields( "upper" )};
    Tap hBaseTap = new HBaseTap( "multitable", new HBaseScheme( keyFields,
              familyNames, valueFields ), SinkMode.REPLACE );

    FlowConnector flowConnector = createHadoopFlowConnector();
    Flow parseFlow = flowConnector.connect( source, hBaseTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 5 );

    // create flow to read from hbase and save to local file
    hBaseTap = new HBaseTap( "multitable", new HBaseScheme( keyFields,
            familyNames, valueFields, Bytes.toBytes("2")));

    Tap sink = new Lfs( new TextLine(), "build/test/output/multifamily",
            SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = flowConnector.connect( hBaseTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 4 );

    hBaseTap = new HBaseTap( "multitable", new HBaseScheme( keyFields,
            familyNames, valueFields, Bytes.toBytes("2"), Bytes.toBytes("5")));

    copyFlow = flowConnector.connect( hBaseTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 3 );
  }


  @Test
  public void testHBaseMultiFamilyCascade() throws IOException
    {

    deleteTable( configuration, "multitable" );

    String inputFile = "src/test/shared-resources/data/small.txt";
    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFile );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ),
      new RegexSplitter( new Fields( "ignore", "lower", "upper" ), " " ) );
    parsePipe = new Each( parsePipe,
      new ExpressionFunction( new Fields( "num" ),
        "(int) (Math.random() * Integer.MAX_VALUE)" ),
      Fields.ALL );

    Fields keyFields = new Fields( "num" );
    String[] familyNames = {"left", "right"};
    Fields[] valueFields = new Fields[]{new Fields( "lower" ),
                                        new Fields( "upper" )};
    Tap hBaseTap = new HBaseTap( "multitable", new HBaseScheme( keyFields,
      familyNames, valueFields ), SinkMode.UPDATE );

    FlowConnector flowConnector = createHadoopFlowConnector();
    Flow parseFlow = flowConnector.connect( source, hBaseTap, parsePipe );

    // create flow to read from hbase and save to local file
    Tap sink = new Lfs( new TextLine(), "build/test/output/multifamilycascade",
      SinkMode.KEEP );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = flowConnector.connect( hBaseTap, sink, copyPipe );

    // reversed order intentionally
    Cascade cascade = new CascadeConnector().connect( copyFlow, parseFlow );
    cascade.complete();

    verify( "multitable", "left", "lower", 13 );

    verifySink( parseFlow, 13 );
    verifySink( copyFlow, 13 );

    parseFlow = flowConnector.connect( source, hBaseTap, parsePipe );
    copyFlow = flowConnector.connect( hBaseTap, sink, copyPipe );

    // reversed order intentionally
    cascade = new CascadeConnector().connect( copyFlow, parseFlow );
    cascade.complete();

    verify( "multitable", "left", "lower", 26 );

    verifySink( parseFlow, 26 );
    verifySink( copyFlow, 26 );

    }
  }
