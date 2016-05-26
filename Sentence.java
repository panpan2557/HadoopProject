import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.BreakIterator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.client.HTable;

public class Sentence {

  public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";


  public static class SplitMapper extends Mapper<Object, Text, Text, Text>{

    String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
      filename = context.getConfiguration().get(fsFileSplit.getPath().getParent().getName());
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String content = value.toString();
      BreakIterator iterator = BreakIterator.getSentenceInstance(Locale.US);
      iterator.setText(value.toString());
      int start = iterator.first();
      int i = 0;
      for (int end = iterator.next(); end != BreakIterator.DONE; 
          start = end, end = iterator.next()) {
        String sentence = content.substring(start,end);
        String info = "#" + i + "/" + filename;
        context.write(sentence, info);
      }
    }
  }

  public static class TableReducer extends TableReducer<Text,Text,ImmutableBytesWritable> {
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      //sentence in many book
      //ex. "I love you", [#1/book1, #1/book2]
      int i = 1;
      for (Text val: values) {
        //each location
        String[] valArr = val.split("/");
        valArr[0] = valArr[0].substring(1); //remove #
        Put p = new Put(Bytes.toBytes(key.toString()));
        p.add(Bytes.toBytes("info" + i), Bytes.toBytes("sentence no."), Bytes.toBytes(valArr[0]));
        p.add(Bytes.toBytes("info" + i), Bytes.toBytes("book"), Bytes.toBytes(valArr[1]));
        i++;
      }
      //write to table
      context.write(key, put);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Configuration hConf = HBaseConfiguration.create(conf);

    String hbaseZookeeperQuorum="52.221.246.249";
    String hbaseZookeeperClientPort=2181;
    String tableName="sentenceTable";

    hConf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
    hConf.setInt(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);
    
    //create sentenceTable
    hTable = new HTable(hConf, tableName);

    Job job = Job.getInstance(conf, "split");
    job.setJarByClass(Sentence.class);
    job.setMapperClass(SplitMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    TableMapReduceUtil.initTableReducerJob("summary_sentence", TableReducer.class, job);
    
    //args[0] = directory path
    File folder = new File(args[0]);
    File[] listOfFiles = folder.listFiles();

    //each file in directory
    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile()) {
        MultipleInputs.addInputPath(job,listOfFiles[i].getName(),TextInputFormat.class);
      } 
      else if (listOfFiles[i].isDirectory()) {
        System.out.println("Directory " + listOfFiles[i].getName());
      }
    }

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}