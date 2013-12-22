import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class Vcftoplink{
  public static class PositionMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private LongWritable outkey = new LongWritable();
    private Text Result=new Text();
    private String chrom;
    private String[] temp;
    private String filename;

    public void setup(Context context) throws IOException, InterruptedException{
      Configuration con=context.getConfiguration();
      chrom="chr"+con.get("chrom");
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String fullname=fileSplit.getPath().getName();
      int i=fullname.indexOf('.');
      filename=fullname.substring(0, i);
    }

    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      String outputResult;
      String line=value.toString();
      if(line.contains(chrom)){
        temp=line.split("\\s+");
        if(temp[0].equals(chrom)){
          if(temp[6].equals("PASS")){
            outputResult=(filename+","+temp[0]+","+temp[1]+","+temp[3]+","+temp[4]);
            Result.set(outputResult);

            outkey.set(Long.parseLong(temp[1]));

            context.write(outkey, Result);}

        }

      }

    }

  }


  public static class ValueReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
    private Text result=new Text();
    private Text frequency=new Text();
    private String[] temp;
    private int total;
    private boolean first=true;
    private org.apache.hadoop.mapreduce.lib.output.MultipleOutputs mos;

    public void setup(Context context) throws IOException, InterruptedException{
      Configuration con=context.getConfiguration();
      total=Integer.parseInt(con.get("fileno"));
      mos=new org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<Text, NullWritable>(context);
    }

    public void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      String outputStr=null;
      boolean [] notNull=new boolean[total];
      String [] ref=new String [total];
      String [] alt=new String [total];
      String al1=null;
      String al2=null;
      int al1num=0;
      int al2num=0;
      boolean freq=true;
      String outputfreq=null;

      for(int i=0;i<total;i++){
        notNull[i]=false;
        ref[i]="	";
        alt[i]="	";
      }

      for (Text t : values) {
        String line=t.toString();
        temp=line.split(",");
        if(first) { 
          outputStr=(temp[1]+"	"+temp[2]+"	");
          outputfreq=(temp[1]+"	"+temp[2]+"	");
          first=false;
        }

        int current=Integer.parseInt(temp[0]);

        notNull[current-1]=true;
        ref[current-1]=temp[3];
        alt[current-1]=temp[4];
        if(al1==null) {
          al1=temp[4];
          al1num++;
        }
        else if (al1.equals(temp[4]))
          al1num++;
        else if(al2==null) {
          al2=temp[4];
          al2num++;
        }
        else if(al2.equals(temp[4]))
          al2num++;
        else freq=false;
      }

      for(int count=1;count<=total;count++){
        if(!notNull[count-1]){
          outputStr+=(count+":"+"xx  ");
        }
        else{
          outputStr+=(count+":"+ref[count-1]+alt[count-1]+"  ");
        }
      }

      first=true;
      result.set(outputStr);
      mos.write(NullWritable.get(),result, "result");
      if(freq){
        double al1freq=(double)al1num/total;
        double al2freq=(double)al2num/total;

        if(al2num==0) al2=" ";
        if(al1num>=al2num) outputfreq+="AL1 "+al1+"  AL2 "+al2+"  Freq1 "+al1freq+"  MAF "+al2freq+"  ";
        else outputfreq+="AL1 "+al2+"  AL2 "+al1+"  Freq1 "+al2freq+"  MAF "+al1freq+"  ";
        frequency.set(outputfreq);
        mos.write(NullWritable.get(), frequency, "freq");
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("chrom", args[2]);
    conf.set("fileno", args[3]);
    double sampleRate = Double.parseDouble(args[4]);
    int sampleNum=Integer.parseInt(args[5]);
    int reducerNum=(sampleNum/1000)*6;
    Path inputPath = new Path(args[0]);
    Path partitionFile = new Path(args[1] + "_partitions.lst");
    Path outputStage = new Path(args[1] + "_staging");
    Path outputOrder = new Path(args[1]);
    Path mergedFile=new Path(args[1]+"_merged");
    Path freqFile=new Path(args[1]+"_freq");
    // Configure job to prepare for sampling
    Job sampleJob = new Job(conf, "QUERY1Sorting");
    sampleJob.setJarByClass(Vcftoplink.class);

    // Use the mapper implementation with zero reduce tasks
    sampleJob.setMapperClass(PositionMapper.class);
    sampleJob.setNumReduceTasks(0);
    sampleJob.setOutputKeyClass(LongWritable.class);
    sampleJob.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(sampleJob, inputPath);
    // Set the output format to a sequence file
    sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);
    // Submit the job and get completion code.
    int code = sampleJob.waitForCompletion(true) ? 0 : 1;
    if (code == 0) {
      Job orderJob = new Job(conf, "VCFTransformation");
      orderJob.setJarByClass(Vcftoplink.class);
      // Here, use the identity mapper to output the key/value pairs in
      // the SequenceFile
      orderJob.setMapperClass(Mapper.class);
      orderJob.setReducerClass(ValueReducer.class);
      // Set the number of reduce tasks to an appropriate number for the
      // amount of data being sorted
      orderJob.setNumReduceTasks(reducerNum);
      // Use Hadoop's TotalOrderPartitioner class
      orderJob.setPartitionerClass(TotalOrderPartitioner.class);
      // Set the partition file
      TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),partitionFile);
      orderJob.setOutputKeyClass(LongWritable.class);
      orderJob.setOutputValueClass(Text.class);

      // Set the input to the previous job's output
      orderJob.setInputFormatClass(SequenceFileInputFormat.class);
      orderJob.setOutputFormatClass(TextOutputFormat.class);

      org.apache.hadoop.mapreduce.lib.output.MultipleOutputs.addNamedOutput(orderJob, "freq",TextOutputFormat.class, LongWritable.class, Text.class); 
      org.apache.hadoop.mapreduce.lib.output.MultipleOutputs.addNamedOutput(orderJob, "result",TextOutputFormat.class, Text.class, NullWritable.class); 
      SequenceFileInputFormat.setInputPaths(orderJob, outputStage);
      // Set the output path to the command line parameter
      FileOutputFormat.setOutputPath(orderJob, outputOrder);
      // Set the separator to an empty string
      orderJob.getConfiguration().set( "mapred.textoutputformat.separator", "");
      // Use the InputSampler to go through the output of the previous

      // job, sample it, and create the partition file
      InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(sampleRate, sampleNum));
      // Submit the job
      code = orderJob.waitForCompletion(true) ? 0 : 2;
    }
    System.exit(code);
  }
}

