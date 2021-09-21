import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Clean {
 public static void main(String[] args) throws Exception {

 Job job = new Job();
 job.setJarByClass(Clean.class);
 job.setJobName("Clean");
 job.setNumReduceTasks(1);
 //ileInputFormat.addInputPath(job, new Path(args[0]));
 //FileOutputFormat.setOutputPath(job, new Path(args[1]));
 MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, covidMapper.class);
 MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, NYPDMapper.class);
 FileOutputFormat.setOutputPath(job, new Path(args[2]));

 //job.setMapperClass(CleanMapper.class);
 job.setReducerClass(CleanReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);

 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
