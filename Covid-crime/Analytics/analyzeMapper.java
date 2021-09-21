import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class analyzeMapper
 extends Mapper<LongWritable, Text, Text, Text> {
 private static final int MISSING = 9999;

 @Override
 public void map(LongWritable key, Text value, Context context)
 throws IOException, InterruptedException {
 String line = value.toString();
 int val = 0;
 ArrayList<String> date= new ArrayList<String>(Arrays.asList(line.split(":")));
 if(date.get(0).contains("01")){
    val = 1;
 }
 else if(date.get(0).contains("02")){
    val = 2;
 }
 else if(date.get(0).contains("3")){
    val = 3;
 }
 else if(date.get(0).contains("4")){
    val = 4;
 }
 else if(date.get(0).contains("5")){
    val = 5;
 }
 else if(date.get(0).contains("6")){
    val = 6;
 }
 else if(date.get(0).contains("7")){
    val = 7;
 }
 else if(date.get(0).contains("8")){
    val = 8;
 }
 else if(date.get(0).contains("9")){
    val = 9;
 }
 else if(date.get(0).contains("10")){
    val = 10;
 }
 else if(date.get(0).contains("11")){
    val = 11;
 }
 else{
    val = 12;
 }

 context.write(new Text("Sum"), new Text(date.get(1)+","+ val));
 }
}
