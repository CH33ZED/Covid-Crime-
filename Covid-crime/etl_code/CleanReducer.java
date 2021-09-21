import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class CleanReducer
 extends Reducer<Text, Text, Text, Text> {

 @Override
 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {
 int infection = 0;
 int arrests = 0;
 for(Text value : values){
   String current[] = value.toString().split(":");
   if(current[0].equals("arrest")){
     arrests += 1;
   }
   else if(current[0].equals("infection")){
     infection += Integer.parseInt(current[1]);
   }
 }
 context.write(key, new Text(":" + arrests + "," + infection));
 }
}
