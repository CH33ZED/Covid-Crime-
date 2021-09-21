import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.util.List;

public class covidpMapper extends Mapper<LongWritable, Text, Text, Text> {
     private static final int MISSING = 9999;
    static ArrayList<String> checking = new ArrayList<String>(Arrays.asList("3","4","5","6","7","8","9"));
     @Override
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
     String line = value.toString();
     ArrayList<String> date= new ArrayList<String>(Arrays.asList(line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)));
     if(date.size() >= 38 && date.get(38).equals("0")){
     try{
       int x = Integer.parseInt(date.get(1));
       String time = (String) date.get(0);
       ArrayList month = new ArrayList(Arrays.asList(time.split("/")));
       String mont = (String) month.get(0);
       if(!mont.equals("1") && !mont.equals("02") && !mont.equals("11")&& !mont.equals("10")){//checking.contains(mont)){
            context.write(new Text("total"), new Text("infection:" + x + ":" + mont));
       }
   }
   catch(Exception e){}
     }
    }
}
