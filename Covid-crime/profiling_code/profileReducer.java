import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class profileReducer
 extends Reducer<Text, Text, Text, Text> {

 @Override
 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {
 ArrayList<ArrayList<Integer>> liner = new ArrayList<ArrayList<Integer>>();
 ArrayList<Integer> temp = new ArrayList<Integer>();
 temp.add(0);
 temp.add(0);
 ArrayList<Integer> temp1 = new ArrayList<Integer>();
 temp1.add(0);
 temp1.add(0);
 ArrayList<Integer> temp2 = new ArrayList<Integer>();
 temp2.add(0);
 temp2.add(0);
 ArrayList<Integer> temp3 = new ArrayList<Integer>();
 temp3.add(0);
 temp3.add(0);
 ArrayList<Integer> temp4 = new ArrayList<Integer>();
 temp4.add(0);
 temp4.add(0);
 ArrayList<Integer> temp5 = new ArrayList<Integer>();
 temp5.add(0);
 temp5.add(0);
 ArrayList<Integer> temp6 = new ArrayList<Integer>();
 temp6.add(0);
 temp6.add(0);
 ArrayList<Integer> temp7 = new ArrayList<Integer>();
 temp7.add(0);
 temp7.add(0);
 ArrayList<Integer> temp8 = new ArrayList<Integer>();
 temp8.add(0);
 temp8.add(0);
 ArrayList<Integer> temp9 = new ArrayList<Integer>();
 temp9.add(0);
 temp9.add(0);
 ArrayList<Integer> temp10 = new ArrayList<Integer>();
 temp10.add(0);
 temp10.add(0);
 ArrayList<Integer> temp11 = new ArrayList<Integer>();
 temp11.add(0);
 temp11.add(0);
 ArrayList<Integer> temp12 = new ArrayList<Integer>();
 temp12.add(0);
 temp12.add(0);
 liner.add(temp1);
 liner.add(temp2);
 liner.add(temp3);
 liner.add(temp4);
 liner.add(temp5);
 liner.add(temp6);
 liner.add(temp7);
 liner.add(temp8);
 liner.add(temp9);
 liner.add(temp10);
 liner.add(temp11);
 liner.add(temp12);
 liner.add(temp);
 int min = 13;
 int max = 0;

 int totalinf = 0;
 int totalarr = 0;
 int month = 7;
 for(Text value : values){
   String current[] = value.toString().split(":");
   int u = Integer.parseInt(current[2]);
   //context.write(new Text("" +u), new Text("" + value));
   if(current[0].equals("arrest")){
     int arrest = liner.get(u).get(0) + 1;
     liner.get(u).set(0, arrest);
     totalarr += 1;
   }
   else if(current[0].equals("infection")){
     int infection = Integer.parseInt(current[1]);
     int inff = liner.get(u).get(1) + infection;
     liner.get(u).set(1, inff);
     totalinf += infection;
   }
   min = Math.min(min, u);
   max = Math.max(max, u);
 }
 int minarr = liner.get(min).get(0);
 int maxarr = liner.get(min).get(0);
 int mininf = liner.get(min).get(1);
 int maxinf= liner.get(min).get(1);
 for(int a = min; a <= max; a++){
     int arrs = liner.get(a).get(0);
     int infc = liner.get(a).get(1);
     context.write(new Text("Month " + a), new Text(" Arrests: " + arrs + " Infections: " + infc));
     minarr = Math.min(minarr, arrs);
     maxarr = Math.max(maxarr, arrs);
     mininf = Math.min(mininf, infc);
     maxinf = Math.max(maxinf, infc);
 }
     context.write(new Text("Average Arrests per month: "), new Text(" " + totalarr/month));
     context.write(new Text("Average Infection per month: "), new Text(" " + totalinf/month));
     context.write(new Text("Minimum Arrests per month: "), new Text(" " + minarr));
     context.write(new Text("Maximum Arrests per month: "), new Text(" " + maxarr));
     context.write(new Text("Minimum Infections per month: "), new Text(" " + mininf));
     context.write(new Text("Maximum Infections per month: "), new Text(" " + maxinf));
}
}
