import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
public class analyzeReducer
 extends Reducer<Text, Text, Text, Text> {

 @Override
 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {
 int arrests = 0;
 int infection = 0;
 int min = 13;
 int max = 0;
 ArrayList<ArrayList<Integer>> liner = new ArrayList<ArrayList<Integer>>();
 ArrayList<ArrayList<Double>> rate = new ArrayList<ArrayList<Double>>();
 ArrayList<Integer> emp = new ArrayList<Integer>();
 emp.add(1);
 emp.add(1);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 liner.add(emp);
 for(Text value : values){
       String line = value.toString();
       ArrayList<String> date= new ArrayList<String>(Arrays.asList(line.split(",")));
       int arrestmont = Integer.parseInt(date.get(0).replaceAll(" ",""));
       int infectmont = Integer.parseInt(date.get(1).replaceAll(" ",""));
       arrests += arrestmont;
       infection += infectmont;
       //int pos = new Integer(Integer.parseInt((String)key));
       int pos = Integer.parseInt(date.get(2).replaceAll(" ",""));
       min = Math.min(pos, min);
       max = Math.max(pos, max);
       ArrayList<Integer> temp = new ArrayList<Integer>();
       temp.add(arrestmont);
       temp.add(infectmont);
       liner.set(pos,temp);
 }
 int limit = liner.size()-1;
 double avgarrest = arrests/(limit);
 double avginfect = infection/(limit);
 if(min != 13 || max != 0){
   for(int a = min; a < max; a++){
     double arrchange = (double)(liner.get(a+1).get(0) - (double)liner.get(a).get(0))/avgarrest;
     double infectchange = (double)(liner.get(a+1).get(1) - (double)liner.get(a).get(1))/avginfect;
     ArrayList<Double> temp = new ArrayList<Double>();
     temp.add(arrchange);
     temp.add(infectchange);
     rate.add(temp);
     context.write(new Text("Month: " + a) , new Text("   Arrests: " + liner.get(a).get(0) + " Infection: " + liner.get(a).get(1)));
     context.write(new Text("Change over period: " + a + "-" + (a + 1)), new Text(" Percent-Change-Arrests: " + arrchange + " Percent-Change-Infection: " + infectchange));
   }
  context.write(new Text("Month: " + max) , new Text(("   Arrests: " + liner.get(max).get(0) + " Infection: " + liner.get(max).get(1))));
   double sumarr = 0.0;
   double suminf = 0.0;
   double sumai = 0.0;
   double sumaa = 0.0;
   double sumii = 0.0;
   for(int a = min; a < rate.size(); a++){
     double arrs = rate.get(a).get(0);
     double infec = rate.get(a).get(1);
     sumarr += arrs;
     suminf += infec;
     sumai += arrs * infec;
     sumaa += arrs * arrs;
     sumii += infec * infec;
   }
   double n = rate.size() + 0.0;
   double coefficient = ((n * sumai) - (sumarr * suminf))/ Math.sqrt((n*(sumaa)-sumaa)*(n*(sumii)-sumii));
   context.write(new Text("Correlation Coefficient: "), new Text(" " + coefficient));
 }
 }
}
