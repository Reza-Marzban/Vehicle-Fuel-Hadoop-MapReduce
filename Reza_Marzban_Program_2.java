import java.util.HashSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class Program2Mapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
	//the mapper sends make as key and barrels08 as value (annual petroleum consumption).
	private Text ResultKey = new Text();
	private FloatWritable ResultValue = new FloatWritable();
    public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
      String line = value.toString();
	  String[] values = line.split(",");
	  String Make = values[46];
	  String barrels08 = values[0];
	  if (!Make.matches("-?\\d+(\\.\\d+)?") && barrels08.matches("-?\\d+(\\.\\d+)?")) {//make sure Make is not numerical and barrels 08 is numerical.
      if (Make != null && !Make.isEmpty()) {//make sure Model and Make are not empty or null.
        if (barrels08 != null && !barrels08.isEmpty()) {ResultKey.set(Make);ResultValue.set(Float.parseFloat(barrels08));context.write(ResultKey, ResultValue);}
      }
	  }
    }
}

class Program2Reducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	//Reducer produce the average of values for each key.
    public static boolean ascending = true;
    public static boolean descending = false;
	private Map<String, Float> result = new HashMap<String, Float>();
	 
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
	  float count = 0;
	  float sum = 0;
      for (FloatWritable barrels08 : values) {
        count++;
		sum+=barrels08.get();
      }
	  float ave=sum/count;
	  result.put(key.toString(),ave);
    }
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		//sort the output of reducer.
		Map<String, Float> sortedResult =mapSortValue(result,descending);
		for (Map.Entry<String, Float> entry : sortedResult.entrySet()) {
			Text key = new Text(entry.getKey());
			FloatWritable value =new FloatWritable(entry.getValue());
			context.write(key, value);
		}
    }
	private static Map<String, Float> mapSortValue(Map<String, Float> unsortMap, final boolean order)
    {
	//Sort the result of reducer according to the value not the key
        List<Entry<String, Float>> l1 = new LinkedList<Entry<String, Float>>(unsortMap.entrySet());
        Collections.sort(l1, new Comparator<Entry<String, Float>>()
        {
            public int compare(Entry<String, Float> first,Entry<String, Float> second)
            {
                if (order){return first.getValue().compareTo(second.getValue());}
                else{return second.getValue().compareTo(first.getValue());}
            }
        });
        Map<String, Float> output = new LinkedHashMap<String, Float>();
        for (Entry<String, Float> entry : l1)
        {output.put(entry.getKey(), entry.getValue());}
		return output;
    }
}
public class Reza_Marzban_Program_2{
  public static void main(String[] args) throws Exception{
	if(args.length!=2){
		System.err.println("Usage: Reza_Marzban_Program_2 <input path> <output path>");
		System.exit(-1);
	}
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Reza_Marzban_Program_2.class);
	job.setJobName("Reza Marzban Program 2");
    job.setMapperClass(Program2Mapper.class);
    job.setReducerClass(Program2Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}