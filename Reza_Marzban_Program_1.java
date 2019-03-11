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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class Program1Mapper extends Mapper<LongWritable, Text, Text, Text>{
	//the mapper sends make as key and model as value.
	private Text ResultKey = new Text();
	private Text ResultValue = new Text();
    public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
      String line = value.toString();
	  String[] values = line.split(",");
	  String Make = values[46];
	  String Model = values[47];
	  if (!Make.matches("-?\\d+(\\.\\d+)?") && !Model.matches("-?\\d+(\\.\\d+)?")) {//make sure Model and Make are not numerical.
      if (Make != null && !Make.isEmpty()) {//make sure Model and Make are not empty or null.
        if (Model != null && !Model.isEmpty()) {ResultKey.set(Make);ResultValue.set(Model);context.write(ResultKey, ResultValue);}
      }
	  }
    }
}

class Program1Reducer extends Reducer<Text,Text,Text,IntWritable> {
	//Reducer save the models into a set to get rid of repeated models, and then count them.
    public static boolean ascending = true;
    public static boolean descending = false;
	private Map<String, Integer> result = new HashMap<String, Integer>();
	 
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	  int count = 0;
	  HashSet<String> models = new HashSet<String>();
      for (Text Model : values) {
        models.add(Model.toString());
      }
	  count=models.size();
	  result.put(key.toString(),count);
    }
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		//sort the output of reducer and also choose to save top 5 makes.
		int counter=0;
		Map<String, Integer> sortedResult =mapSortValue(result,descending);
		for (Map.Entry<String, Integer> entry : sortedResult.entrySet()) {
			counter++;
			if(counter>5){break;}
			Text key = new Text(entry.getKey());
			IntWritable value =new IntWritable(entry.getValue());
			context.write(key, value);
		}
    }
	private static Map<String, Integer> mapSortValue(Map<String, Integer> unsortMap, final boolean order)
    {
	//Sort the result of reducer according to the value not the key
        List<Entry<String, Integer>> l1 = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());
        Collections.sort(l1, new Comparator<Entry<String, Integer>>()
        {
            public int compare(Entry<String, Integer> first,Entry<String, Integer> second)
            {
                if (order){return first.getValue().compareTo(second.getValue());}
                else{return second.getValue().compareTo(first.getValue());}
            }
        });
        Map<String, Integer> output = new LinkedHashMap<String, Integer>();
        for (Entry<String, Integer> entry : l1)
        {output.put(entry.getKey(), entry.getValue());}
		return output;
    }
}
public class Reza_Marzban_Program_1{
  public static void main(String[] args) throws Exception{
	if(args.length!=2){
		System.err.println("Usage: Reza_Marzban_Program_1 <input path> <output path>");
		System.exit(-1);
	}
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Reza_Marzban_Program_1.class);
	job.setJobName("Reza Marzban Program 1");
    job.setMapperClass(Program1Mapper.class);
    job.setReducerClass(Program1Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}