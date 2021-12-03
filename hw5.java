import java.util.*;
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


public class hw5 {
	
	
	public static class TopMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		private TreeSet<String> stopSet = new TreeSet<String>(Arrays.asList("a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because",
				"been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each",
				"few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how",
				"how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off",
				"on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than",
				"that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under",
				"until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom",
				"why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"));	    
		private static Map<String, Integer> map = new HashMap<String, Integer>();
	    private static PriorityQueue<Entry<String, Integer>> queue;
	    
	    @Override
	    public void setup(Context context) throws IOException, InterruptedException 
		{
			//create queue to weight and sort by number of occurrences; prevent two jobs
	    	queue = new PriorityQueue<Map.Entry<String, Integer>>(new Comparator<Map.Entry<String, Integer>>()
			{
	    		@Override
	    		public int compare(Map.Entry<String, Integer> first, Map.Entry<String, Integer> second) 
				{
	    			if(first.getValue() == second.getValue()) 
					{
	    				return first.getKey().compareTo(second.getKey());
	    			} 
					else 
					{
	    				return first.getValue() - second.getValue();
	    			}
	    		}
	    	});
	    }
	    
	    @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
	    	//clean data
	    	String line = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
	    	
	    	StringTokenizer tokenizer = new StringTokenizer(line);
	    	
			//while another word exists, tokenize it and check if it should be counted
	    	while(tokenizer.hasMoreTokens()) 
			{
	    		String token = tokenizer.nextToken();
	    		if(stopSet.contains(token)) 
				{
	    			continue;
	    		}
				//if new token, add entry
	    		if(!map.containsKey(token)) 
				{
	    			map.put(token, 1);
	    		}
				else 
				{
	    			Integer temp = map.get(token) + 1;
	    			map.replace(token, temp);
	    		}
	    	}
	    	
	    	Iterator<Map.Entry<String, Integer>> mapIterator = map.entrySet().iterator();
	    	
			//continue adding to queue
	    	while(mapIterator.hasNext()) 
			{
	    		queue.add(mapIterator.next());
				//bop it out if the queue too big
	    		if(queue.size() > 5) 
				{
	    			queue.remove();
	    		}
	    	}
	    }
	    	
	    @Override
	    public void cleanup(Context context) throws IOException, InterruptedException 
		{
			//while objects exist remove them and write them at the end
	    	while(!queue.isEmpty()) 
			{
	    		Map.Entry<String, Integer> entry = queue.remove();
				//actual write
	    		context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
	    	}
		}
	}

  public static class TopReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
	  
	  private static PriorityQueue<Map.Entry<String, Integer>> queue;
    
	  @Override
	  public void setup(Context context) throws IOException, InterruptedException 
	  {
		  //create priorityqueue for comparisons
		  queue = new PriorityQueue<Map.Entry<String, Integer>>(new Comparator<Map.Entry<String, Integer>>()
		    {
	    		@Override
	    		public int compare(Map.Entry<String, Integer> first, Map.Entry<String, Integer> second) 
				{
	    			if(first.getValue() == second.getValue()) 
					{
	    				return first.getKey().compareTo(second.getKey());
	    			} 
					else 
					{
	    				return first.getValue() - second.getValue();
	    			}
	    		}
	    	});
	  }
	  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	  {
		  Integer total = 0;
		  for(IntWritable val: values) 
		  {
			  total += val.get();
		  }
		 
		  //create next entry for the map
		  Map.Entry<String, Integer> entry = new AbstractMap.SimpleEntry<>(key.toString(), total);
		  
		  queue.add(entry);
		  if(queue.size() > 5) 
		  {
			  queue.remove();
		  }	  
	  }
	  
	  @Override
	  public void cleanup(Context context) throws IOException, InterruptedException 
	  {
		  while(!queue.isEmpty()) 
		  {
	    		Map.Entry<String, Integer> entry = queue.remove();
				//actual write
	    		context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		  }
	  }
  }

  public static void main(String[] args) throws Exception 
  {
		if(args.length != 2) 
	    {
			 System.err.println("Usage: Top N <input path> <output path>");
			 System.exit(0);
		}
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Top N");
		job.setReducerClass(TopReducer.class);
		job.setOutputKeyClass(Text.class);
	    job.setJarByClass(hw5.class);
	    job.setMapperClass(TopMapper.class);
		job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(1);
	    
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}