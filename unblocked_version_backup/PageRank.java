import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {
	public static long nodes = 5;
	public static long passes = 5;
	public static long multiplication_factor = 100000;
	public static void main(String[] args) throws Exception {
		int pass = 0;
		nodes = Long.parseLong(args[1]);
		while(pass < passes)  {
			Configuration conf = new Configuration();
	
			Job job = new Job(conf, "pagerank");
			job.setJarByClass(PageRank.class);
	
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
	
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
	
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
	
			//FileInputFormat.addInputPath(job, new Path( ));
			FileSystem fs = FileSystem.get(URI.create( /*"s3n://pagerank-test1/pass"*/args[0] + pass ), job.getConfiguration());
	        FileStatus[] files = fs.listStatus(new Path( /*"s3n://pagerank-test1/pass"*/args[0] + pass++ ));
	        for(FileStatus sfs:files){
	        	System.out.println(sfs.getPath().toUri().toString());
	        	if(!sfs.getPath().toUri().toString().contains("_")) {
	        		FileInputFormat.addInputPath(job, sfs.getPath());
	        	}
	        }
	        fs.delete(new Path(/*"s3n://pagerank-test1/pass"*/args[0]+pass),true);
			FileOutputFormat.setOutputPath(job, new Path(/*"s3n://pagerank-test1/pass"*/args[0]+ pass));
	
			job.waitForCompletion(true);
			
			long residue = job.getCounters().findCounter(Reduce.ResidualCounter.RESIDUE).getValue();
			//System.out.println("Simple counter value:"+residue);
			double total_residue = (residue/(double)nodes)/multiplication_factor;
			total_residue = (double) Math.round(total_residue * 10000) / 10000;
			System.out.println("Residual value is for pass " + pass + ": "+ total_residue);
			
			//Not needed - Thread.sleep(5000);
		}
	}
}