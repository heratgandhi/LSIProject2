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
	public static long nodes = 685230;
	public static long passes = 5;
	public static void main(String[] args) throws Exception {
		int pass = 0;
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
			FileSystem fs = FileSystem.get(URI.create( "s3n://pagerank-test1/pass" + pass ), job.getConfiguration());
	        FileStatus[] files = fs.listStatus(new Path( "s3n://pagerank-test1/pass" + pass++ ));
	        for(FileStatus sfs:files){
	        	System.out.println(sfs.getPath().toUri().toString());
	        	if(!sfs.getPath().toUri().toString().contains("_")) {
	        		FileInputFormat.addInputPath(job, sfs.getPath());
	        	}
	        }
			FileOutputFormat.setOutputPath(job, new Path("s3n://pagerank-test1/pass"+ pass));
	
			job.waitForCompletion(true);
			
			long residue = job.getCounters().findCounter(Reduce.ResidualCounter.RESIDUE).getValue();
			System.out.println("Residual value is for pass " + pass + ": "+residue/(long)nodes);
			
			//Not needed - Thread.sleep(5000);
		}
	}
}