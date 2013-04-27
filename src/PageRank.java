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
	public static String bucket = "s3n://wordcount-test-herat/pass";
	public static void main(String[] args) throws Exception {
		int pass = 0;
		double residue = 1000;
		
		bucket = args[0];
		
		while(residue > 0.001)  {
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
			FileSystem fs = FileSystem.get(URI.create( bucket + pass ), job.getConfiguration());
	        FileStatus[] files = fs.listStatus(new Path( bucket + pass ));
	        for(FileStatus sfs:files){
	        	System.out.println(sfs.getPath().toUri().toString());
	        	if(!sfs.getPath().toUri().toString().contains("_")) {
	        		FileInputFormat.addInputPath(job, sfs.getPath());
	        	}
	        }
	        pass++;
	        fs.delete(new Path(bucket+pass),true);
			FileOutputFormat.setOutputPath(job, new Path(bucket+ pass));

			job.waitForCompletion(true);

			residue = job.getCounters().findCounter(Reduce.ResidualCounter.RESIDUE).getValue();
			residue = (residue/nodes)/multiplication_factor;
			System.out.println("Residual value is for pass " + pass + ": "+ residue);
		}
	}
}