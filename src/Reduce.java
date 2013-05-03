import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	public static enum ResidualCounter {
        RESIDUE,
        PASSES
	}
	
	void printTable(Hashtable<Long,Double> t) {
		Enumeration<Long> enumKey = t.keys();
		while(enumKey.hasMoreElements()) {
		    Long key = enumKey.nextElement();
		    System.out.println(key + "  " + t.get(key));
		}
	}
	
	boolean checkconvergence(Hashtable<Long,Double> tpr, Hashtable<Long,Double> cpr) {
		Enumeration<Long> enumKey = cpr.keys();
		double residual = 0;
		double val;
		long cnt = 0;
		while(enumKey.hasMoreElements()) {
		    Long key = enumKey.nextElement();
		    val = Math.abs(tpr.get(key) - cpr.get(key));
		    if(cpr.get(key) != 0) {
		    	val /= cpr.get(key);
		    }
		    residual += val;
		    cnt++;
		}
		residual /= cnt;
		System.out.println("Reducer block residue: "+residual);
		
		if(residual <= 0.001f) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Hashtable<Long, Double> tpr = new Hashtable<Long,Double>(); //Prev iteration values
		Hashtable<Long, Double> ppr = new Hashtable<Long, Double>(); //Prev pass values
		Hashtable<Long, Double> cpr = new Hashtable<Long, Double>(); //Current iteration values
		Hashtable<Long, Double> degree = new Hashtable<Long, Double>();
		Hashtable<Long, Double> indegree = new Hashtable<Long, Double>();
		Hashtable<Long, String> info = new Hashtable<Long,String>();
		String[] parts = null;
		long max_node = -1;
		
		//System.out.println("Got :" + key+ " ");
		ArrayList<String> values1 = new ArrayList<String>();
		
		for(Text val : values) {
			if(val.charAt(0) == 'p') {
				values1.add(val.toString());
				
				//Page rank computation values
				parts = val.toString().split(";");
				
				ppr.put(new Long(parts[2]), Double.parseDouble(parts[3]));
				tpr.put(new Long(parts[2]), Double.parseDouble(parts[3]));
				
				indegree.put(new Long(parts[1]), 1.0);
				
				degree.put(new Long(parts[2]), Double.parseDouble(parts[4]));
			} else {
				//Information
				parts = val.toString().split(";");
				
				info.put(new Long(parts[1]), parts[2]);
				
				if(parts[2].charAt(0) != ' ') {
					
					ppr.put(new Long(parts[1]), Double.parseDouble(parts[2].split(" ")[0]) );
					tpr.put(new Long(parts[1]), Double.parseDouble(parts[2].split(" ")[0]) );
					
					cpr.put(new Long(parts[1]), Double.parseDouble(parts[2].split(" ")[0]) );
					
					degree.put(new Long(parts[1]), (double) (parts[2].split(" ").length-1));
					
					if(Long.parseLong(parts[1]) > max_node) {
						max_node = Long.parseLong(parts[1]);
					}
				} else {
					String t = parts[2].substring(1);
					
					ppr.put(new Long(parts[1]), Double.parseDouble(t.split(" ")[0]) );
					cpr.put(new Long(parts[1]), Double.parseDouble(t.split(" ")[0]) );
					
					tpr.put(new Long(parts[1]), Double.parseDouble(t.split(" ")[0]) );
					
					degree.put(new Long(parts[1]), 0.0);
					
					if(Long.parseLong(parts[1]) > max_node) {
						max_node = Long.parseLong(parts[1]);
					}
				}
			}
		}
		
		double division = 0.0;
		boolean first = true;
		Enumeration<Long> enumKey;
		int no_passes = 0;
		
		do {
			if(!first) {
				enumKey = cpr.keys();
				while(enumKey.hasMoreElements()) {
				    Long keyv = enumKey.nextElement();
				    tpr.put(keyv,cpr.get(keyv));
				    if(indegree.get(keyv) != null)
				    	cpr.put(keyv, 0.0);
				}				
			} else {
				enumKey = cpr.keys();
				while(enumKey.hasMoreElements()) {
					Long keyv = enumKey.nextElement();
					if(indegree.get(keyv) != null)
						cpr.put(keyv, 0.0);
				}
			}
			for(String val1 : values1) {
				parts = val1.toString().split(";");
				
				division = tpr.get(new Long(parts[2])) / degree.get(new Long(parts[2]));
				
				cpr.put(new Long(parts[1]), cpr.get(new Long(parts[1])) + division);
			}
			enumKey = cpr.keys();
			while(enumKey.hasMoreElements()) {
			    Long keyv = enumKey.nextElement();
			    if(indegree.get(keyv) != null) {
			    	double tmp = ((0.15 / PageRank.nodes) + (0.85 * cpr.get(keyv)));
			    	
			    	cpr.put(keyv, tmp);
			    }
			}
			first = false;
			no_passes++;
		} while(!checkconvergence(tpr,cpr));
		
		enumKey = cpr.keys();
		double residual = 0;
		double val;
		while(enumKey.hasMoreElements()) {
		    Long keyv = enumKey.nextElement();
		    val = Math.abs(cpr.get(keyv) - ppr.get(keyv));
		    val /= cpr.get(keyv);
		    residual += val;
		    
		    if(degree.get(keyv) != 0.0)
		    	context.write(key, new Text(keyv+" "+cpr.get(keyv)+" "+degree.get(keyv).intValue() + " " + info.get(keyv).substring((info.get(keyv).indexOf(" ")+1)) ) );
		    else
		    	context.write(key, new Text(keyv+" "+cpr.get(keyv)+" "+degree.get(keyv).intValue()) );
		}
		residual *= PageRank.multiplication_factor; //To map residual to the long rank
		
		System.out.println("Residual: " + residual + " passes: " + no_passes);
		System.out.println("Max node in this block is: " + max_node + " and its page rank is: "+cpr.get(new Long(max_node)));
		
		context.getCounter(ResidualCounter.PASSES).increment(no_passes);
		context.getCounter(ResidualCounter.RESIDUE).increment((long)residual);
	}
}