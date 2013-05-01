import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	public static enum ResidualCounter {
        RESIDUE,
        PASSES
	}
	
	void printTable(Hashtable<Long,Float> t) {
		Enumeration<Long> enumKey = t.keys();
		while(enumKey.hasMoreElements()) {
		    Long key = enumKey.nextElement();
		    System.out.println(key + "  " + t.get(key));
		}
	}
	
	boolean checkconvergence(Hashtable<Long,Float> tpr, Hashtable<Long,Float> cpr) {
		Enumeration<Long> enumKey = cpr.keys();
		float residual = 0;
		float val;
		long cnt = 0;
		while(enumKey.hasMoreElements()) {
		    Long key = enumKey.nextElement();
		    //System.out.println("dfsf:" + cpr.get(key));
		    val = Math.abs(tpr.get(key) - cpr.get(key));
		    
		    if(cpr.get(key) != 0) {
		    	//System.out.println("---Division by:" + cpr.get(key));
		    	val /= cpr.get(key);
		    }
		    residual += val;
		    cnt++;
		}
		residual /= cnt;
		//residual = (float) (Math.round(residual * 10000) / 10000);
		
		System.out.println("Reducer block residue: "+residual);
		
		if(residual <= 0.001f) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Hashtable<Long, Float> tpr = new Hashtable<Long,Float>(); //Prev iteration values
		Hashtable<Long, Float> ppr = new Hashtable<Long, Float>(); //Prev pass values
		Hashtable<Long, Float> cpr = new Hashtable<Long, Float>(); //Current iteration values
		Hashtable<Long, Float> degree = new Hashtable<Long, Float>();
		Hashtable<Long, Float> indegree = new Hashtable<Long, Float>();
		Hashtable<Long, String> info = new Hashtable<Long,String>();
		String[] parts = null;
		long max_node = -1;
		
		//System.out.println("Got :" + key+ " ");
		ArrayList<String> values1 = new ArrayList<String>();
		
		for(Text val : values) {
			//System.out.println("Value: "+val.toString());
			values1.add(val.toString());
			if(val.charAt(0) == 'p') {
				//Page rank computation values
				parts = val.toString().split(";");
				ppr.put(new Long(parts[2]), Float.parseFloat(parts[3]));
				tpr.put(new Long(parts[2]), Float.parseFloat(parts[3]));
				indegree.put(new Long(parts[1]), 1.0f);
				degree.put(new Long(parts[2]), Float.parseFloat(parts[4]));
			} else {
				//Information
				parts = val.toString().split(";");
				info.put(new Long(parts[1]), parts[2]);
				if(parts[2].charAt(0) != ' ') {
					ppr.put(new Long(parts[1]), Float.parseFloat(parts[2].split(" ")[0]) );
					tpr.put(new Long(parts[1]), Float.parseFloat(parts[2].split(" ")[0]) );
					cpr.put(new Long(parts[1]), Float.parseFloat(parts[2].split(" ")[0]) );
					degree.put(new Long(parts[1]), (float) (parts[2].split(" ").length-1));
					
					if(Long.parseLong(parts[1]) > max_node) {
						max_node = Long.parseLong(parts[1]);
					}
				} else {
					String t = parts[2].substring(1);
					ppr.put(new Long(parts[1]), Float.parseFloat(t.split(" ")[0]) );
					cpr.put(new Long(parts[1]), Float.parseFloat(t.split(" ")[0]) );
					tpr.put(new Long(parts[1]), Float.parseFloat(t.split(" ")[0]) );
					degree.put(new Long(parts[1]), 0.0f);
					if(Long.parseLong(parts[1]) > max_node) {
						max_node = Long.parseLong(parts[1]);
					}
				}
			}
		}
		
		//Code for BFS ordering
		ArrayList<Long> start_nodes = new ArrayList<Long>();
		for(String st : values1) {
			if (st.charAt(0) == 'p') {
				parts = st.toString().split(";");
				if(info.get(new Long(parts[2])) == null) {
					if(!start_nodes.contains(new Long(parts[1])))
						start_nodes.add(new Long(parts[1]));					
				}
			}
		}
		System.out.println("Initial vertices:");
		for(Long str1 : start_nodes) {
			System.out.println(str1);
		}
		for(Long nd : start_nodes) {
			for(String st : values1) {
				if(st.charAt(0) == 'p') {
					parts = st.toString().split(";");
					if(nd == new Long(parts[2]) && !start_nodes.contains(new Long(parts[1])) ) {
						start_nodes.add(new Long(parts[1]));
					}
				}
			}
		}
		System.out.println("All vertices:");
		for(Long str1 : start_nodes) {
			System.out.println(str1);
		}
		ArrayList<String> values2 = new ArrayList<String>();
		for(Long nd : start_nodes) {
			for(String st : values1) {
				if(st.charAt(0) == 'p') {
					parts = st.toString().split(";");
					if(nd == new Long(parts[2]) && !values2.contains(st) ) {
						values2.add(st);
					}
				}
			}
		}
		System.out.println("Sorted p msgs:");
		for(String str1 : values2) {
			System.out.println(str1);
		}
		
		//Page rank computation
		float division = 0;
		boolean first = true;
		Enumeration<Long> enumKey;
		int no_passes = 0;
		do {
			if(!first) {
				//System.out.println("Copying...");
				enumKey = cpr.keys();
				while(enumKey.hasMoreElements()) {
				    Long keyv = enumKey.nextElement();
				    tpr.put(keyv,cpr.get(keyv));
				    if(indegree.get(keyv) != null)
				    	cpr.put(keyv, 0.0f);
				}				
			} else {
				enumKey = cpr.keys();
				while(enumKey.hasMoreElements()) {
					Long keyv = enumKey.nextElement();
					if(indegree.get(keyv) != null)
						cpr.put(keyv, 0.0f);
				}
			}
			for(String val1 : values2) {
				//System.out.println("Value: " + val1);
				if(val1.charAt(0) == 'p') {
					parts = val1.toString().split(";");
					division = tpr.get(new Long(parts[2])) / degree.get(new Long(parts[2]));
					
					//System.out.println("Division: "+division);
					
					cpr.put(new Long( parts[1] ), cpr.get(new Long(parts[1])) + division);
					
					//System.out.println("Put: "+cpr.get(new Long(parts[1])));
				}
			}
			enumKey = cpr.keys();
			while(enumKey.hasMoreElements()) {
			    Long keyv = enumKey.nextElement();
			    //System.out.println(keyv+" "+cpr.get(keyv)+" "+(0.85 * cpr.get(keyv))+" "+(0.15 / PageRank.nodes)+" "+PageRank.nodes);
			    if(indegree.get(keyv) != null) {
			    	float tmp = (float) ((0.15 / PageRank.nodes) + (0.85 * cpr.get(keyv)));
			    	//System.out.println(tmp);
			    	//tmp = (float) Math.round(tmp * 10000) / 10000;
			    	cpr.put(keyv, tmp);
			    	//System.out.println("Get: " + cpr.get(keyv));
			    }
			}
			first = false;
			no_passes++;
			//System.out.println("tpr");
			//printTable(tpr);
			//System.out.println("cpr");
			//printTable(cpr);
		} while(!checkconvergence(tpr,cpr));
		
		enumKey = cpr.keys();
		float residual = 0;
		float val;
		while(enumKey.hasMoreElements()) {
		    Long keyv = enumKey.nextElement();
		    val = Math.abs(cpr.get(keyv) - ppr.get(keyv));
		    val /= cpr.get(keyv);
		    residual += val;
		    //System.out.println(key+" "+keyv);
		    if(degree.get(keyv) != 0.0)
		    	context.write(key, new Text(keyv+" "+cpr.get(keyv)+" "+degree.get(keyv).intValue() + " " + info.get(keyv).substring((info.get(keyv).indexOf(" ")+1)) ) );
		    else
		    	context.write(key, new Text(keyv+" "+cpr.get(keyv)+" "+degree.get(keyv).intValue()) );
		}
		//residual = (float) Math.round(residual * 10000) / 10000;
		residual *= PageRank.multiplication_factor; //To map residual to the long rank
		
		System.out.println("Residual: " + residual + " passes: " + no_passes);
		System.out.println("Max node in this block is: " + max_node + " and its page rank is: "+cpr.get(new Long(max_node)));
		
		context.getCounter(ResidualCounter.PASSES).increment(no_passes);
		context.getCounter(ResidualCounter.RESIDUE).increment((long)residual);
	}
}