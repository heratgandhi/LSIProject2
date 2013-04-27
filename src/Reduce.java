import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	public static enum ResidualCounter {
        RESIDUE
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
		    //System.out.println("dfsf:" + cpr.get(key).doubleValue());
		    val = Math.abs(tpr.get(key).doubleValue() - cpr.get(key).doubleValue());
		    if(cpr.get(key).doubleValue() != 0) {
		    	val /= cpr.get(key).doubleValue();
		    }
		    residual += val;
		    cnt++;
		}
		residual /= cnt;
		System.out.println("Reducer block residue: "+residual);
		if(residual <= 0.001) {
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
		Hashtable<Long, String> info = new Hashtable<Long,String>();
		String[] parts = null;
		
		//System.out.println("Got :" + key+ " ");
		ArrayList<String> values1 = new ArrayList<String>();
		
		for(Text val : values) {
			//System.out.println("Value: "+val.toString());
			values1.add(val.toString());
			if(val.charAt(0) == 'p') {
				//Page rank computation values
				parts = val.toString().split(";");
				ppr.put(new Long(parts[2]), new Double(Double.parseDouble(parts[3])));
				tpr.put(new Long(parts[2]), new Double(Double.parseDouble(parts[3])));
				degree.put(new Long(parts[2]), new Double(Double.parseDouble(parts[4])));
			} else {
				//Information
				parts = val.toString().split(";");
				info.put(new Long(parts[1]), parts[2]);
				ppr.put(new Long(parts[1]), new Double(Double.parseDouble(parts[2].split(" ")[0])) );
				tpr.put(new Long(parts[1]), new Double(Double.parseDouble(parts[2].split(" ")[0])) );
				degree.put(new Long(parts[1]), new Double(parts[2].split(" ").length-1));
			}
		}
		double division = 0;
		boolean first = true;
		Enumeration<Long> enumKey;
		do {
			if(!first) {
				//System.out.println("Copying...");
				enumKey = cpr.keys();
				while(enumKey.hasMoreElements()) {
				    Long keyv = enumKey.nextElement();
				    tpr.put(keyv,cpr.get(keyv));
				}				
			}			
			for(String val1 : values1) {
				System.out.println("Value: " + val1);
				if(val1.charAt(0) == 'p') {
					parts = val1.toString().split(";");
					division = tpr.get(new Long(parts[2])).floatValue() / degree.get(new Long(parts[2])).floatValue();
					System.out.println("Division:" + division);
					if(cpr.get(new Long(parts[1])) != null) {
						System.out.println("Sum:" + (cpr.get(new Long(parts[1])).floatValue() + division));
						cpr.put(new Long( parts[1] ), new Double( cpr.get(new Long(parts[1])).floatValue() + division  ));
						System.out.println("Check sum:" + cpr.get(new Long(parts[1])).floatValue());
					} else {
						cpr.put(new Long( parts[1] ), new Double( division ));
						System.out.println("Check sum 0:" + cpr.get(new Long(parts[1])).floatValue());
					}
				}
			}
			enumKey = cpr.keys();
			while(enumKey.hasMoreElements()) {
			    Long keyv = enumKey.nextElement();
			    //System.out.println(keyv+" "+tpr.get(keyv));
			    cpr.put(keyv, (0.15/PageRank.nodes) + (0.85 * cpr.get(keyv).doubleValue()) );
			}
			first = false;
			//System.out.println("degree");
			//printTable(degree);
			System.out.println("tpr");
			printTable(tpr);
			System.out.println("cpr");
			printTable(cpr);
		} while(!checkconvergence(tpr,cpr));
		
		enumKey = cpr.keys();
		double residual = 0;
		double val;
		while(enumKey.hasMoreElements()) {
		    Long keyv = enumKey.nextElement();
		    val = Math.abs(cpr.get(keyv).doubleValue() - ppr.get(keyv).doubleValue());
		    val /= cpr.get(keyv).doubleValue();
		    residual += val;
		    //System.out.println(key+" "+keyv);
		    context.write(key, new Text(keyv+" "+cpr.get(keyv)+" "+degree.get(keyv).intValue()+ " " + info.get(keyv).substring((info.get(keyv).indexOf(" ")+1))) );
		}
		System.out.println("Residual:" + residual);
		residual *= PageRank.multiplication_factor; //To map residual to the long rank
		context.getCounter(ResidualCounter.RESIDUE).increment((long)residual);
	}
}