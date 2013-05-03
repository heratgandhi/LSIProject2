import java.io.*;

public class OutDegree {
	public static void main(String[] args) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("edges1.txt"));
			BufferedWriter bw = new BufferedWriter(new FileWriter("outdegrees.txt"));
			
			String line = "";
			long counter = 0;
			long node = 0;
			String[] parts;
			long s;
			while ( (line=br.readLine()) != null ) {
				parts = line.split(" ");
				s = Long.parseLong(parts[0]);
				if(s == node) {
					counter ++;
				} else {
					bw.write(node+" "+counter+"\n");
					node++;
					counter = 1;
				}
			}
			bw.write(node+" "+counter+"\n");
			br.close();
			bw.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
