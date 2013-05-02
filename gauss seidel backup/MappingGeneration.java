import java.io.BufferedReader;
import java.io.FileReader;


public class MappingGeneration {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			BufferedReader br = new BufferedReader(new FileReader("blocks.txt"));
			String line;
			int no = 0;
			while((line = br.readLine()) != null) {
				System.out.println("blocklimits["+no+"] = "+line.trim()+";");
				no++;
			}
			br.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
