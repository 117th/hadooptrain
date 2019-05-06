
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class Cat {
	
	public static void main(String[] args) throws Exception {		
		
		if(args.length != 1){
			System.err.println("Wrong arguments");
			System.exit(-1);
		}

		String uri = args[0];
		
		//String uri = "hdfs://quickstart.cloudera/user/cloudera/test/test.txt";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		FSDataInputStream in = null;
		
		String test;
		
		try{
			in = fs.open(new Path(uri));
			Scanner scan = new Scanner(in).useDelimiter("\\n");
			/*test = scan.hasNext() ? scan.next() : "";
			System.out.println(test);
			test = scan.hasNext() ? scan.next() : "";
			System.out.println(test);*/
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
	
}
