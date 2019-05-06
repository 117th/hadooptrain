import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Cat {
	
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) throws Exception {
		
		InputStream in = null;
		
		if(args.length != 1){
			System.err.println("Wrong arguments");
			System.exit(-1);
		}

		
		//String test = "hdfs://localhost/user/cloudera/test/test.txt";
		
		try{
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
	
}
