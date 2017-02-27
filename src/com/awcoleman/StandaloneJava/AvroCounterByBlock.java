package com.awcoleman.StandaloneJava;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
/*
 * 
 * Standalone java program that counts Avro records in hdfs file or directory.
 * 
 * 
 * //TODO-- expand to handle local fs or hdfs. Look at how Hadoop handles URIs
 *    also if no URI is specified check HDFS first and if no match, check localfs
 * 
 * @author awcoleman
 * license: Apache License 2.0; http://www.apache.org/licenses/LICENSE-2.0
 */
public class AvroCounterByBlock {
	
	public AvroCounterByBlock(String inDirStr) throws IOException {
		
		long numAvroRecords=0;
		
		//Get list of input files
		ArrayList<FileStatus> inputFileList = new ArrayList<FileStatus>();
		
		Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.set("dfs.replication", "1");  //see http://stackoverflow.com/questions/24548699/how-to-append-to-an-hdfs-file-on-an-extremely-small-cluster-3-nodes-or-less

        FileSystem hdfs = null;
        try {
        	hdfs = FileSystem.get(conf);
        } catch (java.io.IOException ioe) {
        	System.out.println("Error opening HDFS filesystem. Exiting. Error message: "+ioe.getMessage());
			System.exit(1);
        }
		if (hdfs.getStatus()==null) {
			System.out.println("Unable to contact HDFS filesystem. Exiting.");
			System.exit(1);
		}

		//Check if input dirs/file exists and get file list (even if list of single file)
		Path inPath = new Path(inDirStr);
		if ( hdfs.exists(inPath) && hdfs.isFile(inPath) ) {  //single file
			inputFileList.add( hdfs.getFileStatus(inPath) );
		} else if ( hdfs.exists(inPath) && hdfs.isDirectory(inPath) ) { //dir
			//Get list of input files
		    RemoteIterator<LocatedFileStatus> fileStatusListIterator = hdfs.listFiles(inPath, true);
		    while(fileStatusListIterator.hasNext()){
		        LocatedFileStatus fileStatus = fileStatusListIterator.next();
		        
		        if ( fileStatus.isFile() && !fileStatus.getPath().getName().equals("_SUCCESS") ) {
		        	inputFileList.add((FileStatus) fileStatus);
		        }
		    }
		} else {
			System.out.println("Input directory ( "+inDirStr+" ) not found or is not directory. Exiting.");
			System.exit(1);
		}   
	      
	    for (FileStatus thisFileStatus:inputFileList) {
	    	
	    	//_SUCCESS files are 0 bytes
	    	if ( thisFileStatus.getLen() == 0 ) {
	    		continue;
	    	}
		    
	    	DataFileStream<Object> dfs = null;
			FSDataInputStream inStream = hdfs.open( thisFileStatus.getPath() );
	    	GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
			dfs = new DataFileStream<Object>(inStream, reader);
		    
		    
		    long thisFileRecords=0;
		    while ( dfs.hasNext() ) {
		    	
		    	numAvroRecords = numAvroRecords + dfs.getBlockCount();	
		    	thisFileRecords = thisFileRecords + dfs.getBlockCount();

			    //System.out.println("Input file "+thisFileStatus.getPath()+" getBlockCount() is "+dfs.getBlockCount()+"." );

		    	dfs.nextBlock();
		    }
		      
		    System.out.println("Input file "+thisFileStatus.getPath()+" has "+thisFileRecords+" records.");
	    
		    dfs.close();
			inStream.close();

	    	//TODO test on dir with non-avro file and see what the exception is, catch that and log to output but don't die.
	    }
	    
		System.out.println("Input dir/file ( "+inDirStr+" ) has "+inputFileList.size()+" files and "+numAvroRecords+" total records.");

	}

	public static void main(String[] args) throws IOException {
		if (args.length < 1 ) {
			System.out.println("Requires an input directory (containing Avro files) or Avro filename. Exiting.");
			System.exit(1);
		}
		
		AvroCounterByBlock mainObj = new AvroCounterByBlock(args[0]);

	}

}