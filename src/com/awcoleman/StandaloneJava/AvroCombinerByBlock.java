package com.awcoleman.StandaloneJava;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
/*
 * 
 * Standalone Java to combine Avro files (by block)
 * 
 * 
 * @author awcoleman
 * license: Apache License 2.0; http://www.apache.org/licenses/LICENSE-2.0
 */
public class AvroCombinerByBlock {

	public static final String DEFAULTOUTPUTFILENAME = "combinedByBlock.avro";
	public boolean isOutputNameSpecifiedAndAFile = false;
	
	public AvroCombinerByBlock(String inDirStr, String outDirStr, String handleExisting) throws IOException {
		
		//handle both an output directory and an output filename (ending with .avro)
		String outputFilename = DEFAULTOUTPUTFILENAME;
		if (outDirStr.endsWith(".avro")) {
			isOutputNameSpecifiedAndAFile = true;
			//String[] outputParts = outDirStr.split(":?\\\\");
			String[] outputParts = outDirStr.split("/");

			outputFilename = outputParts[outputParts.length-1];

			//remove outputFilename from outDirStr to get new outDirStr which is just directory (and trailing /)
			outDirStr = outDirStr.replaceAll(Pattern.quote(outputFilename), "");
			outDirStr = outDirStr.substring(0, outDirStr.length() - (outDirStr.endsWith("/") ? 1 : 0));
		}
		
		//Get block size - not needed
		//long hdfsBlockSize = getBlockSize();
		//System.out.println("HDFS FS block size: "+hdfsBlockSize);

		
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

		//Check if input and output dirs exist
		Path inDir = new Path(inDirStr);
		Path outDir = new Path(outDirStr);
		if ( ! ( hdfs.exists(inDir) || hdfs.isDirectory(inDir) ) ) {
			System.out.println("Input directory ( "+inDirStr+" ) not found or is not directory. Exiting.");
			System.exit(1);
		}
		
		if ( ! ( hdfs.exists(outDir) || hdfs.isDirectory(outDir) ) ) {
			if ( hdfs.exists(outDir) ) { //outDir exists and is a symlink or file, must die
				System.out.println("Requested output directory name ( "+outDirStr+" ) exists but is not a directory. Exiting.");
				System.exit(1);
			} else {
				hdfs.mkdirs(outDir);
			}
		}
		
	    RemoteIterator<LocatedFileStatus> fileStatusListIterator = hdfs.listFiles(inDir, true);
	    while(fileStatusListIterator.hasNext()){
	        LocatedFileStatus fileStatus = fileStatusListIterator.next();
	        
	        if ( fileStatus.isFile() && !fileStatus.getPath().getName().equals("_SUCCESS") ) {
	        	inputFileList.add((FileStatus) fileStatus);
	        }
	    }

	    if (inputFileList.size()<=1 && ! isOutputNameSpecifiedAndAFile) {  //If an output file is specified assume we just want a rename.
	    	System.out.println("Only one or zero files found in input directory ( "+inDirStr+" ). Exiting.");
			System.exit(1);
	    }

	    //Get Schema and Compression Codec from seed file since we need it for the writer
	    Path firstFile = inputFileList.get(0).getPath();
	    FsInput fsin = new FsInput(firstFile, conf);
	    DataFileReader<Object> dfrFirstFile = new DataFileReader<Object>(fsin,new GenericDatumReader<Object>());
	    Schema fileSchema = dfrFirstFile.getSchema();
		String compCodecName = dfrFirstFile.getMetaString("avro.codec");
		//compCodecName should be null, deflate, snappy, or bzip2
		if (compCodecName==null) {
			compCodecName = "deflate";  //set to deflate even though original is no compression
		}
	    dfrFirstFile.close();
	    
	    //Create Empty HDFS file in output dir
	    String seedFileStr = outDirStr+"/"+outputFilename;
	    Path seedFile = new Path( seedFileStr );
	    FSDataOutputStream hdfsdos = null;
	    try {
	    	hdfsdos = hdfs.create(seedFile, false);
	    } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
	    	if (handleExisting.equals("overwrite")) {
	    		hdfs.delete(seedFile, false);
	    		hdfsdos = hdfs.create(seedFile, false);
	    	} else if (handleExisting.equals("append")) {
	    		hdfsdos = hdfs.append(seedFile);
	    	} else {
	    		System.out.println("File "+seedFileStr+" exists and will not overwrite. handleExisting is set to "+handleExisting+". Exiting.");
				System.exit(1);
	    	}
	    }
	    if (hdfsdos == null) {
	    	System.out.println("Unable to create or write to output file ( "+seedFileStr+" ). handleExisting is set to "+handleExisting+". Exiting.");
			System.exit(1);
	    }
	    
	    //Append other files
	    GenericDatumWriter gdw = new GenericDatumWriter(fileSchema);
	    DataFileWriter dfwBase = new DataFileWriter(gdw);
		//Set compression to that found in the first file
		dfwBase.setCodec( CodecFactory.fromString(compCodecName) );
		
	    DataFileWriter dfw = dfwBase.create(fileSchema, hdfsdos);
	    for (FileStatus thisFileStatus:inputFileList) {
	    	
	    	//_SUCCESS files are 0 bytes
	    	if ( thisFileStatus.getLen() == 0 ) {
	    		continue;
	    	}
	    	
	    	FsInput fsin1 = new FsInput(thisFileStatus.getPath(), conf);
		    DataFileReader dfr = new DataFileReader<Object>(fsin1,new GenericDatumReader<Object>());
		    
		    dfw.appendAllFrom(dfr, false);
	    	
	    	dfr.close();
	    }


	    dfw.close();
	    dfwBase.close();


	}

	public long getBlockSize() throws IOException {
		
		Configuration conf = new Configuration();
		
/*		
		//Block Size from XML conf files (is null if not explicitly defined)
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));  //BigTop path, change to yours
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		System.out.println("Default block size from conf xml files: "+conf.get("dfs.blocksize"));  //Default blocksize
*/	

		//FS blocksize
		FileSystem hdfs = FileSystem.get(conf);
		Configuration cconf = hdfs.getConf();
		String block = cconf.get("dfs.blocksize");
		long blocksize = Long.parseLong(block);
		//System.out.println("FS hdfs block size: "+block+" ( "+blocksize+" )");
		
/*		
		//Block Size of file (not valid for directories - returns 0. Not sure how to get default directory block size if overridden from fs default)
		Path myFile = new Path("/myFile");
		FileStatus fileStatus = hdfs.getFileStatus(myFile);
		long fileBlockSize = fileStatus.getBlockSize();
		System.out.println("Input directory block size: "+fileBlockSize);	
*/		
		return blocksize;
	}
	
	public static String convertTime(long time){
		//http://stackoverflow.com/a/6782571
		Date date = new Date(time);
		Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
		return format.format(date).toString();
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 2 ) {
			System.out.println("Requires an input directory (containing Avro files) and an output directory to place the combined files. Exiting.");
			System.exit(1);
		}

		//If last argument is overwrite, then overwrite existing output. If append, then append to output.
		String handleExisting = "exit";
		if (args.length==3) {
			handleExisting = args[2];
		}
		
		AvroCombinerByBlock mainObj = new AvroCombinerByBlock(args[0], args[1], handleExisting);

	}

}