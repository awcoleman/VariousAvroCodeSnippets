package com.awcoleman.StandaloneJava;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
/*
 * !! AvroCombinerByBlock should be used instead !! Remove this warning when AvroCombinerByRecord has been updated to AvroCombinerByBlock standards.
 * 
 * 
 * 
 * @author awcoleman
 * license: Apache License 2.0; http://www.apache.org/licenses/LICENSE-2.0
 *
 */
public class AvroCombinerByRecord {

	public AvroCombinerByRecord(String inDirStr, String outDirStr) throws IOException {

		//Get list of input files
		ArrayList<FileStatus> inputFileList = new ArrayList<FileStatus>();

		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

		FileSystem hdfs = FileSystem.get(conf);

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

			if ( fileStatus.isFile() ) {
				inputFileList.add((FileStatus) fileStatus);
			}
		}

		if (inputFileList.size()<=1) {
			System.out.println("Only one or zero files found in input directory ( "+inDirStr+" ). Exiting.");
			System.exit(1);
		}

		//Get Schema and Compression Codec from seed file since we need it for the writer
		Path firstFile = inputFileList.get(0).getPath();
		FsInput fsin = new FsInput(firstFile, conf);
		DataFileReader<Object> dfrFirstFile = new DataFileReader<Object>(fsin,new GenericDatumReader<Object>());
		Schema fileSchema = dfrFirstFile.getSchema();
		String compCodecName = dfrFirstFile.getMetaString("avro.codec");
		dfrFirstFile.close();

		//Create Empty HDFS file in output dir
		Path seedFile = new Path(outDirStr+"/combinedByRecord.avro" );
		FSDataOutputStream hdfsdos = hdfs.create(seedFile, false);

		//Append other files
		GenericDatumWriter gdw = new GenericDatumWriter(fileSchema);
		DataFileWriter dfwBase = new DataFileWriter(gdw);
		//Set compression to that found in the first file
		dfwBase.setCodec( CodecFactory.fromString(compCodecName) );
		
		DataFileWriter dfw = dfwBase.create(fileSchema, hdfsdos);
		
		
		for (FileStatus thisFileStatus:inputFileList) {

			DataFileStream<Object> avroStream = null;
			FSDataInputStream inStream = hdfs.open( thisFileStatus.getPath() );
			GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
			avroStream = new DataFileStream<Object>(inStream, reader);

			long recordCounter=0;
			while (avroStream.hasNext()) {
				dfw.append( avroStream.next() );
				
				recordCounter++;
			}
			avroStream.close();
			inStream.close();

			System.out.println("Appended "+recordCounter+" records from "+thisFileStatus.getPath().getName()+" to "+seedFile.getName());
		}


		dfw.close();
		dfwBase.close();
	}

	public static void main(String[] args) throws IOException {
		
		System.out.println("!! AvroCombinerByBlock should be used instead !! Remove this warning when AvroCombinerByRecord has been updated to AvroCombinerByBlock standards.");
		
		if (args.length < 2 ) {
			System.out.println("Requires an input directory (containing Avro files) and an output directory to place the combined files. Exiting.");
			System.exit(1);
		}

		AvroCombinerByRecord mainObj = new AvroCombinerByRecord(args[0], args[1]);

	}

}