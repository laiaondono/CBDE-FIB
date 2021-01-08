package cbde.labs.hbase_mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Selection extends JobMapReduce {

	public static class SelectionMapper extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// Obtain the parameters sent during the configuration of the job
			String selection = context.getConfiguration().getStrings("selection")[0];
			String type = context.getConfiguration().getStrings("type1")[0];
			// Since the value is a CSV, just get the lines split by commas
			String[] values = value.toString().split(",");
			// Obtenim el valor de l'atribut "type"
			String selectionValue = Utils.getAttribute(values, selection);
			// Si el valor és el que ens interessa per a la selecció, escrivim els valors de la fila
			if (selectionValue.equals(type))
				context.write(key, value);
		}
	}
    
	public Selection() {
		this.input = null;
		this.output = null;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "Selection");
		configureJob(job,this.input, this.output);
	    // Let's run it!
	    return job.waitForCompletion(true);
	}

    public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Selection.class);

        // Configure the rest of parameters required for this job

		// Set the mapper class it must use
		job.setMapperClass(SelectionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// No combiner or reducer classes for the selection

		// The output will be Text and Text
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// The files and formats the job will read from/write to
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(pathIn));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));
		// These are the parameters that we are sending to the job
		job.getConfiguration().setStrings("selection", "type"); // Ens interessa un valor determinat de l'atribut "type"
		job.getConfiguration().setStrings("type1", "type_1"); // El valor que ens interessa és "type_1"
    }
}
