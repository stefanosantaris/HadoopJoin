package HadoopJoin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Main HadoopJoin class where the type of the join is defined and the hadoop
 * jobs are committed for execution.
 */
public class HadoopJoin extends Configured implements Tool {

	/**
	 * The parameters needed to optimize the three way chained join.
	 */
	private static int k, r, s, t, b, c;

	/**
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		boolean result = false;
		if (args[0].toLowerCase().equals("3")) {
			Configuration conf = new Configuration();
			getParameters(conf, args[3]);
			conf.set("k", String.valueOf(k));
			conf.set("r", String.valueOf(r));
			conf.set("s", String.valueOf(s));
			conf.set("t", String.valueOf(t));
			conf.set("b", String.valueOf(b));
			conf.set("c", String.valueOf(c));
			Job job = new Job(conf);
			job.setJobName("ThreeWayJoin");
			job.setJarByClass(HadoopJoin.class);
			job.setMapperClass(ThreeWayJoin.Map.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setPartitionerClass(ThreeWayJoin.Partition.class);
			job.setReducerClass(ThreeWayJoin.Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(k);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputPaths(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			result = job.waitForCompletion(true);
		} else if (args[0].toLowerCase().equals("3a")) {
			Job job = new Job(new Configuration());
			job.setJobName("ThreeWayJoinOnA");
			job.setJarByClass(HadoopJoin.class);
			job.setMapperClass(ThreeWayJoinOnA.Map.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(ThreeWayJoinOnA.Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(2);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputPaths(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			result = job.waitForCompletion(true);
		} else if (args[0].toLowerCase().equals("2")) {
			Path inputPath = null, temporaryInputPath = new Path(
					"TemporaryInputDirectory"), temporaryOutputPath = new Path(
					"TemporaryOutputDirectory");
			String[] relationData = args[1].split(","), relationNames = new String[relationData.length / 2], resultRelationJoinFieldIndices = new String[relationNames.length - 2];
			Configuration configuration = new Configuration();
			// Parse the data regarding the relations participating in the join
			// process in the form
			// Relation_1_Name,Relation_1_Join_Field_Index,Relation_2_Name,Relation_2_Join_Field_Index[-Result_2_Join_Field_Index,Relation_3_Name,Relation_3_Join_Field_Index...].
			for (int i = 0, relationIndex = 0, resultRelationIndex = 0; i < relationData.length; i += 2) {
				relationNames[relationIndex] = relationData[i];
				if (relationData[i + 1].contains("-")) {
					String[] relationDataIndexParts = relationData[i + 1]
							.split("-");
					configuration.set(relationNames[relationIndex]
							.concat("JoinFieldIndex"),
							relationDataIndexParts[0]);
					resultRelationJoinFieldIndices[resultRelationIndex] = relationDataIndexParts[1];
					resultRelationIndex += 1;
				} else {
					configuration.set(relationNames[relationIndex]
							.concat("JoinFieldIndex"), relationData[i + 1]);
				}
				relationIndex += 1;
			}
			// Sort the array holding the relation names in the same way the
			// sort comparator class will after the partitioning phase in order
			// to assure the functionality of the reducer class.
			Arrays.sort(relationNames);
			FileSystem fileSystem = FileSystem.get(configuration);
			Job job;
			// Interchange the relation names passed to the TwoWayJoin class and
			// the input and output paths before the beginning of consecutive
			// executions of the join process.
			for (int relationIndex = 1; relationIndex < relationNames.length; relationIndex += 1) {
				if (relationIndex == 1) {
					configuration.set("FirstRelationName", relationNames[0]);
					inputPath = new Path(args[2]);
				} else {
					configuration.set("FirstRelationName",
							configuration.get("ResultRelationName"));
					configuration.set(configuration.get("FirstRelationName")
							.concat("JoinFieldIndex"),
							resultRelationJoinFieldIndices[relationIndex - 2]);
					if (fileSystem.exists(temporaryInputPath)) {
						fileSystem.delete(temporaryInputPath, true);
					} else {
						inputPath = new Path(temporaryInputPath.getName());
					}
					fileSystem.rename(temporaryOutputPath, inputPath);
				}
				configuration.set("SecondRelationName",
						relationNames[relationIndex]);
				configuration
						.set("ResultRelationName",
								configuration
										.get("FirstRelationName")
										.concat("|><|")
										.concat(configuration
												.get("SecondRelationName")));
				job = new Job(configuration);
				job.setJobName("TwoWayJoin");
				job.setJarByClass(HadoopJoin.class);
				job.setMapperClass(TwoWayJoin.Map.class);
				job.setMapOutputKeyClass(TextPair.class);
				job.setMapOutputValueClass(TextPair.class);
				job.setPartitionerClass(TwoWayJoin.Partition.class);
				// Use custom sorting and grouping classes to assure the correct
				// Map/Reduce way of operation.
				job.setSortComparatorClass(TextPair.TextPairComparator.class);
				job.setGroupingComparatorClass(TextPair.TextPairFirstValueComparator.class);
				job.setReducerClass(TwoWayJoin.Reduce.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				FileInputFormat.setInputPaths(job, inputPath);
				FileOutputFormat.setOutputPath(job, temporaryOutputPath);
				result = job.waitForCompletion(true);
			}
			if (fileSystem.exists(temporaryInputPath)) {
				fileSystem.delete(temporaryInputPath, true);
			}
			fileSystem.rename(temporaryOutputPath, new Path(args[3]));
		}
		if (result) {
			return (0);
		}
		return (1);
	}

	/**
	 * Gets the three way join optimization parameters from an hdfs mounted
	 * properties file.
	 * 
	 * @param conf
	 *            The org.apache.hadoop.conf.Configuration used by the
	 *            org.apache.hadoop.mapreduce.Job object.
	 * @param filePath
	 *            The file path to the hdfs mounted properties file for the
	 *            three way join optimization parameters.
	 */
	private static void getParameters(Configuration conf, String filePath) {
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path(filePath));
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] lineBuffer = line.split(" ");
				if (lineBuffer[0].equals("k")) {
					k = Integer.parseInt(lineBuffer[1]);
				} else if (lineBuffer[0].equals("r")) {
					r = Integer.parseInt(lineBuffer[1]);
				} else if (lineBuffer[0].equals("s")) {
					s = Integer.parseInt(lineBuffer[1]);
				} else if (lineBuffer[0].equals("t")) {
					t = Integer.parseInt(lineBuffer[1]);
				}
			}
			reader.close();
			if ((r != 0) && (t != 0)) {
				b = (int) Math.sqrt(k * ((float) t / (float) r));
				if (b == 0) {
					throw new IllegalArgumentException(
							"Cannot proceed to the join operation with the given argument k ("
									.concat(String.valueOf(k)).concat(")."));
				}
				c = (int) Math.sqrt(k * ((float) r / (float) t));
				if (c == 0) {
					throw new IllegalArgumentException(
							"Cannot proceed to the join operation with the given argument k ("
									.concat(String.valueOf(k)).concat(")."));
				}
			}
		} catch (IllegalArgumentException illegalArgumentException) {
			System.out.println("Exception occured: "
					.concat(illegalArgumentException.getMessage()));
			System.exit(0);
		} catch (FileNotFoundException e) {
			System.out.println("Exception occured: ".concat(e.getMessage()));
			System.exit(0);
		} catch (IOException e) {
			System.out.println("Exception occured: ".concat(e.getMessage()));
			System.exit(0);
		}
	}

	/**
	 * The HadoopJoin main method.
	 * 
	 * @param args
	 *            The HadoopJoin arguments.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out
					.println("HadoopJoin usage:\n\n"
							.concat(" Two way join:                    hadoop jar HadoopJoin.jar HadoopJoin.HadoopJoin 2 relation_1_name,relation_1_join_field_index,relation_2_name,relation_2_join_field_index[-result_2_join_field_index,relation_3_name,relation_3_join_field_index...] input_hdfs_folder output_hdfs_folder")
							.concat(" Three way join:                  hadoop jar HadoopJoin.jar HadoopJoin.HadoopJoin 3 input_hdfs_folder output_hdfs_folder properties_hdfs_filepath\n")
							.concat(" Three way join on attribute \"A\": hadoop jar HadoopJoin.jar HadoopJoin.HadoopJoin 3a input_hdfs_folder output_hdfs_folder\n"));
			System.exit(0);
		}
		System.exit(ToolRunner.run(new HadoopJoin(), args));
	}
}