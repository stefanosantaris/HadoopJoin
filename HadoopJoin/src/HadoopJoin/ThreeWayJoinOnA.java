package HadoopJoin;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The ThreeWayJoinOnA class, which implements the hadoop Map/Reduce
 * implementation of joining three relations, in a star topology, on a single
 * attribute per relation which is common to all the relations, and it is
 * designed to be able to perform a single pass join of the relations in
 * question.
 */
public class ThreeWayJoinOnA {

	/**
	 * The Mapper class implementation of the ThreeWayJoinOnA class.
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		/**
		 * Emits, for each record read, a tuple with a <i>key element</i> with a
		 * value of the form <i>IntWritable(JoinAttributeValue)</i> and a
		 * <i>value element</i> with a value of the form
		 * <i>Text("NonJoinAttributeValues_RelationName")</i>.
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
		 *      java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split(" ");
			// First parameter contains the map-key <a> and the second one the
			// value and the relation's name <b R>
			context.write(new Text(line[1]), new Text(line[2].concat(" ")
					.concat(line[0])));
		}
	}

	/**
	 * The Reducer class implementation of the ThreeWayJoinOnA class.
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		/**
		 * Receives <i>(Key, Value)</i> tuples of the form
		 * </i>(Text("JoinAttributeValue"),
		 * [Text("NonJoinAttributeValues_RelationName"), ...])</i>.
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
		 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIterator = values.iterator();
			// Sets which contain the value of the tuples removing the
			// duplicates
			Set<String> bSet = new HashSet<String>();
			Set<String> cSet = new HashSet<String>();
			Set<String> dSet = new HashSet<String>();
			while (valuesIterator.hasNext()) {
				String[] valueArray = valuesIterator.next().toString()
						.split(" ");
				String relation = valueArray[1];
				String bcd = valueArray[0].toString();
				if (relation.equals("R")) {
					bSet.add(bcd);
				} else if (relation.equals("S")) {
					cSet.add(bcd);
				} else if (relation.equals("T")) {
					dSet.add(bcd);
				}
			}
			// Output the values whenever values of each relation matches
			if (!bSet.isEmpty() && !cSet.isEmpty() && !dSet.isEmpty()) {
				for (String b : bSet) {
					for (String c : cSet) {
						for (String d : dSet) {
							context.write(
									new Text("R|><|S|><|T"),
									new Text(key.toString().concat(" ")
											.concat(b).concat(" ").concat(c)
											.concat(" ").concat(d)));
						}
					}
				}
			}
		}
	}
}