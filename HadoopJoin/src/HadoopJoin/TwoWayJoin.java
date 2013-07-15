package HadoopJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The TwoWayJoin class, which implements the hadoop Map/Reduce implementation
 * of joining two relations on a single attribute, designed to be able to run
 * consecutive two way joins on a series of relations.
 */
public class TwoWayJoin {
	/**
	 * The Mapper class implementation of the TwoWayJoin class.
	 */
	public static class Map extends
			Mapper<LongWritable, Text, TextPair, TextPair> {

		/**
		 * Emits, for each record read, a tuple with a <i>key element</i> with a
		 * value of the form <i>TextPair(Join_Attribute_Value,
		 * Relation_Name)</i> and a <i>value element</i> with a value of the
		 * form <i>TextPair(Relation_Name, Non_Join_Attribute_Values)</i>.
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
		 *      java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split(" ");
			// Read index of join field for relation with name line[0].
			String joinFieldIndexString = context.getConfiguration().get(
					line[0].concat("JoinFieldIndex"));
			if (joinFieldIndexString != null) {
				int joinFieldIndex = Integer.parseInt(joinFieldIndexString);
				context.write(
						new TextPair(line[joinFieldIndex], line[0]),
						new TextPair(line[0], joinStringArray(line, 0,
								joinFieldIndex)));
			}
		}
	}

	/**
	 * The Partitioner class implementation of the TwoWayJoin class.
	 */
	public static class Partition extends Partitioner<TextPair, TextPair> {

		/**
		 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object,
		 *      java.lang.Object, int)
		 */
		@Override
		public int getPartition(TextPair key, TextPair value, int numPartitions) {
			return (key.getFirstValue().hashCode() & Integer.MAX_VALUE
					% numPartitions);
		}
	}

	/**
	 * The Reducer class implementation of the TwoWayJoin class.
	 */
	public static class Reduce extends Reducer<TextPair, TextPair, Text, Text> {

		/**
		 * Receives <i>(Key, Value)</i> tuples of the form
		 * <i>(TextPair(Join_Attribute_Value, Relation_Name),
		 * [TextPair(Relation_Name, Non_Join_Attribute_Values), ...]</i>).
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
		 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(TextPair key, Iterable<TextPair> values,
				Context context) throws IOException, InterruptedException {
			ArrayList<String> firstRelationValues = new ArrayList<String>();
			Iterator<TextPair> valuesIterator = values.iterator();
			TextPair value;
			String firstRelationName = context.getConfiguration().get(
					"FirstRelationName"), secondRelationName = context
					.getConfiguration().get("SecondRelationName"), keyRelationValue = key
					.getFirstValue().toString(), keyRelationName = key
					.getSecondValue().toString(), valueRelationName, valueRelationValue;
			while (valuesIterator.hasNext()) {
				value = valuesIterator.next();
				valueRelationName = value.getFirstValue().toString();
				valueRelationValue = value.getSecondValue().toString();
				// Check whether the file contains a record of a relation not
				// participating in the join process and output it unchanged to
				// the intermediate join result.
				if (!valueRelationName.equals(firstRelationName)
						&& !valueRelationName.equals(secondRelationName)) {
					String joinFieldIndexString = context.getConfiguration()
							.get(valueRelationName.concat("JoinFieldIndex"));
					if (joinFieldIndexString != null) {
						int joinFieldIndex = Integer
								.parseInt(joinFieldIndexString);
						ArrayList<String> valueRelationValueParts = new ArrayList<String>();
						String[] valueRelationValuePartsList = valueRelationValue
								.split(" ");
						for (String valueRelationValuePart : valueRelationValuePartsList) {
							valueRelationValueParts.add(valueRelationValuePart);
						}
						valueRelationValueParts.add(0, valueRelationName);
						valueRelationValueParts.add(joinFieldIndex,
								keyRelationValue);
						context.write(
								null,
								new Text(
										joinStringArray(valueRelationValueParts
												.toArray(valueRelationValuePartsList))));
					}
					continue;
				}
				if (valueRelationName.equals(keyRelationName)) {
					// If the value belongs to the first join relation store it
					// with the rest.
					firstRelationValues.add(valueRelationValue);
				}
				// If the value belongs to the second join relation, for each
				// value belonging to the first join relation, combine each of
				// the second with the first.
				else {
					for (String firstRelationValue : firstRelationValues) {
						String joinFieldIndexString = context
								.getConfiguration().get(
										keyRelationName
												.concat("JoinFieldIndex"));
						if (joinFieldIndexString != null) {
							int joinFieldIndex = Integer
									.parseInt(joinFieldIndexString);
							ArrayList<String> keyRelationValueParts = new ArrayList<String>();
							String[] keyRelationValuePartsList = firstRelationValue
									.split(" ");
							for (String keyRelationValuePart : keyRelationValuePartsList) {
								keyRelationValueParts.add(keyRelationValuePart);
							}
							keyRelationValueParts.add(
									0,
									context.getConfiguration().get(
											"ResultRelationName"));
							keyRelationValueParts.add(joinFieldIndex,
									keyRelationValue);
							keyRelationValueParts.add(valueRelationValue);
							context.write(
									null,
									new Text(
											joinStringArray(keyRelationValueParts
													.toArray(keyRelationValuePartsList))));
						}
					}
				}
			}
		}
	}

	/**
	 * Joins a string array into a single space delimited string excluding only
	 * the elements of the string array that have been passed as the vararg
	 * parameter <i>exclusionIndices</i>.
	 * 
	 * @param stringArray
	 *            The string array containing the non join attribute values of a
	 *            record.
	 * @param exclusionIndices
	 *            The exclusion indices that will not contribute to the returned
	 *            string.
	 * @return The string representing the joined non join attribute values of a
	 *         record.
	 */
	private static String joinStringArray(String[] stringArray,
			int... exclusionIndices) {
		String result = "";
		if (stringArray == null) {
			throw new IllegalArgumentException(
					"joinStringArray: null string array passed");
		}
		outerLoop: for (int i = 0; i < stringArray.length; i += 1) {
			for (int j = 0; j < exclusionIndices.length; j += 1) {
				if (exclusionIndices[j] == i) {
					continue outerLoop;
				}
			}
			result = result.concat(result.equals("") ? "" : " ").concat(
					stringArray[i]);
		}
		return (result);
	}
}