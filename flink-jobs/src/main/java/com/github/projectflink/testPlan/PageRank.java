package com.github.projectflink.testPlan;


import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

@SuppressWarnings("serial")
public class PageRank {

	private static final double DAMPENING_FACTOR = 0.85;

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Tuple2<Integer, Integer>> linksInput =  env.readCsvFile(linksInputPath)
			.fieldDelimiter(' ')
			.lineDelimiter("\n")
			.types(Integer.class, Integer.class);

		// build adjacency list from link input
		DataSet<Tuple2<Integer, Integer[]>> adjacencyListInput =
			linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

		// assign initial rank to pages
		DataSet<Tuple2<Integer, Double>> pagesWithRanks = adjacencyListInput.
			map(new RankAssigner((1.0d / numPages)));

		// set iterative data set
		IterativeDataSet<Tuple2<Integer, Double>> iteration = pagesWithRanks.iterate(maxIterations);

		DataSet<Tuple2<Integer, Double>> newRanks = iteration
			// join pages with outgoing edges and distribute rank
			.join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
				// collect and sum ranks
			.groupBy(0).aggregate(SUM, 1)
				// apply dampening factor
			.map(new Dampener(DAMPENING_FACTOR, numPages));

		DataSet<Tuple2<Integer, Double>> finalPageRanks = iteration.closeWith(newRanks);

		// emit result
		finalPageRanks.writeAsCsv(outputPath, "\n", " ");

		// execute program
		env.execute("Basic Page Rank Example");

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * A map function that assigns an initial rank to all pages.
	 */
	public static final class RankAssigner implements MapFunction<Tuple2<Integer, Integer[]>, Tuple2<Integer, Double>> {
		Tuple2<Integer, Double> outPageWithRank;

		public RankAssigner(double rank) {
			this.outPageWithRank = new Tuple2<Integer, Double>(-1, rank);
		}

		@Override
		public Tuple2<Integer, Double> map(Tuple2<Integer, Integer[]> page) {
			outPageWithRank.f0 = page.f0;
			return outPageWithRank;
		}
	}

	/**
	 * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
	 * originate. Run as a pre-processing step.
	 */
	@ConstantFields("0")
	public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer[]>> {

		private final ArrayList<Integer> neighbors = new ArrayList<Integer>();

		@Override
		public void reduce(Iterable<Tuple2<Integer, Integer>> values, Collector<Tuple2<Integer, Integer[]>> out) {
			neighbors.clear();
			Integer id = 0;

			for (Tuple2<Integer, Integer> n : values) {
				id = n.f0;
				neighbors.add(n.f1);
			}
			out.collect(new Tuple2<Integer, Integer[]>(id, neighbors.toArray(new Integer[neighbors.size()])));
		}
	}

	/**
	 * Join function that distributes a fraction of a vertex's rank to all neighbors.
	 */
	public static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Integer, Double>, Tuple2<Integer, Integer[]>>, Tuple2<Integer, Double>> {

		@Override
		public void flatMap(Tuple2<Tuple2<Integer, Double>, Tuple2<Integer, Integer[]>> value, Collector<Tuple2<Integer, Double>> out){
			Integer[] neigbors = value.f1.f1;
			double rank = value.f0.f1;
			double rankToDistribute = rank / ((double) neigbors.length);

			for (int i = 0; i < neigbors.length; i++) {
				out.collect(new Tuple2<Integer, Double>(neigbors[i], rankToDistribute));
			}
		}
	}

	/**
	 * The function that applies the page rank dampening formula
	 */
	@ConstantFields("0")
	public static final class Dampener implements MapFunction<Tuple2<Integer,Double>, Tuple2<Integer,Double>> {

		private final double dampening;
		private final double randomJump;

		public Dampener(double dampening, double numVertices) {
			this.dampening = dampening;
			this.randomJump = (1 - dampening) / numVertices;
		}

		@Override
		public Tuple2<Integer, Double> map(Tuple2<Integer, Double> value) {
			value.f1 = (value.f1 * dampening) + randomJump;
			return value;
		}
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	private static String linksInputPath = null;
	private static String outputPath = null;
	private static Integer numPages = 0;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] args) {
		if(args.length == 4) {
			linksInputPath = args[0];
			outputPath = args[1];
			numPages = Integer.parseInt(args[2]);
			maxIterations = Integer.parseInt(args[3]);
			return true;
		} else {
			System.err.println("Usage: PageRankBasic <links path> <output path> <num pages> <num iterations>");
			return false;
		}
	}
}
