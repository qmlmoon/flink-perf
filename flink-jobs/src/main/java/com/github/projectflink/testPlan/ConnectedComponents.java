/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.github.projectflink.testPlan;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * A copy from https://github.com/apache/incubator-flink. The only difference is that it doesn't
 * require a vertex input file.
 */
@SuppressWarnings("serial")
public class ConnectedComponents implements ProgramDescription {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String... args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> edges = env
			.readCsvFile(edgesPath)
			.fieldDelimiter(' ')
			.types(Long.class, Long.class)
			.flatMap(new UndirectEdge());

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = edges
			.groupBy(0)
			.reduceGroup(new InitialValue());

		// open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
			verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);

		// apply the step logic: join with the edges, select the minimum neighbor, update if the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset().join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
			.groupBy(0).aggregate(Aggregations.MIN, 1)
			.join(iteration.getSolutionSet()).where(0).equalTo(0)
			.with(new ComponentIdFilter());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);

		// emit result
		result.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("Connected Components Example");
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Function that initial the connected components with its own id.
	 */
	public static final class InitialValue implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> t, Collector<Tuple2<Long, Long>> c) throws Exception {
			Long v = t.iterator().next().f0;
			c.collect(new Tuple2<Long, Long>(v, v));
		}
	}



	/**
	 * Undirected edges by emitting for each input edge the input edges itself and an inverted version.
	 */
	public static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

		@Override
		public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
			invertedEdge.f0 = edge.f1;
			invertedEdge.f1 = edge.f0;
			out.collect(edge);
			out.collect(invertedEdge);
		}
	}

	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that
	 * a vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	@ConstantFieldsFirst("1 -> 0")
	@ConstantFieldsSecond("1 -> 1")
	public static final class NeighborWithComponentIDJoin implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	}



	@ConstantFieldsFirst("0")
	public static final class ComponentIdFilter implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old, Collector<Tuple2<Long, Long>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
	}



	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>";
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] programArguments) {

		// parse input arguments
		if(programArguments.length == 3) {
			edgesPath = programArguments[0];
			outputPath = programArguments[1];
			maxIterations = Integer.parseInt(programArguments[2]);
			return true;
		} else {
			System.err.println("Usage: ConnectedComponents <edges path> <result path> <max number of iterations>");
			return false;
		}
	}


}