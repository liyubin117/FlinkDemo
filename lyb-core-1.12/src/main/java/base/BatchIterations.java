package base; // package base;
//
// import org.apache.flink.api.java.DataSet;
// import org.apache.flink.api.java.ExecutionEnvironment;
// import org.apache.flink.api.java.operators.DeltaIteration;
// import org.apache.flink.api.java.tuple.Tuple2;
//
// import java.util.ArrayList;
//
// public class Test {
//    private final static int MAX_ITERATION_NUM = 10;
//
//    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        ArrayList<Tuple2<Integer,Integer>> vertices = new ArrayList();
//        vertices.add(new Tuple2<>(1,1));
//        vertices.add(new Tuple2<>(2,2));
//        vertices.add(new Tuple2<>(3,3));
//        vertices.add(new Tuple2<>(4,4));
//        vertices.add(new Tuple2<>(5,5));
//        vertices.add(new Tuple2<>(6,6));
//        vertices.add(new Tuple2<>(7,7));
//        vertices.add(new Tuple2<>(8,8));
//        vertices.add(new Tuple2<>(9,9));
//        vertices.add(new Tuple2<>(10,10));
//        vertices.add(new Tuple2<>(11,11));
//        vertices.add(new Tuple2<>(12,12));
//        vertices.add(new Tuple2<>(13,13));
//        vertices.add(new Tuple2<>(14,14));
//        vertices.add(new Tuple2<>(15,15));
//
//        ArrayList<Tuple2<Integer,Integer>> edges = new ArrayList();
//        vertices.add(new Tuple2<>(1,2));
//        vertices.add(new Tuple2<>(2,3));
//        vertices.add(new Tuple2<>(2,4));
//        vertices.add(new Tuple2<>(4,5));
//        vertices.add(new Tuple2<>(6,7));
//        vertices.add(new Tuple2<>(5,8));
//        vertices.add(new Tuple2<>(9,10));
//        vertices.add(new Tuple2<>(9,11));
//        vertices.add(new Tuple2<>(8,12));
//        vertices.add(new Tuple2<>(10,13));
//        vertices.add(new Tuple2<>(1,14));
//        vertices.add(new Tuple2<>(11,15));
//
//        DataSet<Tuple2<Integer, Integer>> verticesAsWorkset = env.fromCollection(vertices);
//        DataSet<Tuple2<Integer, Integer>> edgesAsWorkset = env.fromCollection(edges);
//
//        int vertexIdIndex = 0;
//        DeltaIteration<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> iteration =
// verticesAsWorkset
//                .iterateDelta(verticesAsWorkset, MAX_ITERATION_NUM, vertexIdIndex);
//
//        DataSet<Tuple2<Integer, Integer>> delta = iteration.getWorkset()
//                .join(edgesAsWorkset).where(0).equalTo(0)
//                .with(new NeighborWithParentIDJoin())
//                .join(iteration.getSolutionSet()).where(0).equalTo(0)
//                .with(new RootIdFilter());
//        DataSet<Tuple2<Integer, Integer>> finalDataSet = iteration.closeWith(delta, delta);
//
//        finalDataSet.print();
//    }
// }
