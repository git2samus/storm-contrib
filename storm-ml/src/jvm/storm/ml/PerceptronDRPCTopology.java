package storm.ml;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.MemcachedClient;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import storm.ml.MLTopologyBuilder;
import storm.ml.spout.ExampleTrainingSpout;

public class PerceptronDRPCTopology {
    public static final String MEMCACHED_SERVERS = "127.0.0.1:11211";
    public static final Double bias          = 0.0;
    public static final Double threshold     = 0.1;
    public static final Double learning_rate = 0.1;

    public static void main(String[] args) throws Exception {
        MemcachedClient memcache = new MemcachedClient(AddrUtil.getAddresses(PerceptronDRPCTopology.MEMCACHED_SERVERS));
        OperationFuture promise = memcache.set("weights", 0, "[0.0, 0.0]");
        promise.get();

        Config topology_conf = new Config();

        if (args==null || args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();

            MLTopologyBuilder ml_topology_builder = new MLTopologyBuilder("perceptron");
            cluster.submitTopology("perceptron", topology_conf, ml_topology_builder.createLocalTopology("evaluate", drpc));

            int error_count = 0;
            FileWriter fstream = new FileWriter("out.csv");
            BufferedWriter out = new BufferedWriter(fstream);

            List<String> input_vectors = get_input_vectors();
            for (int i=0; i<input_vectors.size(); i++) {
                String input_vector = input_vectors.get(i);

                List<Double> parsed_iv = Util.parse_str_vector(input_vector);
                Double x = parsed_iv.get(0);
                Double y = parsed_iv.get(1);

                String result = drpc.execute("evaluate", input_vector);
                Integer expected_result = ExampleTrainingSpout.get_label(x, y);

                if (!result.equals(expected_result.toString()))
                    error_count += 1;

                Double error_perc = 100 * error_count / Double.valueOf(i+1);

                String format = "%s -> %s (expected: %s, %.2f%% error)";
                System.out.println(String.format(format, input_vector, result, expected_result, error_perc));

                format = "%s	%s	%s	%.2f";
                out.write(String.format(format, input_vector, result, expected_result, error_perc));
                out.newLine();
            }
            out.close();

            cluster.shutdown();
            drpc.shutdown();
        } else {
            MLTopologyBuilder ml_topology_builder = new MLTopologyBuilder(args[0]);
            StormSubmitter.submitTopology(args[0], topology_conf, ml_topology_builder.createRemoteTopology("evaluate"));
        }
    }

    public static List<String> get_input_vectors() {
        List<String> input_vectors = new ArrayList<String>();
        for (Double x=-10.0; x<=10.0; x+=0.5) {
            for (Double y=-10.0; y<=10.0; y+=0.5) {
                List<Double> result_item = new ArrayList<Double>();
                result_item.add(x);
                result_item.add(y);

                input_vectors.add(result_item.toString());
            }
        }

        return input_vectors;
    }
}
