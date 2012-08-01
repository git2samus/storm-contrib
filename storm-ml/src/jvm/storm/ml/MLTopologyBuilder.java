package storm.ml;

import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.generated.StormTopology;
import backtype.storm.ILocalDRPC;
import backtype.storm.topology.TopologyBuilder;

import storm.ml.bolt.EvaluationBolt;
import storm.ml.bolt.TrainingBolt;
import storm.ml.spout.ExampleTrainingSpout;

public class MLTopologyBuilder {
    String topology_prefix;

    public MLTopologyBuilder(String topology_prefix) {
        this.topology_prefix = topology_prefix;
    }

    public TopologyBuilder prepareTopology(String drpc_function_name, ILocalDRPC drpc) {
        TopologyBuilder topology_builder = new TopologyBuilder();

        // training
        topology_builder.setSpout(this.topology_prefix + "-training-spout",
            new ExampleTrainingSpout()
        );

        topology_builder.setBolt(this.topology_prefix + "-training-bolt",
            new TrainingBolt(
                PerceptronDRPCTopology.bias,
                PerceptronDRPCTopology.threshold,
                PerceptronDRPCTopology.learning_rate,
                PerceptronDRPCTopology.MEMCACHED_SERVERS
            )
        ).shuffleGrouping(this.topology_prefix + "-training-spout");

        // evaluation
        DRPCSpout drpc_spout;
        if (drpc!=null)
            drpc_spout = new DRPCSpout(drpc_function_name, drpc);
        else
            drpc_spout = new DRPCSpout(drpc_function_name);

        topology_builder.setSpout(this.topology_prefix + "-drpc-spout",
            drpc_spout
        );

        topology_builder.setBolt(this.topology_prefix + "-drpc-evaluation",
            new EvaluationBolt(
                PerceptronDRPCTopology.bias,
                PerceptronDRPCTopology.threshold,
                PerceptronDRPCTopology.MEMCACHED_SERVERS
            )
        ).shuffleGrouping(this.topology_prefix + "-drpc-spout");

        topology_builder.setBolt(this.topology_prefix + "-drpc-return",
            new ReturnResults()
        ).shuffleGrouping(this.topology_prefix + "-drpc-evaluation");

        // return
        return topology_builder;
    }

    public StormTopology createLocalTopology(String drpc_function_name, ILocalDRPC drpc) {
        return prepareTopology(drpc_function_name, drpc).createTopology();
    }

    public StormTopology createRemoteTopology(String drpc_function_name) {
        return prepareTopology(drpc_function_name, null).createTopology();
    }
}
