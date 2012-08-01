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
    public TopologyBuilder prepareTopology(String drpc_function_name, ILocalDRPC drpc) {
        TopologyBuilder topology_builder = new TopologyBuilder();

        // training
        topology_builder.setSpout("training-spout",
            new ExampleTrainingSpout()
        );

        topology_builder.setBolt("training-bolt",
            new TrainingBolt(
                PerceptronDRPCTopology.bias,
                PerceptronDRPCTopology.threshold,
                PerceptronDRPCTopology.learning_rate,
                PerceptronDRPCTopology.MEMCACHED_SERVERS
            )
        ).shuffleGrouping("training-spout");

        // evaluation
        DRPCSpout drpc_spout;
        if (drpc!=null)
            drpc_spout = new DRPCSpout(drpc_function_name, drpc);
        else
            drpc_spout = new DRPCSpout(drpc_function_name);

        topology_builder.setSpout("drpc-spout",
            drpc_spout
        );

        topology_builder.setBolt("drpc-evaluation",
            new EvaluationBolt(
                PerceptronDRPCTopology.bias,
                PerceptronDRPCTopology.threshold,
                PerceptronDRPCTopology.MEMCACHED_SERVERS
            )
        ).shuffleGrouping("drpc-spout");

        topology_builder.setBolt(
            "drpc-return",
            new ReturnResults()
        ).shuffleGrouping("drpc-evaluation");

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
