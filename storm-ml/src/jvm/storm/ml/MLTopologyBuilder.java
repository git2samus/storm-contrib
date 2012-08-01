package storm.ml;

import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.generated.StormTopology;
import backtype.storm.ILocalDRPC;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.TopologyBuilder;

import storm.ml.spout.BaseTrainingSpout;

public class MLTopologyBuilder {
    String topology_prefix;

    BaseTrainingSpout training_spout; Number training_spout_parallelism;
    IBasicBolt training_bolt;         Number training_bolt_parallelism;
    IBasicBolt evaluation_bolt;       Number evaluation_bolt_parallelism;

    public MLTopologyBuilder(String topology_prefix) {
        this.topology_prefix = topology_prefix;
    }

    public void setTrainingSpout(BaseTrainingSpout training_spout, Number parallelism) {
        this.training_spout = training_spout;
        this.training_spout_parallelism = training_spout_parallelism;
    }
    public void setTrainingSpout(BaseTrainingSpout training_spout) {
        setTrainingSpout(training_spout, 1);
    }

    public void setTrainingBolt(IBasicBolt training_bolt, Number parallelism) {
        this.training_bolt = training_bolt;
        this.training_bolt_parallelism = training_bolt_parallelism;
    }
    public void setTrainingBolt(IBasicBolt training_bolt) {
        setTrainingBolt(training_bolt, 1);
    }

    public void setEvaluationBolt(IBasicBolt evaluation_bolt, Number parallelism) {
        this.evaluation_bolt = evaluation_bolt;
        this.evaluation_bolt_parallelism = evaluation_bolt_parallelism;
    }
    public void setEvaluationBolt(IBasicBolt evaluation_bolt) {
        setEvaluationBolt(evaluation_bolt, 1);
    }

    public TopologyBuilder prepareTopology(String drpc_function_name, ILocalDRPC drpc) {
        TopologyBuilder topology_builder = new TopologyBuilder();

        // training
        topology_builder.setSpout(this.topology_prefix + "-training-spout",
            this.training_spout, this.training_spout_parallelism
        );

        topology_builder.setBolt(this.topology_prefix + "-training-bolt",
            this.training_bolt, this.training_bolt_parallelism
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
            this.evaluation_bolt, this.evaluation_bolt_parallelism
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
