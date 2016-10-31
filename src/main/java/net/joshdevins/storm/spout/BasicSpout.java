package net.joshdevins.storm.spout;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;

/**
 * An default implementation if {@link IRichSpout} leaving basic methods empty. This will also save the collector that
 * is set on {@link #open(Map, TopologyContext, SpoutOutputCollector)} and call a simpler {@link #open()} method on
 * sub-classes as a convenience/simplification.
 * 
 * @author Josh Devins
 */
public abstract class BasicSpout implements IRichSpout {

    private static final long serialVersionUID = -9082020138402819214L;

    private SpoutOutputCollector collector;


    public void ack(final Object msgId) {
    }

    public void close() {
    }


    public void fail(final Object msgId) {
    }


    public boolean isDistributed() {
        return true;
    }


    public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
            final SpoutOutputCollector collector) {

        this.collector = collector;

        // call the abstract method that most spouts will probably implement over this
        open();
    }

    protected List<Integer> emit(final List<Object> values) {
        return collector.emit(values);
    }

    protected SpoutOutputCollector getOutputCollector() {
        return collector;
    }

    protected abstract void open();
}
