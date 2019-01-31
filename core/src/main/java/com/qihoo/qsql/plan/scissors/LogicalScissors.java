package com.qihoo.qsql.plan.scissors;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.rel.RelNode;

//一条路径上只有一个标记物生效
public class LogicalScissors {

    private Set<LogicalMarker> markers = new HashSet<>();

    public LogicalScissors() {
        // registerMarker(new ElasticJoinMarker())
        //     .registerMarker(new DiffSourceMarker());
    }

    public void exec(RelNode relNode) {
        injectMarker(relNode, null);

    }

    public LogicalScissors registerMarker(LogicalMarker marker) {
        markers.add(marker);
        return this;
    }

    /**
     * .
     */
    public void injectMarker(RelNode child, RelNode parent) {
        if (! Objects.isNull(parent)) {
            //do replacing
            //create a memento
        }
        // markers.forEach(marker -> marker.mark(child));
        child.getInputs().forEach(sub -> injectMarker(sub, child));
    }


}
