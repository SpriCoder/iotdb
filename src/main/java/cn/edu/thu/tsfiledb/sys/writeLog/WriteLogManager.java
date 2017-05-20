package cn.edu.thu.tsfiledb.sys.writeLog;

import java.io.IOException;
import java.util.HashMap;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

public class WriteLogManager {
    private static final Logger LOG = LoggerFactory.getLogger(WriteLogManager.class);
    private static WriteLogManager instance;
    private static HashMap<String, WriteLogNode> logNodeMaps;

    private WriteLogManager() {
        logNodeMaps = new HashMap<>();
    }

    public static WriteLogManager getInstance() {
        if (instance == null) {
            synchronized (WriteLogManager.class) {
                instance = new WriteLogManager();
            }
        }
        return instance;
    }

    public static WriteLogNode getWriteLogNode(Path path) {
        if (logNodeMaps.containsKey(path.toString())) {
            return logNodeMaps.get(path.toString());
        }
        logNodeMaps.put(path.toString(), new WriteLogNode(path));
        return logNodeMaps.get(path.toString());
    }

    public void write(PhysicalPlan plan) throws IOException {

    }

    public PhysicalPlan getPhysicalPlan() throws IOException {
        return null;
    }
}
