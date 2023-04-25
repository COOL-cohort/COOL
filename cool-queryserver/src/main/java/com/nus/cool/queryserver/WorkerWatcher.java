package com.nus.cool.queryserver;

import com.nus.cool.queryserver.model.Parameter;
import com.nus.cool.queryserver.singleton.TaskQueue;
import com.nus.cool.queryserver.singleton.WorkerIndex;
import com.nus.cool.queryserver.singleton.ZKConnection;
import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


/**
 * listening when zookeeper has change.
 */
public class WorkerWatcher implements Watcher {

  private final ZooKeeper zk;

  public WorkerWatcher(ZKConnection zk) throws KeeperException, InterruptedException {
    this.zk = zk.getZK();
    this.zk.getChildren("/workers", this);
  }

  /**
   * Re-add all urls assigned to the worker back to the task queue.
   *
   * @param event event
   */
  @Override
  public void process(WatchedEvent event) {
    try {
      if (event.getType() == Event.EventType.NodeChildrenChanged) {
        WorkerIndex workerIndex = WorkerIndex.getInstance();
        List<String> workers = zk.getChildren("/workers", this);
        List<String> parameters = workerIndex.checkDisconnected(workers);
        TaskQueue taskQueue = TaskQueue.getInstance();
        for (String parameter : parameters) {
          Parameter p = new Parameter(0, parameter);
          taskQueue.add(p);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
