package com.nus.cool.queryserver.model;

/**
 * cube.
 */
public class Parameter implements Comparable<Parameter> {

  private final Integer priority;

  // url content, eg. cohort?path=filePath/&file=fileName&queryId=1
  private final String content;

  public Parameter(int priority, String content) {
    this.content = content;
    this.priority = priority;
  }

  public Integer getPriority() {
    return this.priority;
  }

  public String getContent() {
    return this.content;
  }

  @Override
  public int compareTo(Parameter p) {
    return this.priority.compareTo(p.getPriority());
  }
}
