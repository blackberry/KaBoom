package com.blackberry.logdriver.kaboom;

public class KaBoomNodeInfo {
  private String hostname;
  private int weight;

  private int load = 0;
  private double targetLoad = 0.0;

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  public int getLoad() {
    return load;
  }

  public void setLoad(int load) {
    this.load = load;
  }

  public double getTargetLoad() {
    return targetLoad;
  }

  public void setTargetLoad(double targetLoad) {
    this.targetLoad = targetLoad;
  }

  @Override
  public String toString() {
    return "KaBoomNodeInfo [hostname=" + hostname + ", weight=" + weight + "]";
  }

}
