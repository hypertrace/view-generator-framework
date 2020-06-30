package org.hypertrace.core.viewcreator.pinot;

/**
 * Generic Pair class, to store a pair of data
 */
public class Pair<K, V> {
  private final K key;
  private final V value;

  public Pair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

}
