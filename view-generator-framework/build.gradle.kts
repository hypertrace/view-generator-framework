plugins {
  `java-library`
  jacoco
  id("com.commercehub.gradle.plugin.avro") version "0.9.1"
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

repositories {
  // Need this to fetch confluent's kafka-clients dependency
  maven("http://packages.confluent.io/maven")
}

sourceSets {
  test {
    java {
      srcDirs("src/test/java", "build/generated-test-avro-java")
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.2")
  implementation("org.hypertrace.core.flinkutils:flink-utils:0.1.1")

  implementation("org.apache.avro:avro:1.9.2")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.25")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  implementation("com.typesafe:config:1.3.2")

  // Flink
  implementation("org.apache.flink:flink-streaming-java_2.11:1.7.0")
  implementation("org.apache.flink:flink-connector-kafka_2.11:1.7.0")
  implementation("de.javakaffee:kryo-serializers:0.45")
  runtimeOnly("org.apache.flink:flink-avro:1.9.2")

  // Needed for flink metric exporter. Used for Hypertrace and debugging.
  runtimeOnly("org.apache.flink:flink-metrics-slf4j:1.10.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
}
