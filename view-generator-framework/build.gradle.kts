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
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.14")
  implementation("org.hypertrace.core.flinkutils:flink-utils:0.1.6")

  implementation("org.apache.avro:avro:1.9.2")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.25")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  implementation("com.typesafe:config:1.3.2")

  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.13")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-serdes:0.1.13")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
}
