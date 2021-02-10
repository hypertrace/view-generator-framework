plugins {
  `java-library`
  jacoco
  id("com.commercehub.gradle.plugin.avro") version "0.9.1"
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}


sourceSets {
  test {
    java {
      srcDir("build/generated-test-avro-java")
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.20")

  implementation("org.apache.avro:avro:1.9.2")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")

  implementation("com.typesafe:config:1.4.1")

  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.13")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-serdes:0.1.13")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  testImplementation("org.mockito:mockito-core:3.6.28")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")

  constraints {
    implementation("com.google.guava:guava:30.0-jre") {
      because("Information Disclosure (new) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415] in com.google.guava:guava@29.0-android")
    }
    implementation("org.glassfish.jersey.ext:jersey-bean-validation:2.31") {
      because("XML Entity Expansion [High Severity][https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYMEDIA-595972] in org.glassfish.jersey.media:jersey-media-jaxb@2.30")
    }

  }
}
