plugins {
  `java-library`
  jacoco
  id("com.github.davidmc24.gradle.plugin.avro")
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
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.33")

  implementation("org.apache.avro:avro:1.10.2")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  implementation("com.typesafe:config:1.4.1")

  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.25")
  constraints {
    implementation("org.glassfish.jersey.core:jersey-common:2.34") {
      because("Information Disclosure [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637] in org.glassfish.jersey.core:jersey-common@2.30")
    }
    implementation("org.apache.commons:commons-compress:1.21") {
      because("Denial of Service (DoS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGAPACHECOMMONS-1316640] in org.apache.commons:commons-compress@1.20")
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.1") {
      because("Denial of Service (DoS) " +
          "[Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2326698] " +
          "in com.fasterxml.jackson.core:jackson-databind@2.12.2")
    }
  }

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.junit-pioneer:junit-pioneer:1.3.8")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:6.0.1-ccs")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  constraints {
  }
}
