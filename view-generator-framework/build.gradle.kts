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
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.0")
  implementation("org.hypertrace.core.flinkutils:flink-utils:0.1.0")
  constraints {
    implementation("com.google.guava:guava:29.0-jre") {
      because("Deserialization of Untrusted Data [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-32236] in com.google.guava:guava@20.0\n" +
          "   introduced by org.hypertrace.core.flinkutils:flink-utils@0.1.0 > io.confluent:kafka-avro-serializer@5.5.0 > io.confluent:kafka-schema-registry-client@5.5.0 > io.swagger:swagger-core@1.5.3 > com.google.guava:guava@18.0")
    }
    implementation("commons-codec:commons-codec:1.14") {
      because("Information Exposure [Low Severity][https://snyk.io/vuln/SNYK-JAVA-COMMONSCODEC-561518] in commons-codec:commons-codec@1.10\n" +
          "   introduced by org.hypertrace.core.serviceframework:platform-service-framework@0.1.0 > org.apache.httpcomponents:httpclient@4.5.12 > commons-codec:commons-codec@1.11")
    }
    implementation("org.hibernate.validator:hibernate-validator:6.1.5.Final") {
      because("Cross-site Scripting (XSS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGHIBERNATEVALIDATOR-541187] in org.hibernate.validator:hibernate-validator@6.0.17.Final\n" +
          "   introduced by org.hypertrace.core.flinkutils:flink-utils@0.1.0 > io.confluent:kafka-avro-serializer@5.5.0 > io.confluent:kafka-schema-registry-client@5.5.0 > org.glassfish.jersey.ext:jersey-bean-validation@2.30 > org.hibernate.validator:hibernate-validator@6.0.17.Final")
    }
    implementation("org.yaml:snakeyaml:1.26") {
      because("Denial of Service (DoS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGYAML-537645] in org.yaml:snakeyaml@1.12\n" +
          "   introduced by org.hypertrace.core.flinkutils:flink-utils@0.1.0 > io.confluent:kafka-avro-serializer@5.5.0 > io.confluent:kafka-schema-registry-client@5.5.0 > io.swagger:swagger-core@1.5.3 > com.fasterxml.jackson.dataformat:jackson-dataformat-yaml@2.4.5 > org.yaml:snakeyaml@1.12")
    }
  }

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

  testImplementation("org.apache.kafka:kafka-clients:5.5.0-ccs")
  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
}
