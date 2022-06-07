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
      srcDirs("build/generated-test-avro-java")
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.31")
  constraints {
    implementation("io.netty:netty-all:4.1.68.Final") {
      because("HTTP Request Smuggling [High Severity][https://snyk.io/vuln/SNYK-JAVA-IONETTY-559515] in io.netty:netty-all@4.1.28.Final")
    }
  }

  implementation("org.apache.avro:avro:1.10.2")
  implementation("org.apache.pinot:pinot-tools:0.7.1") {
    // All these third party libraries are not used in view creation workflow.
    // They bring in lot of vulnerabilities (snyk). so, excluding unused libs
    exclude("com.google.protobuf", "protobuf-java")
    exclude("com.jayway.jsonpath", "json-path")
    exclude("commons-codec", "commons-codec")
    exclude("commons-collections", "commons-collections")
    exclude("commons-httpclient", "commons-httpclient")
    exclude("commons-io", "commons-io")
    exclude("io.grpc", "grpc-netty-shaded")
    exclude("io.netty", "netty")
    exclude("javax.servlet", "javax.servlet-api")
    exclude("org.apache.calcite", "calcite-core")
    exclude("org.apache.hadoop", "hadoop-common")
    exclude("org.apache.hadoop", "hadoop-hdfs")
    exclude("org.apache.helix", "helix-core")
    exclude("org.apache.httpcomponents", "httpclient")
    exclude("org.apache.kafka", "kafka_2.10")
    exclude("org.apache.logging.log4j", "log4j-core")
    exclude("org.apache.pinot", "pinot-controller")
    exclude("org.apache.pinot", "pinot-broker")
    exclude("org.apache.pinot", "pinot-kafka-2.0")
    exclude("org.apache.pinot", "pinot-parquet")
    exclude("org.apache.pinot", "pinot-server")
    exclude("org.apache.pinot", "pinot-s3")
    exclude("org.apache.thrift", "libthrift")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.glassfish.jersey.containers", "jersey-container-grizzly2-http")
    exclude("org.glassfish.jersey.core", "jersey-server")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.webjars", "swagger-ui")
    exclude("org.yaml", "snakeyaml")
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.datasketches", "datasketches-java")
  }
  constraints {
    implementation("com.google.guava:guava:30.0-android") {
      because("Information Disclosure [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415] in com.google.guava:guava@28.2-android")
    }
    implementation("org.apache.commons:commons-compress:1.21") {
      because("Denial of Service (DoS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGAPACHECOMMONS-1316640] in org.apache.commons:commons-compress@1.20")
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.1") {
      because("Denial of Service (DoS) [High Severity]" +
          "[https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2421244] " +
          "in com.fasterxml.jackson.core:jackson-databind@2.13.1")
    }
  }

  compileOnly("org.projectlombok:lombok:1.18.24")
  annotationProcessor("org.projectlombok:lombok:1.18.24")
  implementation("org.slf4j:slf4j-api:1.7.36")
  implementation("com.typesafe:config:1.4.1")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")

  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
  testImplementation("org.mockito:mockito-core:4.5.1")
}

group = "org.hypertrace.core.viewcreator"
