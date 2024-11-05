plugins {
  `java-library`
  jacoco
  id("org.hypertrace.avro-plugin")
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.80")
  implementation(platform("io.grpc:grpc-bom:1.60.0"))
  implementation("org.apache.avro:avro:1.11.4")
  api("org.apache.commons:commons-compress:1.26.0") {
    because("https://www.tenable.com/cve/CVE-2024-25710")
  }
  implementation("org.apache.pinot:pinot-tools:1.2.0") {
    // All these third party libraries are not used in view creation workflow.
    // They bring in lot of vulnerabilities (snyk). so, excluding unused libs
    exclude("com.google.protobuf", "protobuf-java")
    exclude("com.jayway.jsonpath", "json-path")
    exclude("commons-codec", "commons-codec")
    exclude("commons-io", "commons-io")
    exclude("io.airlift", "aircompressor")
    exclude("io.grpc", "grpc-netty-shaded")
    exclude("io.netty", "netty")
    exclude("javax.servlet", "javax.servlet-api")
    exclude("org.apache.datasketches", "datasketches-java")
    exclude("org.apache.hadoop", "hadoop-common")
    exclude("org.apache.hadoop", "hadoop-hdfs")
    exclude("org.apache.hadoop", "hadoop-hdfs-client")
    exclude("org.apache.hadoop.thirdparty", "hadoop-shaded-protobuf_3_21")
    exclude("org.apache.helix", "helix-core")
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.httpcomponents", "httpclient")
    exclude("org.apache.kafka", "kafka_2.10")
    exclude("org.apache.logging.log4j", "log4j-core")
    exclude("org.apache.lucene", "lucene-analysis-common")
    exclude("org.apache.lucene", "lucene-backward-codecs")
    exclude("org.apache.lucene", "lucene-core")
    exclude("org.apache.lucene", "lucene-queryparser")
    exclude("org.apache.lucene", "lucene-sandbox")
    exclude("org.apache.pinot", "pinot-controller")
    exclude("org.apache.pinot", "pinot-broker")
    exclude("org.apache.pinot", "pinot-kafka-2.0")
    exclude("org.apache.pinot", "pinot-minion-builtin-tasks")
    exclude("org.apache.pinot", "pinot-minion")
    exclude("org.apache.pinot", "pinot-orc")
    exclude("org.apache.pinot", "pinot-pulsar")
    exclude("org.apache.pinot", "pinot-parquet")
    exclude("org.apache.pinot", "pinot-segment-local")
    exclude("org.apache.pinot", "pinot-server")
    exclude("org.apache.pinot", "pinot-s3")
    exclude("org.apache.spark", "spark-launcher_2.12")
    exclude("org.apache.thrift", "libthrift")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.glassfish.jersey.containers", "jersey-container-grizzly2-http")
    exclude("org.glassfish.jersey.core", "jersey-server")
    exclude("org.quartz-scheduler", "quartz")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.webjars", "swagger-ui")
    exclude("org.yaml", "snakeyaml")
  }

  constraints {
    implementation("com.google.code.gson:gson:2.10.1")
  }

  implementation(platform("io.netty:netty-bom:4.1.108.Final"))
  implementation(platform("org.glassfish.jersey:jersey-bom:2.40"))
  implementation(platform("org.jetbrains.kotlin:kotlin-bom:1.6.21"))

  compileOnly("org.projectlombok:lombok:1.18.30")
  annotationProcessor("org.projectlombok:lombok:1.18.30")
  implementation("org.slf4j:slf4j-api:2.0.7")
  implementation("com.typesafe:config:1.4.2")

  testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
  testImplementation("org.mockito:mockito-core:5.2.0")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  setAgainstFiles(null)
}

group = "org.hypertrace.core.viewcreator"
