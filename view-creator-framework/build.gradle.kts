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
      srcDirs("build/generated-test-avro-java")
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("org.hypertrace.core.eventstore:event-store:0.1.1")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.16")
  constraints {
    implementation("io.netty:netty-all:4.1.50.Final") {
      because("HTTP Request Smuggling [High Severity][https://snyk.io/vuln/SNYK-JAVA-IONETTY-559515] in io.netty:netty-all@4.1.28.Final")
    }
    implementation("commons-collections:commons-collections:3.2.2") {
      because("Deserialization of Untrusted Data [High Severity][https://snyk.io/vuln/SNYK-JAVA-COMMONSCOLLECTIONS-30078] in commons-collections:commons-collections@3.2.1")
    }
    implementation("org.apache.thrift:libthrift:0.13.0") {
      because("Denial of Service (DoS) [High Severity][https://snyk.io/vuln/SNYK-JAVA-ORGAPACHETHRIFT-474610] in org.apache.thrift:libthrift@0.12.0")
    }
    implementation("org.webjars:swagger-ui:3.27.0") {
      because("Reverse Tabnabbing [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGWEBJARS-449821] in org.webjars:swagger-ui@2.2.2")
    }
    implementation("org.yaml:snakeyaml:1.26") {
      because("Denial of Service (DoS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGYAML-537645] in org.yaml:snakeyaml@1.23")
    }
    implementation("org.apache.zookeeper:zookeeper:3.6.1") {
      because("Access Control Bypass [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGAPACHEZOOKEEPER-174781] in org.apache.zookeeper:zookeeper@3.4.13")
    }
    implementation("io.netty:netty:3.10.6.Final") {
      because("Information Exposure [High Severity][https://snyk.io/vuln/SNYK-JAVA-IONETTY-30430] in io.netty:netty@3.9.6.Final")
    }
    implementation("org.glassfish.jersey.core:jersey-server:2.31") {
      because("XML Entity Expansion [High Severity][https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYMEDIA-595972] in org.glassfish.jersey.media:jersey-media-jaxb@2.28")
    }
    implementation("io.grpc:grpc-core:1.31.0") {
      because("Information Exposure [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-IOGRPC-571957] in io.grpc:grpc-core@1.30.0")
    }
  }

  implementation("org.apache.avro:avro:1.9.2")
  implementation("org.apache.kafka:kafka-clients:5.5.0-ccs")
  implementation("org.apache.pinot:pinot-tools:0.5.0") {
    // We use newer version of servlet-api brought by jetty so we exclude the older
    // version here.
    exclude("javax.servlet", "javax.servlet-api")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.apache.kafka", "kafka_2.10")
    exclude("org.apache.pinot", "pinot-controller")
    exclude("org.apache.pinot", "pinot-server")
    exclude("org.apache.pinot", "pinot-broker")
  }

  implementation("org.slf4j:slf4j-api:1.7.25")
  implementation("com.typesafe:config:1.3.2")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
}

group = "org.hypertrace.core.viewcreator"
