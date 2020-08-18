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
  implementation("org.hypertrace.core.eventstore:event-store:0.1.1")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.8")
  constraints {
    implementation("com.google.guava:guava:29.0-jre") {
      because("Deserialization of Untrusted Data [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-32236] in com.google.guava:guava@20.0\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.pinot:pinot-common@0.3.0 > com.google.guava:guava@20.0")
    }
    implementation("io.netty:netty-all:4.1.50.Final") {
      because("HTTP Request Smuggling [High Severity][https://snyk.io/vuln/SNYK-JAVA-IONETTY-559515] in io.netty:netty-all@4.1.28.Final\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.pinot:pinot-core@0.3.0 > io.netty:netty-all@4.1.28.Final")
    }
    implementation("com.jcraft:jsch:0.1.55") {
      because("Directory Traversal [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMJCRAFT-30302] in com.jcraft:jsch@0.1.42\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.hadoop:hadoop-common@2.7.0 > com.jcraft:jsch@0.1.42")
    }
    implementation("commons-codec:commons-codec:1.14") {
      because("Information Exposure [Low Severity][https://snyk.io/vuln/SNYK-JAVA-COMMONSCODEC-561518] in commons-codec:commons-codec@1.10\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.hadoop:hadoop-common@2.7.0 > commons-codec:commons-codec@1.10")
    }
    implementation("commons-collections:commons-collections:3.2.2") {
      because("Deserialization of Untrusted Data [High Severity][https://snyk.io/vuln/SNYK-JAVA-COMMONSCOLLECTIONS-30078] in commons-collections:commons-collections@3.2.1\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.hadoop:hadoop-common@2.7.0 > commons-collections:commons-collections@3.2.1")
    }
    implementation("org.apache.hadoop:hadoop-common:3.2.1") {
      because("Arbitrary File Write via Archive Extraction (Zip Slip) [High Severity][https://snyk.io/vuln/SNYK-JAVA-ORGAPACHEHADOOP-174573] in org.apache.hadoop:hadoop-common@2.7.0\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.hadoop:hadoop-common@2.7.0")
    }
    implementation("org.apache.thrift:libthrift:0.13.0") {
      because("Denial of Service (DoS) [High Severity][https://snyk.io/vuln/SNYK-JAVA-ORGAPACHETHRIFT-474610] in org.apache.thrift:libthrift@0.12.0\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.pinot:pinot-common@0.3.0 > org.apache.thrift:libthrift@0.12.0 and 7 other path(s)")
    }
    implementation("org.webjars:swagger-ui:3.27.0") {
      because("Reverse Tabnabbing [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGWEBJARS-449821] in org.webjars:swagger-ui@2.2.2\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.pinot:pinot-common@0.3.0 > org.webjars:swagger-ui@2.2.2")
    }
    implementation("org.yaml:snakeyaml:1.26") {
      because("Denial of Service (DoS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGYAML-537645] in org.yaml:snakeyaml@1.23\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.yaml:snakeyaml@1.23")
    }
    implementation("commons-beanutils:commons-beanutils:1.9.4") {
      because("Deserialization of Untrusted Data [High Severity][https://snyk.io/vuln/SNYK-JAVA-COMMONSBEANUTILS-460111] in commons-beanutils:commons-beanutils@1.9.3\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.hadoop:hadoop-common@3.2.1 > commons-beanutils:commons-beanutils@1.9.3")
    }
    implementation("com.nimbusds:nimbus-jose-jwt:8.19") {
      because("Improper Check for Unusual or Exceptional Conditions [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMNIMBUSDS-536068] in com.nimbusds:nimbus-jose-jwt@4.41.1\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.hadoop:hadoop-common@3.2.1 > org.apache.hadoop:hadoop-auth@3.2.1 > com.nimbusds:nimbus-jose-jwt@4.41.1")
    }
    implementation("org.eclipse.jetty:jetty-http:9.4.30.v20200611") {
      because("Denial of Service (DoS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGECLIPSEJETTY-174011] in org.eclipse.jetty:jetty-http@9.3.24.v20180605\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.hadoop:hadoop-common@3.2.1 > org.eclipse.jetty:jetty-server@9.3.24.v20180605 > org.eclipse.jetty:jetty-http@9.3.24.v20180605")
    }
    implementation("org.eclipse.jetty:jetty-server:9.4.30.v20200611") {
      because("Information Exposure [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGECLIPSEJETTY-174560] in org.eclipse.jetty:jetty-server@9.3.24.v20180605\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.hadoop:hadoop-common@3.2.1 > org.eclipse.jetty:jetty-server@9.3.24.v20180605")
    }
    implementation("org.apache.zookeeper:zookeeper:3.6.1") {
      because("Access Control Bypass [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGAPACHEZOOKEEPER-174781] in org.apache.zookeeper:zookeeper@3.4.13\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.pinot:pinot-common@0.3.0 > org.apache.zookeeper:zookeeper@3.4.13")
    }
    implementation("io.netty:netty:3.10.6.Final") {
      because("Information Exposure [High Severity][https://snyk.io/vuln/SNYK-JAVA-IONETTY-30430] in io.netty:netty@3.9.6.Final\n" +
          "   introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.pinot:pinot-common@0.3.0 > io.netty:netty@3.9.6.Final")
    }

    implementation("org.glassfish.jersey.core:jersey-server:2.31") {
      because("XML Entity Expansion [High Severity][https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYMEDIA-595972] in org.glassfish.jersey.media:jersey-media-jaxb@2.28\n" +
              "    introduced by org.apache.pinot:pinot-tools@0.3.0 > org.apache.pinot:pinot-common@0.3.0 > org.glassfish.jersey.core:jersey-server@2.28 > org.glassfish.jersey.media:jersey-media-jaxb@2.28 and 1 other path(s)\n")
    }
  }

  implementation("org.apache.avro:avro:1.9.2")
  implementation("org.apache.kafka:kafka-clients:5.5.0-ccs")
  implementation("org.apache.pinot:pinot-tools:0.3.0") {
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
