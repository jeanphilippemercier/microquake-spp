FROM confluentinc/cp-kafka-connect:5.0.0

ADD prom-jmx-agent-config.yml /usr/share/java/kafka/prom-jmx-agent-config.yml
ADD http://central.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.3.1/jmx_prometheus_javaagent-0.3.1.jar /usr/share/java/kafka/jmx_prometheus_javaagent.jar
