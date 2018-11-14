


env destination="mseed-blocks" filename=$SPP_COMMON/synthetic/simdat10s.mseed brokers="localhost" java -jar $SPP_COMMON/jars/mseed-loader-1.0-SNAPSHOT.jar

env destination="mseed_1sec" source="mseed-blocks" brokers="localhost" java -jar $SPP_COMMON/jars/mseed-streams-1.0-SNAPSHOT.jar

# env variables:
# - destination - topic name
# default: mseed-blocks
# - brokers (default: localhost)

# - filename (required; no defaults)
# The utility loads the <filename> file into the <destination> kafka topic