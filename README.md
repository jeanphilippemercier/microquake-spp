# Installing the package

The package can be installed by simply executing the following command from this directory

```
>> pip install -e .
```

# Set-up of environment variables

The following environment variables need to be set for the package to work
```
>> export spp_config="[...]/config"
>> export spp_temp="[???]/.spp"
```
where [...] represents the directory in which the spp source code is located and [???]/.spp refers to any directory of
your choice where you want to store temporary files.

# Starting kafke

```
docker-compose up
docker-compose exec broker kafka-topics --create --zookeeper \
    zookeeper:2181 --replication-factor 1 --partitions 1 --topic test
```
