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

## Starting kafka

```
docker-compose up
docker-compose exec broker kafka-topics --create --zookeeper \
    zookeeper:2181 --replication-factor 1 --partitions 1 --topic test

# consumer and producer
docker-compose exec broker kafka-console-producer --broker-list localhost:9092 --topic test
docker-compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
```

## Importing the Grafana dashboard

Configure a datasource with http://prometheus:9090

Then, go to http://localhost:3000/dashboard/import and enter 721


## Creating a deployment user

1. add ssh key /admin/users
2. impersonate user
3. add public key /profile/keys
4. create api access token for registry access /profile/personal_access_tokens

## Hello

```
docker login seismic-gitlab.eastus.cloudapp.azure.com:5005
```

Docker Preferences +Daemon

```
/etc/docker/daemon.json
{
  "insecure-registries" : ["seismic-gitlab.eastus.cloudapp.azure.com:5005"]
}
```

