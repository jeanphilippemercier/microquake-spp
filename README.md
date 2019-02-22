# Installing the package

The package can be installed by simply executing the following command from this directory

```
>> pip install -e .
```

## Set-up of environment variables

The following environment variable need to be set for the package to work properly
```
>> export SPP_CONFIG="[SPP_HOME]/config"
>> export SPP_COMMON="[SPP_HOME]/common"
```
make sure there is settings.toml config file in the $SPP_CONFIG directory 

## run the initialization script 

The initialization script can be found in bin/00_prepare_project.py

Note that for the script to run, NonLinLoc binaries (http://alomax.free.fr/nlloc/) 
must be in the path.


## Starting kafka

```
docker-compose up
docker-compose exec broker kafka-topics --create --zookeeper \
    zookeeper:2181 --replication-factor 1 --partitions 1 --topic test

# consumer and producer
docker-compose exec broker kafka-console-producer --broker-list localhost:9092 --topic test
docker-compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
```

## Deleting topic and clearing message queues

docker-compose exec broker kafka-topics --zookeeper zookeeper:2181 --delete --topic

## Importing the Grafana dashboard

Configure a datasource with http://prometheus:9090
This does not work for me (JP)?

Then, go to http://localhost:3000/dashboard/import and enter 721

What is the Graphana Username and Password? 

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

## Docker install on ubuntu 16

```
sudo apt-get remove docker docker-engine docker.io
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
sudo apt-get update
sudo apt-get install docker-ce docker-compose
```

## Add user to the docker group and start docker daemon

```
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -a -G docker phil
```

## pip 18.1 causes issues

```
File "/usr/lib/python3.7/site-packages/pipenv/vendor/requirementslib/models/requirements.p[65/705]
704, in from_line
line, extras = _strip_extras(line)
TypeError: 'module' object is not callable
```


```
pipenv run python -m pip install -U 'pip==18.0'
```

# Development

See https://git.microquake.org/rio-tinto/infrastructure/wikis/home for system-wide standards

It's recommended to run code through a linter, formatter, and static type checker before committing.

## Linting

Linting will catch any basic programming errors and enforce basic coding standards.
Pylint is the linter for this project. See https://www.pylint.org for more information
A .pylintrc is included in this repo that will configure the linter with our team's standards.

Run the linter like this for a single file:

```
pylint ./bin/send_api_data_to_channel.py
```

Or like this to cover a package you're intereted in:
```
pylint spp.utils.application
```

Integrations are available for most editors to show these hints inline.

## Formatting

To unify our coding style, run your code through a code formatter. This way we'll avoid unproductive conversations about brackets and spaces ;)
black is the code formatter for this project. See https://github.com/ambv/black for more information.

To run:

```
black ./bin/send_api_data_to_channel.py 
```

Integrations are available for most editors to run this every time you save.

## Static type checking

Type annotations and static type checking serves two purposes:
- Acts as machine-checked documentation 
- Helps us more quickly debug errors

mypy is the type checker used for this project. See http://mypy-lang.org/ for more information.

To run:
```
mypy ./bin/send_local_data_to_channel.py --ignore-missing-imports
```

