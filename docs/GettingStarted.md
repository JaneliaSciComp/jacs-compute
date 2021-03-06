# Getting Started

## Setting up the development environment

To build the application you can either install gradle 4.x locally on your machine using the appropriate package manager for your OS
(brew or macports for OSX, yum for Centos based Linux distros, apt-get for Debian based Linux distros) or use gradle scripts packaged
with the project.

Because the build is configured to run the integration tests as part of the full build you also need to have access to a Mongo
database server. The default configuration expects the database server to be running on the development machine but it doesn't have to.

## Setup MongoDB

To install MongoDB on MacOS:

With Homebrew:
`brew install mongodb`

With macports:
`sudo port install mongodb`

On Centos based Linux distributions (Centos, Scientific Linux) you can use:
`yum install mongodb-org-server`

On Debian based Linux distributions (Debian, Ubuntu) you can use:
`sudo apt-get install mongodb-org`

Once MongoDB is installed on your machine you really don't have to do anything else because the tests will create the needed databases and
the collections as long as the user has prvileges to do so.

## Building and running the application

### Building the application

`gradle build`
or
`./gradlew build`

### Running only the integration tests

`./gradlew integrationTest`

If you want to use a different test database than the one running locally on your development machine you can create a configuration file
in which you override the database connection settings and then use JACS2_CONFIG_TEST environment variable to point to it, eg.,
`JACS2_CONFIG_TEST=/my/prefered/location/for/dev/my-config-test.properties ./gradlew integrationTest`

Keep in mind that since the integrationTests are configured to run as part of the build check stage you also need to have the environment variable
set you you run the build:
`JACS2_CONFIG_TEST=/my/prefered/location/for/dev/my-config-test.properties ./gradlew build`

For example my-config-test.properties could look as below if you want to use the dev mongo database. I recommend to prefix the database name with your
user name so that your tests will not clash with other users' tests in case the build runs simultaneously.
`
MongoDB.ConnectionURL=mongodb://dev-mongodb:27017
MongoDB.Database=myusername_jacs_test
`

and to build the application you simply run:

`JACS2_CONFIG_TEST=$PWD/my-config-test.properties ./gradlew clean build installDist`

Note:

When using the environment variable to reference the configuration use the full path in order to guarantee that the right properties are being used.

### Package the application

`./gradlew installDist`

Note that 'installDist' target will not run any unit tests or integration tests.

### Run the application

To run the async services with the default settings which assume a Mongo database instance running on the same machine where the web server is running:

`jacs2-asyncweb/build/install/jacs2-asyncweb/bin/jacs2-asyncweb`

If you want to debug the application you can start the application with the debug agent as below:

`JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" jacs2-asyncweb/build/install/jacs2-asyncweb/bin/jacs2-asyncweb`

The default settings could be overwritten with your own settings in a java properties file that contains only the updated properties
and then use JACS2_CONFIG environment variable to reference the settings file, e.g.

`JACS2_CONFIG=/usr/local/etc/myjacs2-config.properties jacs2-asyncweb/build/install/jacs2-asyncweb/bin/jacs2-asyncweb`

### Run multiple jacsV2 instances against the same mongo database store

If you want to run multiple instances of JacsV2, whethere it's on the same or on different machines, but against the same mongo database, in order to prevent
services from being processed multiple times, you need every instance of JacsV2 to use a different "service.queue.id". This can be specified either in the configuration
properties and then each instance will have to use its own configuration properties or in the command line, e.g.:

`JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" jacs2-asyncweb/build/install/jacs2-asyncweb/bin/jacs2-asyncweb -Dservice.queue.id=MyQueueID`

As a note you can overwrite every configuration property in the command line using -D<propertyName>=<value>

### Run the admin dashboard

To run the dashboard, navigate to the jacs2-admin directory after building and type npm start.  This will kick off the administration dashboard in hot-deploy mode.
Any items that you change in the source will be rebuilt an deployed to the server, so you can edit your React pages live.

### Test the application

Here are a few service invocation examples:

* Generate sample MIPs and movies:

`
curl --request POST --url http://goinac-ws1:8080//api/v2/async-services/getSampleMIPsAndMovies --header 'acc: application/json' --header 'content-type: application/json' --data '{"processingLocation": "LOCAL", "args": ["-sampleId", "2230165384508473442" ,"-objective", "20x", "-sampleDataDir", "/home/goinac/Work/jacs-2/tt/missing"]}'
`

* Generate LSM metadata for a single LSM file on the LSF cluster

`
curl --request POST --url http://goinac-ws1:8080/api/v2/async-services/lsmFileMetadata --header 'acc: application/json' --header 'content-type: application/json' --header 'postman-token: c8f66acb-91e1-00a7-944a-12da50719688' --data '{"processingLocation": "LSF_DRMAA", "args": ["-inputLSM", "/home/goinac/Work/jacs-2/tt/missing/f1.lsm", "-outputLSMMetadata", "/home/goinac/Work/jacs-2/tt/missing/f1.json"]}'
`

* Generate sample LSM metadata:

`
curl --request POST --url http://goinac-ws1:8080/api/v2/async-services/getSampleLsmMetadata --header 'acc: application/json' --header 'content-type: application/json' --data '{"processingLocation": "LOCAL", "args": ["-sampleId", "2230165384508473442" , "-objective", "20x", "-sampleDataDir", "/home/goinac/Work/jacs-2/tt/missing"]}'
`

* Run a Fiji macro on the LSF cluster

`
curl --request POST --url http://goinac-ws1:8080/api/v2/async-services/fijiMacro --header 'acc: application/json' --header 'cache-control: no-cache' --header 'content-type: application/json' --header 'postman-token: 224b8f88-fd54-69f9-457f-0ec5e6c23a1f' --data '{"processingLocation": "LSF_DRMAA",	"args": ["-macro", "Basic_MIP_StackAvi.ijm" , "-macroArgs", "/home/goinac/Work/jacs-2/tt/missing/mips,FLPO_20160121130448632_61713,,/home/goinac/Work/jacs-2/tt/missing/FLPO_20160121130448632_61713.lsm,,,,r,1,2,mips:movies:legends:bcomp"],"resources": {"gridAccountId": "jacs", "gridPE": "batch 8", "gridResourceLimits": "haswell=true,h_rt=1200" }}'
`

* Run the aligner

`
curl --request POST --url http://goinac-ws1:8080/api/v2/async-services/align --header 'acc: application/json' --header 'cache-control: no-cache' --header 'content-type: application/json' --data '{"processingLocation": "LOCAL", "args": ["-i1File", "/nrs/scicompsoft/yuy/flylight/brains/issue/A4_ZB4_T1_CEN_20160318_24_40X_R1.v3draw", "-i1Channels", "4", "-i1Ref", "4", "-i1Res", "0.4413x0.4413x0.44", "-config", "/nrs/scicompsoft/yuy/brainaligner/systemvars.apconf", "-templateDir", "/nrs/scicompsoft/yuy/configured_templates", "-toolsDir", "/nrs/scicompsoft/yuy/Toolkits", "-mountingProtocol", "DPX PBS Mounting", "-gender", "f", "-resultsDir", "/home/goinac/Work/jacs-2/tt/missing/align"]}'
`

