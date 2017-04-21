# basic-tme-transformer

## Introduction
The Basic TME Transformer generates UPP representations of TME concepts for a set of taxonomies:
Genres, Locations, Special Reports, Topics, Subjects, Sections and Alphaville Series.

## Installation

Download the source code, dependencies and test dependencies:

        go get -u github.com/Financial-Times/basic-tme-transformer
        cd $GOPATH/src/github.com/Financial-Times/basic-tme-transformer
        go get -t


## Running locally

1. Run the tests and install the binary:

        cd $GOPATH/src/github.com/Financial-Times/basic-tme-transformer
        go test -race ./...
        go install

1. Run the binary (using the `help` flag to see the available optional arguments):

        $GOPATH/bin/basic-tme-transformer [--help]

1. Test:

    1. Either using curl:

            curl http://localhost:8080/transformers/topics/__ids | json_pp

    1. Or using [httpie](https://github.com/jkbrzt/httpie):

            http GET http://localhost:8080/transformers/topics/__ids

## Build and deployment
How can I build and deploy it (lots of this will be links out as the steps will be common)

e.g.
* Built by Docker Hub on merge to master: [coco/basic-tme-transformer](https://hub.docker.com/r/coco/basic-tme-transformer/)
* CI provided by CircleCI: [basic-tme-transformer](https://circleci.com/gh/Financial-Times/basic-tme-transformer)

## Endpoints
For Swagger style documentation, see [here](swagger.yml).

## Healthchecks
The standard admin endpoints are supported:
- /__health - Checks whether all taxonomies have their data loaded.
- /__gtg - Checks whether all taxonomies have their data loaded.
- /__build-info

