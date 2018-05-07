#!/bin/sh -e

curl -L http://search.maven.org/remotecontent?filepath=org/graphstream/gs-core/1.2/gs-core-1.2.jar > gs-core-1.2.jar
curl -L http://search.maven.org/remotecontent?filepath=org/graphstream/gs-ui/1.2/gs-ui-1.2.jar     > gs-ui-1.2.jar
curl -L http://search.maven.org/remotecontent?filepath=org/graphstream/pherd/1.0/pherd-1.0.jar     > pherd-1.0.jar
curl -L http://search.maven.org/remotecontent?filepath=org/graphstream/mbox2/1.0/mbox2-1.0.jar     > mbox2-1.0.jar
