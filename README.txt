Welcome to Glimmer

 Glimmer is a Hadoop based distributed indexing system for building MG4J indexes from RDF tuples in NQuads format.  It also includes a simple web application for querying the resulting indexes <TODO +something about this+scorer..).  Both the indexing and web application are written in Java.  There are also a few shell scripts to execute the steps need to build the indexes for a given NQuads file and query the resulting index from the command line.  Glimmer is an academic project and is the implementation of distributed indexing detailed in the paper 'Distributed Indexing for Semantic Search' by Peter Mika(Yahoo Research).

Prerequisites
-------------
- Java JDK 6. Other versions may work. The code was written against version 1.6.0_31
- A Maven installation.  We used version 3.0.3 during development.
- A Hadoop cluster. Probably version 0.20.205.0. The version of Hadoop we developed the code with is defined in the pom.xml file.
- If you want try out the web app an install of a Java servlet container such as Tomcat, Jetty etc..
- If you want a more usable interface to the MG4J query command-line you can install rlwrap to get command-line history/editing.

All other dependencies are jars that are automatically downloaded by Maven.

Index Building
--------------

1. Build an uber jar with mvn for building indexes with.

In the root directory of Glimmer run:

	mvn compile (or mvn test)
	mvn assembly:single

This will produce a jar Glimmer-?.?.?-SNAPSHOT-jar-with-dependencies.jar in the ./target directory. This jar contains the Glimmer classes + all dependencies zipped into one file.

2. The process of building an index with Glimmer consists of the following steps.  (The build-index.sh shell script is provided to automate the process.)

* Preprocess the NQuads tuple file to get:
  - A sorted unique list by subject with the subjects associated predicates, object, context. 
  - A sorted unique list of subjects resources.
  - A sorted unique list of predicates resources.
  - A sorted unique list of all resources(Subject, Predicate, Object & Context).

* Build minimal perfect hash functions over the unique sorted lists of resources. This function is a mapping of a given resource to its position in the unique sorted list.

* Build the 'horizontal' and 'vertical'(See the paper..) MG4J indexes.

* Compute the Document sizes.

* Build the MG4J document collection.

* Copy all the generated files to the desired location.


Querying
--------

Once you have the indexes build you can use the MG4J query command line class it.unimi.dsi.mg4j.query.Query to query them.  You may find the query-index.sh script helpful here.

You can also build and deploy the Java web application as follows:

1. Use the maven-war-plugin to build the war file.  From the projects root directory run the command

	mvn war:war

This will build the war file Glimmer-?.?.?-SNAPSHOT.war in the target directory.


