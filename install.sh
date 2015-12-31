#!/bin/sh

if [[ -z $PRESTO_HOME ]] ; then
	echo "PRESTO_HOME is not set"
	exit 1
fi

if [[ -z $ACCUMULO_HOME ]] ; then
	echo "ACCUMULO_HOME is not set"
	exit 1
fi

function check_exit_code() {
	if [[ $? -ne 0 ]] ; then
        	echo "Failed, exiting"
		exit 1
	fi
}

echo "Building with Maven... Sorry."
mvn clean package -DskipTests
check_exit_code

echo "Making plugin directory..."
sudo rm -rf $PRESTO_HOME/plugin/accumulo/
sudo mkdir -p $PRESTO_HOME/plugin/accumulo/
check_exit_code

echo "Copying plugin and dependencies..."
sudo cp target/presto-accumulo-0.*/* $PRESTO_HOME/plugin/accumulo/
check_exit_code

echo "Copying jar to Accumulo lib directory..."
sudo cp target/presto-accumulo-0.*/presto-accumulo-0.*.jar $ACCUMULO_HOME/lib
check_exit_code

echo "Restarting Accumulo..."
$ACCUMULO_HOME/bin/stop-here.sh
sudo rm -rf $ACCUMULO_HOME/logs/*
$ACCUMULO_HOME/bin/start-here.sh
check_exit_code

echo "Copying catalog config..."
sudo mkdir -p $PRESTO_HOME/etc/catalog
sudo cp etc/catalog/accumulo.properties $PRESTO_HOME/etc/catalog/
check_exit_code

echo "Done."

