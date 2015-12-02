#!/bin/sh

if [[ -z $PRESTO_HOME ]] ; then
	echo "PRESTO_HOME is not set"
	exit 1
fi

function check_exit_code() {
	if [[ $? -ne 0 ]] ; then
        	echo "Failed, exiting"
		exit 1
	fi
}

echo "Building with Maven..."
mvn clean package
check_exit_code

echo "Making plugin directory..."
sudo mkdir -p $PRESTO_HOME/plugin/accumulo/
check_exit_code

echo "Copying plugin and dependencies..."
sudo cp target/presto-accumulo-0.*/* $PRESTO_HOME/plugin/accumulo/
check_exit_code

sudo cp etc/catalog/accumulo.properties $PRESTO_HOME/etc/catalog/
check_exit_code

echo "Done."

