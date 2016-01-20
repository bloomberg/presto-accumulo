#!/bin/sh

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
mvn clean install -DskipTests
check_exit_code

echo "Copying jar to Accumulo lib directory..."
sudo cp target/presto-accumulo-iterators-0.*.jar $ACCUMULO_HOME/lib/ext
check_exit_code

#echo "Restarting Accumulo..."
#$ACCUMULO_HOME/bin/stop-here.sh
#sudo rm -rf $ACCUMULO_HOME/logs/*
#$ACCUMULO_HOME/bin/start-here.sh
#check_exit_code

echo "Done."

