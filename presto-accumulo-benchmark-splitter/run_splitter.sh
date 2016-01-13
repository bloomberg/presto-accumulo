if [[ $# -ne 2 ]]; then
    echo "usage: sh run_splitter.sh [table] [csv.splits]"
    exit 1
fi

java -jar target/presto-accumulo-benchmark-splitter-0.131.jar accumulo.dev.shared-ny1 gb-mesosmaster-dny1.bdns.bloomberg.com:4000 root secret $1 $2
