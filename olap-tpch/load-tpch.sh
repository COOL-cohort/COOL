. env.sh

if [ "$#" -ne 1 ]; then
  echo "load-tpch.sh <data_source_name>"
  exit
fi

# create folder to store dz file.
mkdir -p cube/$1
mkdir -p cube/$1/version1

cd $DBGEN
cp *.tbl $COOL_NAME/olap-tpch/scripts
cd  $COOL_NAME/olap-tpch/scripts
sed -i "" 's/,/./g' orders.tbl
sed -i "" 's/,/./g' customer.tbl
sed -i "" 's/,/./g' region.tbl
sed -i "" 's/,/./g' nation.tbl

python merge.py
python dim.py

cd  $COOL_NAME/olap-tpch/
cp table.yaml cube/$1/version1/

java -Xmx16384m -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.loader.LocalLoader ./table.yaml ./scripts/dim.csv ./scripts/data.csv ../cube/$1/version1 1000000
