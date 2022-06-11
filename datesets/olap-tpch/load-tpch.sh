. scripts/env.sh

cd $DBGEN
cp *.tbl $COOL_NAME/olap-tpch/scripts
cd  $COOL_NAME/olap-tpch/scripts
sed -i "" 's/,/./g' orders.tbl
sed -i "" 's/,/./g' customer.tbl
sed -i "" 's/,/./g' region.tbl
sed -i "" 's/,/./g' nation.tbl

cd  $COOL_NAME/olap-tpch/scripts
python merge.py
python dim.py

cd $COOL_NAME
java -cp ./cool-core/target/cool-core-0.1-SNAPSHOT.jar com.nus.cool.functionality.CsvLoader tpc-h-10g olap-tpch/table.yaml olap-tpch/scripts/dim.csv olap-tpch/scripts/data.csv datasetSource
