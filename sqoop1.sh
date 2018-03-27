source config.sh
 


 
function beelineFunc()
{
./beeline.sh<<EOF

show tables

EOF
}

beelineFunc &> hivetablesss.txt
hivetables=`cat hivetablesss.txt`
#grep -F -x -v -f tablename.txt  hivetablesss.txt
#diff -U $(wc -l < fileA) tablename.txt hivetablesss.txt | sed -n 's/^-//p' > fileC

IFS="
"

for i in `cat tablename.txt`

do
IFS="
"

echo $i;       
 
	rename1=$(echo "${i// /_}")
    
if grep -w -i "$rename1" hivetablesss.txt;then
echo "table already exist in database"
else

echo $rename1
echo "table not exist"
sqoop import --connect $JDBC:$MYSQL://$HOSTNAME/$DATABASE --username $USERNAME --password $PASS --table $i --m 1 --delete-target-dir --target-dir $DIR_PATH/$rename1 --hive-import --create-hive-table --hive-table $HIVE_DB.$rename1;

hdfs dfs -cp $DIR_PATH/$rename1/part-m-00000 $CSV_PATH/$rename1.csv

	if [ -d $DIR_PATH/$rename1 ];then

  	echo "exist"
	else

	sqoop import --connect $JDBC:$MYSQL://$HOSTNAME/$DATABASE --username $USERNAME --password $PASS --table $i --m 1 --delete-target-dir --target-dir $DIR_PATH/$rename1;
	hdfs dfs -cp $DIR_PATH/$rename1/part-m-00000 $CSV_PATH/$rename1.csv


	fi

fi
done
