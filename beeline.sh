HOSTNAME="jdbc:hive2://ec2-52-207-2-108.compute-1.amazonaws.com:10000"
DB="miti_db"
PRINCIPAL="hive/ec2-52-207-2-108.compute-1.amazonaws.com@SOLRS.NET"

beeline -u "${HOSTNAME}/${DB};principal=${PRINCIPAL};"

