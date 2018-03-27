spark.sql("CREATE TABLE if NOT EXISTS miti_db.`ProductVendor_Cleansed`(\
	  `productid` int,  \
	  `businessentityid` int, \
 	  `averageleadtime` int, \
 	  `standardprice` double, \
 	  `lastreceiptcost` double,\
  	  `lastreceiptdate` string, \
 	  `minorderqty` int, \
 	  `maxorderqty` int, \
     `onorderqty` int, \
     `unitmeasurecode` string, \
 	  `modifieddate` string) \
	ROW FORMAT delimited \
	fields terminated by ',' \
    location '/user/miti/project_shellscript/alltables_cleansed/ProductVendor'	\
    tblproperties(\"skip.header.line.count\"=\"1\")")

spark.sql("CREATE TABLE if NOT EXISTS miti_db.`SalesOrderDetail_cleansed`(\
 `salesorderid` int, \
 `salesorderdetailid` int,\
 `carriertrackingnumber` string,\
`orderqty` int, \
 `productid` int, \
 `specialofferid` int,\
 `unitprice` double, \
 `unitpricediscount` double,\
 `linetotal` double, \
 `rowguid` string, \
 `modifieddate` string) \
 ROW FORMAT delimited \
 fields terminated by ',' \
 location '/user/miti/project_shellscript/alltables_cleansed/SalesOrderDetail'  \
 tblproperties(\"skip.header.line.count\"=\"1\")")

spark.sql("CREATE TABLE if not exists miti_db.`Product_Cleansed`(\
  `productid` int,\
`name` string, \
  `productnumber` string,\
 `makeflag` boolean,\
  `finishedgoodsflag` boolean, \
  `color` string,\
`safetystocklevel` int,\
 `reorderpoint` int, \
  `standardcost` double,\
  `listprice` double,\
 `size` string,\
  `sizeunitmeasurecode` string, \
  `weightunitmeasurecode` string,\
	`weight` double,\
`daystomanufacture` int,\
`productline` string,\
 `class` string,\
`style` string,\
  `productsubcategoryid` int,\
`productmodelid` int,\
  `sellstartdate` string,\
  `sellenddate` string,\
  `discontinueddate` string,\
  `rowguid` string,\
   `modifieddate` string)\
 ROW FORMAT delimited \
 fields terminated by ',' \
 location '/user/miti/project_shellscript/alltables_cleansed/Product' \
 tblproperties(\"skip.header.line.count\"=\"1\")")

spark.sql("CREATE TABLE if not exists miti_db.`PurchaseOrderDetail_cleansed`(\
	  `purchaseorderid` int,\
	  `purchaseorderdetailid` int,\
	  `duedate` string,\
	  `orderqty` int,\
	 `productid` int,\
	`unitprice` double,\
	`linetotal` double,\
	`receivedqty` double,\
	`rejectedqty` double,\
	`stockedqty` double,\
	`modifieddate` string)\
	ROW FORMAT delimited \
   fields terminated by ',' \
   location '/user/miti/project_shellscript/alltables_cleansed/PurchaseOrderDetail'\
   tblproperties(\"skip.header.line.count\"=\"1\")")

spark.sql("CREATE TABLE if not exists miti_db.`vendor_cleansed`(\
	  `businessentityid` int,\
	  `accountnumber` string,\
	  `name` string,\
	  `creditrating` tinyint,\
	  `preferredvendorstatus` boolean,\
	   `activeflag` boolean,\
	  `purchasingwebserviceurl` string,\
	 `modifieddate` string)\
   ROW FORMAT delimited \
   fields terminated by ',' \
   location '/user/miti/project_shellscript/alltables_cleansed/Vendor'\
   tblproperties(\"skip.header.line.count\"=\"1\")")
spark.sql("CREATE TABLE if not exists miti_db.`BusinessEntityAddress_Cleansed`(\
	  `businessentityid` int,\
	  `addressid` int,\
	  `addresstypeid` int,\
	  `rowguid` string,\
	  `modifieddate` string)\
	  ROW FORMAT delimited \
   fields terminated by ',' \
   location '/user/miti/project_shellscript/alltables_cleansed/BusinessEntityAddress'\
   tblproperties(\"skip.header.line.count\"=\"1\")")
spark.sql("CREATE EXTERNAL TABLE if not exists `miti_db.Address_CLeansed`(\
	  `addressid` int,\
	  `addressline1` varchar(60),\
 `addressline2` varchar(60),\
 `city` varchar(30),\
 `stateprovinceid` int,\
 `postalcode` varchar(15),\ 
	  `spatiallocation` string,\
`rowguid` varchar(64),\
 `modifieddate` timestamp)\
	  ROW FORMAT delimited \
   fields terminated by ',' \
   location '/user/miti/project_shellscript/alltables_cleansed/BusinessEntityAddress'\
   tblproperties(\"skip.header.line.count\"=\"1\")")
