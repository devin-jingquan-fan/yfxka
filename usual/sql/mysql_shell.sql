 mysql> SELECT SUBSTRING_INDEX(USER(), '@', -1) AS ip,  @@hostname as hostname, @@port as port, DATABASE() as current_database;
+-----------+-----------------+------+------------------+
| ip        | hostname        | port | current_database |
+-----------+-----------------+------+------------------+
| localhost | host001         | 3306 | kakadba          |
+-----------+-----------------+------+------------------+
1 row in set (0.00 sec)



mysql -hlocalhost -uroot -p




SHOW VARIABLES LIKE "secure_file_priv";


业务需求尝试着导入上万条数据，以上两种方式就特别慢，然后用命令行的方式导入，如下，把.cvs转换为.txt即可：（windows下“\r\n”） 
Load Data InFile ‘D:/import.txt’ Into Table “ ####”lines terminated by ‘\r\n’; 
然后报错： 
Error Code: 1290. The MySQL server is running with the –secure-file-priv option so it cannot execute this statement 
在网上查了一些资料发现如下解决方式： 
1.进入mysql查看secure_file_prive的值 
$mysql -u root -p 
mysql>SHOW VARIABLES LIKE “secure_file_priv”; 
secure_file_prive=null – 限制mysqld 不允许导入导出 
secure_file_priv=/tmp/ – 限制mysqld的导入导出只能发生在/tmp/目录下 
secure_file_priv=’ ’ – 不对mysqld 的导入 导出做限制

2、在目录C:\ProgramData\MySQL\MySQL Server 5.7下找到my.ini文件，然后修改 secure_file_prive为’ ‘,或者把导入文件放入指定的文件夹，即可完成导入；
