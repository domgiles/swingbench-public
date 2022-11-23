# Swingbench Public Repository
This repository serves as a public location to share the public aspects of swingbench. It will act as a location to download the latest version of the code and a place to raise issues.

If you encounter a problem please create a new issue. in the menu above describing the problem in as much details as possible. Important information includes

* Swingbench version number
* Platform (Mac, Windows, Linux)
* JDK Version (including whether it's OpenJDK or Oracle JDK)
* what you were trying to do
* debug information (collected with the command line option "-debug")
* Screen shots only where necessary, Please don't screen shot an error just copy the text into the issue. 

I will also host the latest build of the code at this location as well as the source code for the transactions used in the benchmarks.

For problems please raise an issues on this web site (In menu at the top of this page).

### Installation...

To install Swingbench all you need to do is **ensure you have Java 17 (or later) jdk or runtime in your executable path**, and then unzip the swingbench zip file. Then change into the bin/winbin directory and run the files from there.
```shell script
unzip swingbench
```
On Microsoft Windows use WinZip to perform this operation.

### Setting Up
Before you can run a test/benchmark you need to create a schema and install a set of sample data. This is done via the wizards that are installed in the bin directory. They can be run in command line or graphical modes. There are four wizards to choose from.
* oewizard : This will install a simple order entry schema that is used to create a heavy write workload
* shwizard : This will install a simple star schema that is used to create a analytics workload
* jsonwizard : This will install a simple JSON schema that is used to create a JSON CRUD workload
* tpcdswizard : This will in a TPC-DS like schema that is used to create a complex analytics workload

To run any of the wizards either double click on the one of the wizards described above if you are running on a GUI with a file manager. Or run it from the command line with
```shell script
./oewizard
```   
Or on Windows from the winbin directory
```cmd
oewizard.bat
```
![OEWizard](https://github.com/domgiles/swingbench-public/blob/master/img/wizard.png)

The wizard will walk you through the various options you can specify for the configuration of the database schema. Enabling you to define it's size and what features of the Oracle Database you want to use or even how many threads you want to use in the data generation. On completition it will create the user, tables, logic and generate the data directly into the database. After it completes generating the data it will then index and analyse the tables. It's final step will be to validate the schema to insure everything has been created correctly. If it hasn't it might be possible to use the ```sbutil``` command line tool to rebuild certain aspects, such as the indexes or stored procedures.

You can also run the wizards from the command in a non interactive mode where you specify all off the options. For example
```shell script
./oewizard -cl -create -cs //localhost/PDB1 -u soe -p soepassword -scale 50 -tc 32 -dba "sys as sysdba" -dbap welcome1 -ts +DATA
```
The command above will run a wizard in command line mode setting username, password, connectstring, a scale of 50 (50GB), using 32 threads also setting the dba username, password and tablespace. For command line generation you will be expected to specify all of the parameters needed on the command line.

For a complete list of all of the many possible command line parameters for the wizards run the following command
```shell script
./oewizard -h
```
### Running a workload
After building a schema you should be in a position to run a workload against it. This done by running the workload generator with one three front ends.

![Swingbench](https://github.com/domgiles/swingbench-public/blob/master/img/swingbench.png)

* **swingbench** : Is a graphically rich frontend that enables you to modify all of the parameters as well as run and monitor the workloads.
* **minibench** : Is a simpler frontend that enables you to run workloads and monitor but parameters must be set in the command line or by editing the config file.
* **charbench** : Is a command line only tool where all of the parameters are set on the command line and metrics like latency and throughput are written to standard out or a file.
To start one of the variants of the load generator simply run a command like the the following
```shell script
./swingbench -c ../configs/SOE_Server_Side_V2.xml
``` 

This will launch swingbench in graphical mode using the config file for the Order Entry workload. Unless additional command line pararmeters are specified swingbench will use the parameters held in the configuration file. If you don't specify a configuration file from the command line swingbench will ask you for one. Note for minibench and charbench you must supply a configuration file or you'll receive an error. Swingbench allows you to specify more complex command line options. For instance the command line below will launch swingbench over riding the connectstring, username, password, user connections, think time and the location and size of the window.
```shell script
./swingbench -c ../configs/SOE_Server_Side_V2.xml -u soe -p soe -cs //localhost/orderentry -min 0 -max 10 -intermin 200 -intermax 500 -dim 1024,768 -pos 100,100 
```
Similarly if you are using charbench you use the command line parameters to configure a run. The command line below will run charbench and out useful metrics similar to the way vmstat runs
```shell script
./charbench -c ../configs/SOE_Server_Side_V2.xml -u soe -p soe -uc 4 -cs //localhost/orderentry -min 0 -max 10 -intermin 200 -intermax 500 -v users,tpm,tps,errs,vresp
Author  :	 Dominic Giles
Version :	 2.6.0.1137

Results will be written to results.xml.
Hit Return to Terminate Run...

Time     Users       TPM      TPS     Errors   NCR   UCD   BP    OP    PO    BO    SQ    WQ    WA    
10:58:35 [0/4]       0        0       0        0     0     0     0     0     0     0     0     0     
10:58:37 [4/4]       0        0       0        0     0     248   414   0     0     0     0     0     
10:58:38 [4/4]       8        8       0        0     0     32    213   0     19    209   0     0     
10:58:39 [4/4]       17       9       0        0     19    39    33    0     10    198   0     0     
10:58:40 [4/4]       26       9       0        0     7     12    150   0     10    198   0     0     
10:58:41 [4/4]       37       11      0        93    7     10    109   0     10    198   0     0     
10:58:42 [4/4]       46       9       0        22    7     30    89    0     28    198   0     0
```
charbench has similar parameters to swingbench but with a few additions. Like swingbench to get a comprehensive list use the following command.
```shell script
./charbench -h
```
By default swingbench outputs the results of a run in XMl to the "Output" tab in the UI whilst charbench by default will output the results to a file called results.xml. You can change the location of the results file in charbench with the ```-r``` c0mmand line parameter.

### Formatting the Results File
Whilst XML is simple to parse by a computer it is not necessarily ideal for humans. I supply a utility called ```results2pdf``` in the same bin/winbin directory. This allows you to convert the XML of a run into a pdf file. For example suppose you generated a XML files called results0001.xml to convert it into a pdf file called resultsrun1.pdf you would use a command similar to the own below
```shell script
./results2pdf -c results0001.xml -o resultsrun1.pdf
```
This will convert some of the data held in the XML file into graphs and nicely formatted tables.
**NOTE : At this time you'll need to use a JDK 17 to run this utilites. It should run on Linux, Windows and MacOS

It's also possible that you might want to compare a number of files and then render the results either as human readable tables or CSV files. For this reason I've included a python script in the utils directory. The choice of python was simply to simplify the modification of the script by a user to control what is is rendered. The script that is shipped will pull out the the key metrics as well as the displaying the average,10th,50th and 90th percentile... If multiple result files are supplied the script will display them next to one another. If you use the ```-c``` or ```--csv``` option it will output in comma seperated format making it easier to load into excel or Google Sheets. 
```shell script
python parse_results.py -r ../bin/results.xml ../bin/results00001.xml 
+-------------------------------------------+--------------------------+--------------------------+
| Attribute                                 |       results.xml        |     results00001.xml     |
+-------------------------------------------+--------------------------+--------------------------+
| Benchmark Name                            | "Order Entry (PLSQL) V2" | "Order Entry (PLSQL) V2" |
| Connect String                            |     //localhost/soe      |     //localhost/soe      |
| Time of run                               |   03-Oct-2020 16:49:11   |   03-Oct-2020 18:09:28   |
| Minimum Inter TX Think Time               |            0             |            0             |
| Maximum Inter TX Think Time               |            0             |            0             |
| Maximum Intra TX Think Time               |            0             |            0             |
| Maximum Intra TX Think Time               |            0             |            0             |
| No of Users                               |            4             |            4             |
| Total Run Time                            |         0:10:00          |         0:10:00          |
| Average Tx/Sec                            |          394.55          |          502.04          |
| Maximum Tx/Min                            |          30302           |          35015           |
| Total Completed Transactions              |          236727          |          301222          |
|                                           |                          |                          |
| Average Transaction Response Time         |                          |                          |
| Order Products                            |          16.78           |          12.82           |
| Warehouse Activity Query                  |           6.99           |           7.24           |
| Customer Registration                     |           9.39           |           7.16           |
| Update Customer Details                   |           3.83           |           3.21           |
| Sales Rep Query                           |           4.11           |           3.12           |
| Warehouse Query                           |           3.94           |           3.48           |
| Browse Products                           |           8.89           |           7.12           |
| Browse Orders                             |           6.32           |           5.29           |
| Process Orders                            |           7.37           |           5.87           |
|                                           |                          |                          |
| 10th Percentile Transaction Response Time |                          |                          |
| Order Products                            |           8.00           |           7.00           |
| Warehouse Activity Query                  |           3.00           |           4.00           |
| Customer Registration                     |           5.00           |           4.00           |
| Update Customer Details                   |           2.00           |           2.00           |
| Sales Rep Query                           |           1.00           |           1.00           |
| Warehouse Query                           |           2.00           |           2.00           |
| Browse Products                           |           3.00           |           3.00           |
| Browse Orders                             |           2.00           |           2.00           |
| Process Orders                            |           4.00           |           3.00           |
|                                           |                          |                          |
| 50th Percentile Transaction Response Time |                          |                          |
| Order Products                            |          13.00           |          11.00           |
| Warehouse Activity Query                  |           5.00           |           6.00           |
| Customer Registration                     |           7.00           |           5.00           |
| Update Customer Details                   |           2.00           |           2.00           |
| Sales Rep Query                           |           2.00           |           2.00           |
| Warehouse Query                           |           2.00           |           2.00           |
| Browse Products                           |           7.00           |           5.00           |
| Browse Orders                             |           4.00           |           4.00           |
| Process Orders                            |           5.00           |           4.00           |
|                                           |                          |                          |
| 90th Percentile Transaction Response Time |                          |                          |
| Order Products                            |          23.00           |          17.00           |
| Warehouse Activity Query                  |           9.00           |           9.00           |
| Customer Registration                     |          11.00           |           8.00           |
| Update Customer Details                   |           5.00           |           3.00           |
| Sales Rep Query                           |           6.00           |           4.00           |
| Warehouse Query                           |           5.00           |           4.00           |
| Browse Products                           |          13.00           |          10.00           |
| Browse Orders                             |           9.00           |           7.00           |
| Process Orders                            |           9.00           |           6.00           |
+-------------------------------------------+--------------------------+--------------------------+
```
### Fixing problems with sbutil
It is possible that you may run into problems during the creation of the schema for any number of reasons i.e. run out of space, not enough temp space to create indexes etc. Since the creation of a schema using the waizards can take along time I've create a utility ```sbutil``` to solve many of the common issues as well as provide a tool to change the shape of the created schema by increasing it's size, enabling compression or even partitioning the data. SBUtil is located in the bin directory. The following command validates a newly created schema
```shell script
./sbutil -soe -cs //localhost/soe -soe -u soe -p soe -val
There appears to be an issue with the current Order Entry Schema. Please see below.
--------------------------------------------------
|Object Type    |     Valid|   Invalid|   Missing|
--------------------------------------------------
|Table          |        10|         0|         0|
|Index          |        26|         0|         0|
|Sequence       |         5|         0|         0|
|View           |         2|         0|         0|
|Code           |         0|         1|         0|
--------------------------------------------------
List of missing or invalid objects. 
Invalid Code : ORDERENTRY, 
```
This shows that there is a problem with the ```ORDERENTRY``` package. Instead of running a whole build again we could simply ask ```sbutil``` to reload the package.
```shell script
$> ./sbutil -soe -cs //localhost/soe -soe -u soe -p soe -code
Reloading PL/SQL Packages
Reloaded PL/SQL Package. Completed in : 0:00:00.273
$> ./sbutil -soe -cs //localhost/soe -soe -u soe -p soe -val
The Order Entry Schema appears to be valid.
--------------------------------------------------
|Object Type    |     Valid|   Invalid|   Missing|
--------------------------------------------------
|Table          |        10|         0|         0|
|Index          |        26|         0|         0|
|Sequence       |         5|         0|         0|
|View           |         2|         0|         0|
|Code           |         1|         0|         0|
--------------------------------------------------
```
You can also use the sbutil script to show the contents of a schema i.e.
```shell script
./sbutil -soe -cs //localhost/soe -soe -u soe -p soe -tables
Order Entry Schemas Tables
----------------------------------------------------------------------------------------------------------------------
|Table Name                  |                Rows|              Blocks|           Size|   Compressed?|  Partitioned?|
----------------------------------------------------------------------------------------------------------------------
|INVENTORIES                 |             899,129|              22,343|        176.0MB|      Disabled|            No|
|JUST_A_TABLE                |           1,000,000|              18,901|        149.0MB|      Disabled|            No|
|ORDER_ITEMS                 |             806,135|               6,424|         70.0MB|      Disabled|            No|
|ORDERS                      |             173,095|               2,392|         32.0MB|      Disabled|            No|
|CUSTOMERS                   |             106,470|               1,762|         21.0MB|      Disabled|            No|
|ADDRESSES                   |             150,000|               1,762|         19.0MB|      Disabled|            No|
|LOGON                       |             278,392|               1,006|         12.0MB|      Disabled|            No|
|CARD_DETAILS                |             150,000|               1,006|         11.0MB|      Disabled|            No|
|PRODUCT_DESCRIPTIONS        |               1,000|                 124|         1024KB|      Disabled|            No|
|PRODUCT_INFORMATION         |               1,000|                 124|         1024KB|      Disabled|            No|
|ORDERENTRY_METADATA         |                   4|                 124|         1024KB|      Disabled|            No|
|WAREHOUSES                  |               1,000|                 124|         1024KB|      Disabled|            No|
|JUST_A_SMALL_TABLE          |              20,000|                 124|         1024KB|      Disabled|            No|
----------------------------------------------------------------------------------------------------------------------
                                                            Total Space         495.0MB

```
For a full break down of the possible commands use the ```-h``` command line option.
