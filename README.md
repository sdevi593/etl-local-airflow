# ETL-Local-Airflow
An ETL Project from Local to a Data Warehouse using Airflow as Scheduler

Prerequisites:
1. Python 3.6 or more
2. XAMPP Application
3. Airflow

Data Source
A bunch of dataset with various type:
1. Excel (xls, xlsx)
2. CSV
3. JSON
4. db
5. sqlite

What is ETL?
ETL is a process of extracting data from a source/more, cleaning it, and load it to a data warehouse to be analyzed later by Data Analyze
![image](https://user-images.githubusercontent.com/59094767/123830009-2015fe00-d92d-11eb-9698-642d59d2cc88.png)



Set Up Applications
I was using Anaconda3,Spyder, XAMPP on Windows for this case in the beginning and move to linux in the end for airflow process, because as far as I know, windows doesn't support it currently.Thus, I'm genuinely suggesting you to use linux/mac from the start
1. Download Anaconda3 and XAMPP on Windows and install it directly
2. After the applications has been downloaded successfully, open spyder on anaconda navigator and open XAMPP Control Panel on your computer
3. Activate Apache and MySQL Module on XAMPP Control Panel by clicking Start, then open the database system via browser : http://localhost/phpmyadmin
4. Import the relevant modules, extract data from local data source, Transform the data , and then, connect to MySql XAMPP using sqlite3 command and load the clean data to data    warehouse that I had prepared beforehand
5. A new table contains your data will be emerged on your database then

Airflow
Unfortunately airflow can't be used easily on windows
Thus, I download Oracle VM VirtualBox to create a linux virtual machine where I can run the airflow
1. Download Oracle VM VirtualBox and ubuntu ISO file (ubuntu-20.04.2.0-desktop-amd64)
2. After Virtual Machine has been set up, install pip (sudo apt install python3-pip), and XAMPP(download XAMPP first, then install it via terminal by walking to the file installer location befor type : sudo chmod 755 "namafile.installer.run" and enter
3. After finished setting up the xampp, follow this guidance to set up airflow in linux : https://airflow.apache.org/docs/apache-airflow/stable/start/local.html
4. After getting the airflow ready and its webserver connected, open the given local host : localhost:8080 via URL section in browser
5. Create a DAG scheduler script which will schedule the updating process of your database
6. Run it, and if it's successfull, it will be presented on your DAG section on web

