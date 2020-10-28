This repo has sample working code for testing pyspark on local machine

============
Prerequisite 
============
1. Download Spark from https://spark.apache.org/downloads.html
2. Open Git Bash, and change directory (cd) to the folder where you save the binary package and then unzip:
   $ tar -xvzf   spark-2.2.1-bin-hadoop2.7.tgz
3. Setup environment variables
   JAVA_HOME
   SPARK_HOME 
   Added ‘%SPARK_HOME%\bin’ to your path environment variable.
4. Verify the installation Verify command
   Run the following command in Command Prompt to verify the installation.
   %SPARK_HOME%\bin\spark-shell
6. run %SPARK_HOME%\bin\run-example.cmd SparkPi 10 on command promt. The job should run successfully
7. pip install pyspark==3.0.1      (give the exact spark version you downloaded)
8. you are good to go

https://kontext.tech/column/spark/287/debug-pyspark-code-in-visual-studio-code   

==================================================================
Run testsuite with codecoverage
==================================================================
2) install coverage for code coverage 
   coverage run -m pytest ./testsuite.py
   coverage report -m 
   coverage html