Question 1. Write a MapReduce job to count models of different makes of vehicles. Display the top 5makes.


How to run Reza_Marzban_Program_1.java:
	javac Reza_Marzban_Program_1.java
	jar cfe Reza_Marzban_Program_1.jar Reza_Marzban_Program_1 *.class
	export HADOOP_CLASSPATH=Reza_Marzban_Program_1.jar
	hadoop Reza_Marzban_Program_1 [input csv file address] [output folder address]
	*you should specify exact output folder address. (the folder should not exist. the hadoop will create the new folder).
	

