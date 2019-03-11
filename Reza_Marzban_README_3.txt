Question 3. For electric cars only, write a MapReduce job to display the average city MPG for fuelType1.


How to run Reza_Marzban_Program_3.java:
	javac Reza_Marzban_Program_3.java
	jar cfe Reza_Marzban_Program_3.jar Reza_Marzban_Program_3 *.class
	export HADOOP_CLASSPATH=Reza_Marzban_Program_3.jar
	hadoop Reza_Marzban_Program_3 [input csv file address] [output folder address]
	*you should specify exact output folder address. (the folder should not exist. the hadoop will create the new folder).
	

