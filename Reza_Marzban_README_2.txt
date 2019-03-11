Question 2. For all vehicles, write a MapReduce job to get the average annual petroleum consumption in barrels for fuelType1for each make of vehicle.
			Your MapReduce jobs should print the make of vehicle make in descending order.

How to run Reza_Marzban_Program_2.java:
	javac Reza_Marzban_Program_2.java
	jar cfe Reza_Marzban_Program_2.jar Reza_Marzban_Program_2 *.class
	export HADOOP_CLASSPATH=Reza_Marzban_Program_2.jar
	hadoop Reza_Marzban_Program_2 [input csv file address] [output folder address]
	*you should specify exact output folder address. (the folder should not exist. the hadoop will create the new folder).
	

