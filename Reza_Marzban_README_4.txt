Question 4. Write a MapReduce job to calculate the difference in the average annual petroleum consumption
			in barrels for fuelType1 between electric cars and gasoline cars.

How to run Reza_Marzban_Program_4.java:
	javac Reza_Marzban_Program_4.java
	jar cfe Reza_Marzban_Program_4.jar Reza_Marzban_Program_4 *.class
	export HADOOP_CLASSPATH=Reza_Marzban_Program_4.jar
	hadoop Reza_Marzban_Program_4 [input csv file address] [output folder address]
	*you should specify exact output folder address. (the folder should not exist. the hadoop will create the new folder).
	

