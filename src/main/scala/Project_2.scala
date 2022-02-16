import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import java.util.Scanner
import java.sql.DriverManager
import java.sql.Connection
import com.mysql.cj.xdevapi.UpdateStatement
import java.io.File
import java.io.PrintWriter
import java.io.Console;




object Project_2 {
    
    def main(args: Array[String]): Unit={
                
        // declaring all variables needed for program
        var scanner = new Scanner(System.in)
        val log = new PrintWriter(new File("covidData.log"))
        var console= System.console();
      
        var userName = ""
        var userPassword = ""
        var covidProject = true


        // make the connection to mySQL
        val driver = "com.mysql.jdbc.Driver"
        // Modify for whatever port you are running your DB on
        val url = "jdbc:mysql://localhost:3306/Project_2"
        val username = "root"

        // DON'T FORGET TO DELETE PASSWORD BEFORE PUSHING TO GITHUB
        val password = "##################" // Update to include your password
        var connection:Connection = null 
        //val statement = connection.createStatement()


                      
                
        //method running the DataFrame creation
        val spark=SparkSession
        .builder
        .appName("sparkSQL")
        .master("local[*]")
        .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        //Loading CSV file and creating DataFrame
        val csvFile =spark.read.format("csv")
        .option("mode", "FAILFAST")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("input/covid-data.csv")
        //csvFile.printSchema()
        csvFile.createOrReplaceTempView("temp_data")
        

        // make the connection
        try{


            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)  
            val statement = connection.createStatement() 


            // Welcome screen to the app
            println("")
            println("####################################")
            println("    COVID 19 DATA ANALYSIS          ")
            println("####################################")
            println("")
            
                    
            // Application loop
            while (covidProject){
                mainMenu()
                
                // Start of program
                def mainMenu(){
                    println("###################################")
                    println("Please choose from the menu below: ")
                    println("(1) User Log In ")
                    println("(0) Quit Program")
                    println("###################################")
                    var choice= (scanner.nextInt())
                    scanner.nextLine()                                

                    if(choice == 1){
                        userLogIn()
                    
                    }else if(choice == 0){
                        exitProgram()
                            
                    }else if(( choice != 0 || choice != 1 )){
                        println("Not a valid choice. Try again")
                        mainMenu()
                    }
                            
                }

                                
                            
                        //User logging in
                        def userLogIn(){
                            println(" Please type a User Name")
                            userName = scanner.nextLine().trim()
                        
                            //Username checkpoint
                            try {
                                if (userName == "" || userName.length < 2){
                                    throw new BadDataEntryException
                                }
                            }catch {
                                case bui: BadDataEntryException =>  println("Name must be at least a 2 character string. Try again.")
                                userLogIn()
                                    
                            }
                        
                            println("")

                            println(" Please type A Password")
                            userPassword = scanner.nextLine().trim()
                                    
                        
                            //Password checkpoint
                            try {
                                if (userPassword == "" || userPassword.length < 8){ 
                                    throw new BadDataEntryException
                                }
                            }catch{
                                case bui: BadDataEntryException =>  println("Password must be greater than 7 characters. Try again.")
                                userLogIn()
                            }
                            
                            val resultSet = statement.executeQuery("SELECT COUNT(*) FROM userAccount WHERE userName='"+userName+"' AND userPassword='"+userPassword+"';")
                            log.write("Executing 'SELECT COUNT(*) FROM userAccount WHERE userName='"+userName+"' AND userPassword='"+userPassword+"');\n")

                            while ( resultSet.next() ) {
                                if (resultSet.getString(1) == "1") {
                                    println("You Have Logged In Successfully")
                                    userMenu()
                                    
                                }else{
                                    println("Username/password combo not found. Try again!")
                                    userLogIn()
                                                        
                                }
                            }
                            
                                                        
                        } 
                                            
                                                
                        // User Menu
                        def userMenu(){
                            println(" What type of data would you like to view. Please select below: ")
                            println("")
                            println(" (1) Country with total death rate by continent.")
                            println("")
                            println(" (2) Covid Reported Deaths of Countries with life expectancy below Average")
                            println("")
                            println(" (3) Counties with the highest fully vaccinated people have lower total deaths")
                            println("")
                            println(" (4) Population density compared to total deaths")
                            println("")
                            println(" (5) Population density vs total deaths")
                            println("")
                            println(" (6) Avg total vaccinations per year")
                            println("")
                            println(" (7) How many people died who were fully vaccinated by country")
                            println("")
                            println(" (8) Total deaths compared to hospital beds in a given location")
                            println("")
                            println(" (9) Number of new cases per day")
                            println("")
                            println(" (10) View entire data set used during analysis")
                            println("")
                            println(" (0) To exit the program")
                            println("")
                            var choice2 =  (scanner.nextInt())
                            (scanner.nextLine()) 
                            if (choice2 == 1){
                                println("")
                                method1()
                                userMenu()

                                
                            }else if (choice2 == 2) {
                                println("")
                                method2()
                                userMenu()
                                

                            }else if (choice2 == 3) {
                                println("")
                                method3()
                                userMenu()
                                

                            }else if (choice2 == 4) {
                                println("")
                                method4()
                                userMenu()
                                

                            }else if (choice2 == 5) {
                                println("")
                                method5()
                                userMenu()


                            }else if (choice2 == 6) {
                                println("")
                                method6()
                                userMenu()                            

                            
                            }else if (choice2 == 7) {
                                println("")
                                method7()
                                userMenu()                            

                            
                            }else if (choice2 == 8) {
                                println("")
                                method8()
                                userMenu()                            

                            
                            }else if (choice2 == 9) {
                                println("")
                                method9()
                                userMenu()                            

                            
                            }else if (choice2 == 10) {
                                println("")
                                method10()
                                userMenu()
                            

                            }else if (choice2 == 0) {
                                exitProgram()
                                
                            }else if (( choice2 != 0 || choice2 != 1 || choice2 != 2 || choice2 != 3 || choice2 != 4 || choice2 != 5|| choice2 != 6 || choice2 != 7 || choice2 != 8 || choice2 != 9 || choice2 != 10)) {
                                println(" Not a valid choice, please try again!!!")
                                userMenu()
                                
                            }
                        }

                            
                                
                                                                    
                        // Exit program
                        def exitProgram(){
                            println("")
                            println("####################################")
                            println("    COVID 19 DATA ANALYSIS          ")
                            println("####################################")
                            println("")        
                            println("")
                            println("")
                            covidProject = false                               
                        }

                            
                        // Queries Analysis of Data Set
                        def method1(){
                                println("Country with total death rate by continent") 
                                val result = spark.sql(" WITH cte (SELECT continent,LOCATION   Country ,  sum(TOTAL_DEATHS)  Deaths  FROM temp_data group by continent,LOCATION) SELECT  * FROM cte   WHERE CONTINENT IS NOT NULL")
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/Total death rate by continent")                                     
                        }
                                
                        def method2(){
                                println("Covid Reported Deaths of Countries with life expectancy below Average")
                                val result = spark.sql("select location Country, sum(total_deaths) Total_Deaths, life_expectancy from temp_data group by location , life_expectancy  having life_expectancy  < Avg(life_expectancy) AND life_expectancy is not null")
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/reportedDeathsWithLifeExpectancy")
                        }

                        def method3():Unit={
                                println(" Counties with the highest fully vaccinated people have lower total deaths")
                                val queryMe="select location, NumberOfVaccination, NumberOfDeaths from(select location, max(total_vaccinations) NumberOfVaccination, max(total_deaths) NumberOfDeaths from temp_data where location is not null  group by location) as q1 where location not like \'%%income\' order by NumberOfVaccination desc"
                                val result = spark.sql(queryMe)
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/highestFullyVaccinatedLowerTotalDeaths")
                        }

                        def method4():Unit={
                                println(" Population density compared to total deaths")
                                val result = spark.sql("select max(population_density)  Population_Density, location as Country, max(total_deaths) TotalDeaths from temp_data where population_density is not null group by location order by population_density desc")
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/populationDensityComparedTotalDeaths")
                        } 

                        def method5():Unit={
                                println("Population density vs total deaths")
                                val result = spark.sql("select max(total_deaths) TotalDeaths, hospital_beds_per_thousand  Available_hospital_beds, location From (select distinct location, continent, hospital_beds_per_thousand, total_deaths from temp_data where location is not null and continent is not null)  q1 group by hospital_beds_per_thousand, location order by totaldeaths desc ")
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/populationDensityVsTotalDeaths")
                        }

                        def method6():Unit={
                                println("Avg total vaccinations per year")
                                val result = spark.sql("select location,  sum(cast(total_vaccinations as decimal(20,0))) as Total_Vaccination, trunc(date, 'year') as Year from temp_data where location is not null  AND date= '12/31/2020' OR date='12/31/2021' group by location,date")
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/avgTotalVaccinationsPerYear")
                        } 

                        def method7():Unit={
                                println(" How many people died who were fully vaccinated by country")
                                val result = spark.sql("select location, continent, max(total_deaths) TotalDeath, max( people_fully_vaccinated) FullyVaccinated from temp_data where continent is not null group by location,continent")
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/deathsPeopleFullyVaccinated")
                        }

                        def method8():Unit={
                                println("Total deaths compared to hospital beds in a given location")
                                val result = spark.sql("select median_age, aged_65_older, aged_70_older, max( people_fully_vaccinated)  FullyVaccinated from temp_data group by median_age, aged_65_older, aged_70_older")
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/totalDeathsComparedHospitalBeds")
                        }

                        def method9():Unit={
                                println("Number of new cases per day")
                                val result = spark.sql("select location, continent, new_cases , date from temp_data where continent is not null group by location,continent, new_cases, date ")
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/numberNewCasesPerDay")
                        }

                        def method10():Unit={
                                println("View entire data set used during analysis")
                                val result = spark.sql("select * from temp_data")
                                result.show(100)
                                Thread.sleep(100)
                                result.write.mode("overwrite").csv("results/EntireDataSet")
                        } 
                        
                    spark.stop()        
            
            }
                    
                                    
        }catch{
            
            case e: Exception => e.printStackTrace            

        }
            connection.close()


            log.close()    
        
        
    }
}

