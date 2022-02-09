import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import java.util.Scanner
import java.sql.DriverManager
import java.sql.Connection
import com.mysql.cj.xdevapi.UpdateStatement
import java.io.File
import java.io.PrintWriter





object Project_2 {
    def main(args: Array[String]): Unit = {
        
        // declaring all variables needed for program
        var scanner = new Scanner(System.in)
        val log = new PrintWriter(new File("covidData.log"))


        var userName = ""
        var userPassword = ""
        var covidProject = true





        // This block of code is need to connect to spark/hive/hadoop
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("Project_2")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._



        // make the connection to mySQL
        val driver = "com.mysql.jdbc.Driver"
        // Modify for whatever port you are running your DB on
        val url = "jdbc:mysql://localhost:3306/Project_1_Sharks"
        val username = "root"
        //? DON'T FORGET TO DELETE PASSWORD BEFORE PUSHING TO GITHUB
        val password = "#####################" // Update to include your password
        var connection:Connection = null 
        //val statement = connection.createStatement()




        try{
            // make the connection
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)  
            val statement = connection.createStatement() 


            // Welcome screen to the app
            println("")
            println("###############################")
            println("    COVID 19 ANALYSIS          ")
            println("###############################")
            println("")        
            println("")
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
                            if (userName == "" || userName.length < 3) 
                                throw new BadDataEntryException
                                }
                                catch 
                                {
                                    case bui: BadDataEntryException => { println("Name must be at leat a 3 character string. Try again.")
                                    userLogIn()
                                }
                        println("")

                        println(" Please type A Password")
                        userPassword = scanner.nextLine().trim()
                        try {
                            if (userPassword == "" || userPassword.length < 8) 
                                throw new BadDataEntryException
                                }
                                catch 
                                {
                                    case bui: BadDataEntryException => { println("Password must be greater than 7 characters. Try again.")
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
                        println(" (1) Location with the highest total deaths compared to the country with the lowest total deaths")
                        println("")
                        println(" (2) Do locations with the lowest total deaths have the highest life expectancy?")
                        println("")
                        println(" (3) Counties with the highest fully vaccinated people have lower total deaths?")
                        println("")
                        println(" (4) population density compared to total deaths?")
                        println("")
                        println(" (5) Population density vs total deaths?")
                        println("")
                        println(" (6) Avg total vaccinations per year?")
                        println("")
                        println(" (7) How many people died who were fully vaccinated by country ?")
                        println("")
                        println(" (8) Total deaths compared to hospital beds in a given location ?")
                        println("")
                        println(" (9) STILL NEED TO COME UP WITH?")
                        println("")
                        println(" (10) STILL NEED TO COME UP WITH")
                        println("")
                        println(" (0) To exit the program")
                        println("")
                        var choice2 =  (scanner.nextInt())
                        (scanner.nextLine()) 
                        if (choice2 == 1){
                            println(" Location with the highest total deaths compared to the country with the lowest total deaths?")
                            method1()
                            userMenu()

                            
                        }else if (choice2 == 2) {
                            println(" Do locations with the lowest total deaths have the highest life expectancy?")
                            method2()
                            userMenu()
                            
                        }else if (choice2 == 3) {
                            println(" Counties with the highest fully vaccinated people have lower total deaths?")
                            method3()
                            userMenu()
                            
                        }else if (choice2 == 4) {
                            println(" population density compared to total deaths?")
                            method4()
                            userMenu()
                            
                        }else if (choice2 == 5) {
                            println(" Population density vs total deaths?")
                            method5()
                            userMenu()
                            
                        }else if (choice2 == 6) {
                            println("  Avg total vaccinations per year?")
                            method6()
                            userMenu()
                        

                        
                        }else if (choice2 == 7) {
                            println(" How many people died who were fully vaccinated by country?")
                            method7()
                            userMenu()
                        

                        
                        }else if (choice2 == 8) {
                            println(" Total deaths compared to hospital beds in a given location?")
                            method8()
                            userMenu()
                        

                        
                        }else if (choice2 == 9) {
                            println(" STILL NEED TO COME UP WITH THIS?")
                            method9()
                            userMenu()
                        

                        
                        }else if (choice2 == 10) {
                            println(" STILL NEED TO COME UP WITH THIS?")
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
                        println("###############################")
                        println("    COVID 19 ANALYSIS          ")
                        println("###############################")
                        println("")        
                        println("")
                        println("")
                        covidProject = false
                        
                    }

                    // Creates Create Covid Data Database
                    def covidData(){
                        val output = hiveCtx.read
                            .format("csv")
                            .option("inferSchema", "true")
                            .option("header", "true")
                            .load("input/covid-data.csv")
                                    
                        //output.limit(20).show() // will print out the first 20 lines


                        // This code will create a temp view of the dataset you used and load the data into a permanent table
                        // inside of Hadoop. this will persist the data and only require this code to run once.
                        // After initialization this code will and creation of output will not me necessary
                        output.createOrReplaceTempView("temp_data")
                        hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
                        hiveCtx.sql("SET hive.enforce.bucketing=false")
                        hiveCtx.sql("SET hive.enforce.sorting=false")
                        //hiveCtx.sql("USE project1_hive_scala")
                        
                        
                        
                        
                    }

                    

                    // Query for total number of shark attacks since certain date
                    def method1(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }


                    // Query for total number of shark attacks since certain date
                    def method2(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }


                    // Query for total number of shark attacks since certain date
                    def method3(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }


                    // Query for total number of shark attacks since certain date
                    def method4(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }



                    // Query for total number of shark attacks since certain date
                    def method5(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }
                    
                    // Query for what shark is responsible for the most attacks
                    def method6(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }

                    // Query for location of most shark attacks
                    def method7(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }

                     
                    // STILL NEED TO FIGURE OUT THIS QUERY
                    // Query for what time of day do most shark attacks occur
                    def method8(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }
                    // cast(str_column as int)


                    // Query for the number of provoked and unprovoked shark attacks
                    def method9(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }

                    // Query for average age range of people attacked
                    def method10(){
                        println("Title of Query")
                        val result = hiveCtx.sql("")
                        result.show()
                        result.write.csv("results/")
                        log.write("")
                    }


                            
                }

                //? to overwrite csv file if already exist result.write.mode(SaveMode.Overwrite).csv("filename")
                //? take first 2 digits of time as int then use that to find time of day

            

            
                            
            }catch {
                     case e: Exception => e.printStackTrace            

            }
                connection.close()


            log.close()



    }
  
}
