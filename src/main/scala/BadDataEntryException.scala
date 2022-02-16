class BadDataEntryException(message:String) extends Exception(message) {

    def this(message:String, cause:Throwable){
        this(message)
        initCause(cause)
    }
    
    def this(){
        this ("Bad data entered! try again!")
    }
}