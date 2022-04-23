version 1.0

task Hello {
    input {
        String testString
        String salutation
    }


    command {
        echo "~{salutation} ~{testString}!"
    }

    runtime {

    }

    output {
        String outString = read_string(stdout())
    }
}

workflow HelloExample {
    input {
        String inputString
        String salutation
    }

    call Hello{
        input:
            salutation=salutation,
            testString=inputString
    }
}
