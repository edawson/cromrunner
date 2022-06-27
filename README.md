CromRunner
--------------
Eric T. Dawson, NVIDIA Corporation  
June 2022

# Introduction
[CromRunner](https://github.com/edawson/cromrunner.git) is a python package for automating
the instantiation of the Cromwell workflow engine. CromRunner automatically generates WDL inputs
based on CSV/TSV manifests and templated JSON files and then
generates either an SBATCH/swarm launch file or runs multiple instances of Cromwell
inside a thread pool. The main reason to use Cromrunner is that it provides a very simple
way of scaling Cromwell using declarative inputs, without the need to stand up a server or
rely on Docker Compose or other tools.

# Setup
The only prerequisite for CromRunner is a local Cromwell JAR file.

# Inputs
CromRunner always takes the following three inputs:  
1. A WDL workflow file.
2. A JSON format Input Template for a workflow's inputs.
3. A CSV or TSV manifest, where each line represents a single instantiation of a workflow. Tags
in the input template will be replaced with values from the manifest, based on matching the column names
and tags.

The best way to explain this is with a small example. Imagine we have the following WDL workflow:  

```
## example.wdl
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
```

## The Template JSON file
This workflow takes two inputs. We can create an example input file using womtool:  

```bash
java -jar womtool-78.jar inputs example.wdl
```

This will produce the following text:  

```json
{
  "HelloExample.salutation": "String",
  "HelloExample.inputString": "String"
}
```

To turn this basic JSON stub into a template, we need to add Tags. A Tag is a special signifier for a variable
that CromRunner is to replace from a manifest. Tags are wrapped in angle bracked to ensure they are properly replaced.

Let's edit out JSON stub to include our desired tags and save it as a file named `example_1.template.json`:

```
{
  "HelloExample.salutation": "<salutation>",
  "HelloExample.inputString": "<Name>"
}
## Save this stub to a file named example_1.template.json
```

*Note the brackets around the "salutation" and "Name" tags.*


## The manifest file
Now, we can create a manifest file. A manifest is a TSV or CSV file
that contains variables that correspond to our Tags in its columns. Each row
corresponds to an independent instantiation of Cromwell. For example, if we wanted
to run three instances of our `example.wdl` workflow, we could write the following CSV file:

```
salutation,Name
Greetings,Earthling
Howdy,Partner
Good Day,Fellow Human
```

We now have all the inputs we need to run CromRunner. In the next section, we'll describe how inputs and run files are generated and how
to run our workflow on our various inputs.

# Running CromRunner

Users interface with CromRunner through a python command line interface (CLI). In this section we'll describe this interface and how to run a set of jobs based on CromRunner inputs.

CromRunner performs the following steps when called:  
  1. Load the input template and the paths to Cromwell and the WDL file. Optionally load the config file if passed.
  2. Create a directory with the prefix "CROMRUNNER-INPUTS-" followed by two random strings of characters. This file will contain the inputs for each job.
  3. Load the manifest file. For each line, substitute the tags in the template with the relevant variables from the manifest. Write the substitute input JSON stub to a file in the inputs directory with a sixteen-character random string.
  4. If the backend argument is passed, write a submission script for the input files. For example, requesting the swarm backend will create a swarm_tasks.txt file and a swarm_submit.sh script. **If no backend is passed, the CromRunner python script will start calling Cromwell on the input files.**

## Basic Examples

To download Cromwell:

```bash
wget https://github.com/broadinstitute/cromwell/releases/download/80/cromwell-80.jar
```

To access the help text:

```bash
python cromrunner.py 
```

```bash
usage: cromrunner.py [-h] [-C CROMWELL_PATH] -t TEMPLATE [--config CONFIG] -w WDL -i MANIFEST [-d DELIMITER] [-n THREADS] [-B BACKEND]
cromrunner.py: error: the following arguments are required: -t/--input-template, -w/--wdl, -i/--input-manifest
```

Create a swarm job file and submit script for an example template, WDL, and CSV input file:

```bash
python cromrunner.py -C ./cromwell-80.jar -t example.template.json -w example.wdl -i example.manifest.csv -d "," -B swarm
```

Run the same command as above, but explicitly pass a custom Cromwell configuration and a path
to a specific Cromwell JAR file. If no JAR file is explicitly passed, the current behavior is to simply fail.

```bash
python cromrunner.py -C /home/eric/cromwells/cromwell-76.jar -t example.template.json -w example.wdl -i example.manifest.csv -d "," -B swarm --config custom.config.hocon
```

