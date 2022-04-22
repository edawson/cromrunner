import argparse
import os
import random
import string
from collections import defaultdict
import multiprocessing as mp

INPUT_TAG = "INPUT_TAG"
CROMWELL_TAG = "CROMWELL_TAG"
CONFIG_TAG = "CONFIG_TAG"
WDL_TAG = "WDL_TAG"

def get_verified_absolute_path(path: str) -> str:
    """Verify and return absolute path of argument.
    Args:
        path : Relative/absolute path
    Returns:
        Absolute path
    """
    installed_path = os.path.abspath(path)
    if not os.path.exists(installed_path):
        raise RuntimeError("The requested path does not exist:{}".format(installed_path))
    return installed_path

def get_random_string(length: int) -> str:
    """
    Return a string of <length> random uppercase letters
    """
    return "".join(random.choice(string.ascii_uppercase) for i in range(0, length))

def write_to_tmp_file(filename: str, s: str) -> None:
    with open(filename, "w") as ofi:
        ofi.write(s)
    return

class WorkResult:
    def __init__(self):
        self.cromwell_log: str = None
        self.std_err_files: list = None
        self.std_out_files: list = None
        self.return_code: int = -1
        self.run_output_path: str = None
        self.inputs_json: str = "{}"
        self.outputs_json: str = "{}"

class WorkInstance:
    def __init__(self):
        self.cromwell_config_path: str = None
        self.cromwell_path: str = None
        self.wdl_path: str = None
    
        self.inputs_directory_path: str = None
        self.input_template_map: dict
        self.input_template_json: str = None ## JSON string
        self.input_json: str = "{}" ## JSON string
        self.rand_id: str = None
        ## A string of the form "java CONFIG_TAG -jar CROMWELL_TAG run -i INPUT_TAG WDL_TAG"
        ## should be filled by the CromRunner instance
        self.cromwell_invocation_template: str = None

    def set_rand_id(self):
        self.rand_id = get_random_string(16)
    
    def create_inputs(self):
        """
        Replace the placeholder tags in input_json with values from the input_template_map dictionary
        and write the resulting JSON to a temporary file with a unique name.
        """
        if self.rand_id is None:
            self.set_rand_id()
        self.input_json = self.input_template_json
        for i in self.input_template_map:
            self.input_json.replace(i, self.input_template_map[i])

        return 

    def create_run_string(self):
        """
        Create a basic cromwell CLI invocation that will be passed to each WorkInstance.
        Creates the paths to cromwell, config, a tag for the input file, and a tag for the WDL
        """
        if self.cromwell_invocation_template is None:
            print("Error: cromwell base template must not be empty")
            exit(9)
        self.cromwell_invocation_template.replace("CONFIG_TAG", self.cromwell_config_path)
        self.cromwell_invocation_template.replace("CROMWELL_TAG", self.cromwell_path)
        self.cromwell_invocation_template.replace("WDL_TAG", self.wdl_path)
        return self.cromwell_invocation_template
    
    def run(self) -> WorkResult:
        return
    def output(self):
        return

class CromRunner:
    """
    A server class for running multiple instances of cromwell.
    Manages a list of WorkInstances, each of which is an instantiation of Cromwell
    running a single WDL on a single input, taken from the lines of the input manifest file.
    """
    def __init__(self):
        self.available_backends = ["local", "slurm", "swarm"]
        self.server_max_runtime_minutes: int = 24 * 60
        self.slurm_base_command: str = None
        self.swarm_base_command: str = None
        self.work_instances: list
        self.max_simultaneous_instances: int = None
        self.results: list
        self.backend: str = "local"
        self.nthreads: int = 4

        self.input_manifest_path: str
        self.input_manifest_delim: str = ","
        self.input_template_path: str = None
        self.input_template: str = None
        self.wdl_path: str = None

        self.cromwell_path: str = None
        self.cromwell_config_path: str = None
        self.cromwell_invocation_template: str = "java CONFIG_TAG -jar CROMWELL_TAG run -i " + INPUT_TAG + " WDL_TAG"
        self.cromwell_invocation: str = ""

        self.input_tmp_dir_path: str = None

        self.input_manifest_header: list = None

        self.max_input_lines = 64000

    def init(self, args):
        self.cromwell_path = args.cromwell_path
        self.input_manifest_path = args.manifest
        self.nthreads = args.threads
        self.wdl_path = args.wdl
        self.input_template_path = args.template
        self.cromwell_config_path = "" if args.config is None else "-Dconfig.file" + get_verified_absolute_path(args.config)

    def get_config_target(self):
        if self.cromwell_config_path is not None:
            return "-Dconfig.file=" + self.cromwell_config_path
        return ""

    def create_tmp_dir(self):
        rand_one = get_random_string(8)
        rand_two = get_random_string(8)
        rand_three = get_random_string(8)
        
        self.input_tmp_dir_path = "-".join([rand_one, rand_two, rand_three])
        os.mkdir(self.input_tmp_dir_path)
        return self.input_tmp_dir_path
        
    def create_cromwell_base_instantiation(self):
        """
        Create a basic cromwell CLI invocation that will be passed to each WorkInstance.
        Creates the paths to cromwell, config, a tag for the input file, and a tag for the WDL
        """
        self.cromwell_invocation = self.cromwell_invocation_template
        self.cromwell_invocation = self.cromwell_invocation.replace("CONFIG_TAG", self.cromwell_config_path)
        self.cromwell_invocation = self.cromwell_invocation.replace("CROMWELL_TAG", self.cromwell_path)
        self.cromwell_invocation = self.cromwell_invocation.replace("WDL_TAG", self.wdl_path)
        return self.cromwell_invocation
    
    def load_inputs_template(self):
        with open(self.input_template_path, "r") as ifi:
            self.input_template = ifi.read()
        return self.input_template

    def create_work_instances(self):
        
        ## Create the base cromwell CLI invocation
        crom_base = self.create_cromwell_base_instantiation()


        ## Set up the input template header
        ## and iterate over each line in the input manifest,
        ## creating a new work instance for each line
        input_lines_read = 0
        with open(self.input_manifest_path, "r") as manifest:
            for line in manifest:
                line = line.strip()
                splits = line.split(self.input_manifest_delim)
                if input_lines_read == 0:
                    self.input_manifest_header = splits
                else:
                    work = WorkInstance()
                    work.inputs_directory_path = self.input_tmp_dir_path
                    work.cromwell_path = self.cromwell_path
                    work.wdl_path = self.wdl_path

                    work.input_template_json = self.load_inputs_template()

                    work.cromwell_config_path = self.get_config_target()

                    for i in range(0, len(self.header)):
                         work.input_template_map[self.header[i]] = splits[i]
                input_lines_read += 1
                if input_lines_read > self.max_input_lines:
                    print("ERROR: number of inputs exceeds maximum allowable number (" + self.max_input_lines + ").")
                    exit(9)

    def run_slurm(self):
        """
        Run a CromRunner server that submits one SLURM job
        per WorkInstance.
        """
        return
    def run_local(self):
        pool = mp.Pool(self.nthreads)
        return
    def run_swarm(self):
        """
        Run a CromRunner server that launches a SWARM job to handle
        all WorkInstances.
        """
        return
    def run_gcp(self):
        """
        Launch a CromRunner server that launches jobs on Google Cloud.
        """
        return

    def __uptime(self) -> int:
        """
        Get the current uptime in minutes
        """
        return


def run_cromwell(work_instances: list) -> list:
    """
    Given a list of WorkInstances, run cromwell, once per instance.
    Return a list of WorkResults, one per instance
    """
    return

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-C", "--cromwell-path", type=str, dest="cromwell_path", default="cromwell.jar", required=False)
    parser.add_argument("-t", "--input-template", dest="template", help="Template file for WDL inputs.", required=True, type=str)
    parser.add_argument("--config", dest="config", help="An optional Cromwell config file.", required=False, type=str, default=None)
    parser.add_argument("-w", "--wdl", dest="wdl", help="A WDL script that defines a workflow.", required=True, type=str)
    parser.add_argument("-i", "--input-manifest", dest="manifest", help="A file containing inputs per instantiation, one per line, delimiter-separated, with a header that corresponds to templated input tags.", required=True, type=str)
    parser.add_argument("-d", "--delimiter", help="The delimiter used for input args in the input-file.", default=",", type=str, required=False)
    parser.add_argument("-n", "--num-concurrent-cromwells",dest="threads", help="The number of cromwell instances to spawn at one time.", required=False, type=int, default=4)
    return parser.parse_args()

if __name__ == "__main__":

    args = get_args()
    runner = CromRunner()
    runner.init(args)
    runner.create_tmp_dir()
    invoc = runner.create_cromwell_base_instantiation()
    print(invoc)



