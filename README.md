# Toy Kafka
An implementation of some parts of the the apache kafka protocol in python. Specifically the Describe Partitions API, and the Produce API. This code was written with specifications gotten primarily by auditing the codecrafters apache kafka challenge, and the apache kafka protocol available on the apache kafka website, and might not be a word for word implementation. Some aspects of the proctocol are skipped, when such decision is taken, it is documented in line in the code. For more information, visit [codecrafters kafka challenge](https://app.codecrafters.io/courses/kafka/).

# Features
- Serial and concurrent connections.
- Serial and concurrent read and writes.
- Descibe Partitions API.
- Produce API.
- API Versions API.

# Setup
- Clone the repository
``` bash
git clone repository-name && cd respository-name
```

- Create a virtual environment in the directory of the cloned repository
``` bash
python3 -m venv .venv
```

- Activate the virtual environment
``` bash
source .venv/bin/activate
```

- Install the requirements
``` bash
python3 install -r requirements.txt
```

- Run the program. Create a shell script, and set environment variables for CLUSTER_LOG_FILE and LOG_DIR, the CLUSTER_LOG_FILE points to the __cluster_metadata used by the program, and the LOG_DIR, points to the log directory for the python file.
``` bash
touch run_program.sh

vi run_program.sh

# you then add the following in the run_program.sh file, and then write and save it.
CLUSTER_LOG_PATH=/somedir LOG_DIR=/somedir python3 main.py

# when you exit the vi enviroment after saving the file, run the bash script
bash run_program.sh
```