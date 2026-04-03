# TPCH Query 5 C++ Multithreading Project

## Overview
Task is to implement TPCH Query 5 using C++ and multithreading. 
1. Understand the query 5 given below.
2. Generate input data for the query using [TPCH Data Generation Tool](https://github.com/electrum/tpch-dbgen)
3. Clone the current repository to your personal github.
4. Complete the incomplete functions in query5.cpp file
5. Compile and run the program. Capture the result with single thread and 4 thread.

## Building the Project
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd tpch-query5-cpp
   ```

2. Create a build directory and navigate into it:
   ```bash
   mkdir build
   cd build
   ```

3. Generate the build files with CMake:
   ```bash
   cmake ..
   ```

4. Compile the project:
   ```bash
   make
   ```

## Running the Program
### Single-Threaded Execution
To run the program in single-threaded mode, use the following command:
```bash
./tpch_query5 --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 1 --table_path /path/to/tables --result_path /path/to/results
```

### Multi-Threaded Execution
To run the program in multi-threaded mode, specify the number of threads (e.g., 4):
```bash
./tpch_query5 --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
```
