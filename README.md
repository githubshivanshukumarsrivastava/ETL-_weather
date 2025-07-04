--- this is a live  data which we capture that's why it is showing same value  in every record(row)
![Screenshot 2025-07-05 004321](https://github.com/user-attachments/assets/fe4b2852-fa50-4467-b96b-da7ecf095795)




dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
example_astronauts: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works

Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.

include: This folder contains any additional files that you want to include as part of your project. It is empty by default.

packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.

requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.

plugins: Add custom or community plugins for your project to this file. It is empty by default.

airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.


-- this is airflow dag which shows tha our pipeline is  good to go . 
![Screenshot 2025-07-05 004408](https://github.com/user-attachments/assets/2a0710d8-577c-4a50-929c-009112194723)


 --after run  it into a dbeaver  and  analyzing it that it running correct or not .. i upload it  at amazon rds (AWS) 

![image](https://github.com/user-attachments/assets/a116aaab-148a-4ad7-b5bd-4af6344ffe0d)


