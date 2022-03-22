# Importing required libraries
import pandas as pd
import subprocess

def run_cmd(args_list):
        """
        run linux commands
        """
        # import subprocess
        print('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err 

# Change directory to docker-hadoop where it is installed
(ret, out, err)= run_cmd(['cd', 'docker-hadoop'])

# download all the images defined and create the containers and run them
(ret, out, err)= run_cmd(['sudo', 'docker-compose', 'up', '-d'])

# to see the containers running for the nodes
(ret, out, err)= run_cmd(['sudo', 'docker', 'container', 'ls'])

# to go inside docker's interactive terminal mode
(ret, out, err)= run_cmd(['sudo', 'docker', 'exec', '-it', 'namenode', '/bin/bash'])

# list the directories in HDFS under “/”
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-ls', '/'])

# create a working folder inside the namenode
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '-p', '/user/root'])

(ret, out, err)= run_cmd(['hdfs', 'dfs', '-ls', '/'])

# to get back to docker-hadoop path
(ret, out, err)= run_cmd(['exit'])

# to check if the containers are running for the nodes
(ret, out, err)= run_cmd(['sudo', 'docker', 'container', 'ls'])

# copy the csv files into the docker container
(ret, out, err)= run_cmd(['sudo', 'docker', 'cp', '1_way_file.csv', 'namenode:/tmp/'])
(ret, out, err)= run_cmd(['sudo', 'docker', 'cp', '2_way_file.csv', 'namenode:/tmp/'])
(ret, out, err)= run_cmd(['sudo', 'docker', 'cp', '4_way_file.csv', 'namenode:/tmp/'])
(ret, out, err)= run_cmd(['sudo', 'docker', 'cp', '8_way_file.csv', 'namenode:/tmp/'])


(ret, out, err)= run_cmd(['sudo', 'docker', 'exec', '-it', 'namenode', '/bin/bash'])

# changes directory to "/tmp/"
(ret, out, err)= run_cmd(['cd', '/tmp/'])

# list the files in the docker container
(ret, out, err)= run_cmd(['ls'])
(ret, out, err)= run_cmd(['hdfs', 'dfs', 'ls'])

# create a directory "/input"
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-mkdir', '/user/root/input'])

# to store the csv files in HDFS path
print("Running command : hdfs dfs -put 1_way_file.csv /user/root/input/")
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-put', '1_way_file.csv', '/user/root/input/'])
print("Running command : hdfs dfs -put 2_way_file.csv /user/root/input/")
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-put', '2_way_file.csv', '/user/root/input/'])
print("Running command : hdfs dfs -put 4_way_file.csv /user/root/input/")
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-put', '4_way_file.csv', '/user/root/input/'])
print("Running command : hdfs dfs -put 8_way_file.csv /user/root/input/")
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-put', '8_way_file.csv', '/user/root/input/'])

(ret, out, err)= run_cmd(['hdfs', 'dfs', '-put', 'mapping_file_row_to_group.csv', '/user/root/input/'])

# set replication factor for each of the csv files
print("Running command : hdfs dfs -setrep -w -R 1 /user/root/input/1_way_file.csv")
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-setrep', '-w', '-R', '1', '/user/root/input/1_way_file.csv'])
print("Running command : hdfs dfs -setrep -w -R 2 /user/root/input/2_way_file.csv")
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-setrep', '-w', '-R', '2', '/user/root/input/2_way_file.csv'])
print("Running command : hdfs dfs -setrep -w -R 4 /user/root/input/4_way_file.csv")
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-setrep', '-w', '-R', '4', '/user/root/input/4_way_file.csv'])
print("Running command : hdfs dfs -setrep -w -R 8 /user/root/input/8_way_file.csv")
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-setrep', '-w', '-R', '8', '/user/root/input/8_way_file.csv'])

# get the files to local path "/Documents"
# NOTE: This python file should be present in /Documents path
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-get', '/user/root/input/1_way_file.csv', '/Documents'])
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-get', '/user/root/input/2_way_file.csv', '/Documents'])
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-get', '/user/root/input/4_way_file.csv', '/Documents'])
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-get', '/user/root/input/8_way_file.csv', '/Documents'])

(ret, out, err)= run_cmd(['hdfs', 'dfs', '-get', '/user/root/input/mapping_file_row_to_group.csv', '/Documents'])

(ret, out, err)= run_cmd(['exit'])

# stop the containers
(ret, out, err)= run_cmd(['sudo', 'docker-compose', 'down'])

(ret, out, err)= run_cmd(['cd', 'Documents'])

# Importing required libraries
import pandas as pd

# Defining a new function
def tidy_split(df, column, sep = ',', keep = False):
    """
    Split the values of a column and expand so the new DataFrame has one split
    value per row. Filters rows where the column is missing.

    Params
    ------
    df : pandas.DataFrame
        dataframe with the column to split and expand
    column : str
        the column to split and expand
    sep : str
        the string used to split the column's values
    keep : bool
        whether to retain the presplit value as it's own row

    Returns
    -------
    pandas.DataFrame
        Returns a dataframe with the same columns as `df`.
    """
    indexes = list()
    new_values = list()
    df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            new_values.append(value)
    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values
    return new_df

# Laoding the mapping file
df5 = pd.read_csv("mapping_file_row_to_group.csv")

# Input option for the user to enter the row number
# and creating a logic to retrieve the row from the data
row_num = input("Please enter the row number between 1 and 3522: ")
Row_to_retrieve = "Row_" + row_num 

val = str(df5[df5['Row_num']== Row_to_retrieve]['val_y'].values[0])

# identifying the 'n_way_file.csv' where the row belongs
wayfile = f'{val[0]}_way_file.csv'

df6 = pd.read_csv(wayfile,sep = "|")

# deflating the file so that the required row can be filtered out
df6 = tidy_split(df6,"Row_num")

# filtering the data to identify the row
df7 = df6[df6["Row_num"]== Row_to_retrieve]

# Display the data in the row
print(df7)