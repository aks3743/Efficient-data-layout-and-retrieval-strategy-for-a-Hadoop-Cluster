This is a Readme file.


Folder Structure:
  Grp_189:  
	|->Readme.txt
	|->Grp_189_document
	|->Code files:
                  |->Generate_files.py
                  |->Final_script.py
                  |->1_way_file.csv
                  |->2_way_file.csv
                  |->4_way_file.csv
                  |->8_way_file.csv
                  |->mapping_file_row_to_group.csv
                  |->Coursera-1.csv


Please follow this file for successful working of the commands.


1. Place the Generate_files.py in the directory where docker-hadoop is installed (here it is in /docker-hadoop)
2. Place the Final_script.py file in /Documents path.
3. Open a terminal and change the directory to docker-hadoop.
4. Run the python file using the command: “python Generate_files.py” (it generates all the n_way_files)
5. Change the directory to /Documents
6. Run the python file using the command: “python Final_script.py” (it places the csv files into HDFS and retrieves the record when user enters a row id)