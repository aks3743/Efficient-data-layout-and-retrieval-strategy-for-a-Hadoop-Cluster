# Importing required libraries
import pandas as pd
import numpy as np

# reading csv file using pandas and saving as a dataframe
df = pd.read_csv("Coursera-1.csv")

# Creating a column 'val' with all values=1
df["val"] = 1

# creating and adding row numbers to each individual row
row_num = []
for row in range(len(df)):
    row_num.append('Row_{}'.format(row+1))
df['Row_num'] = row_num

# Grouping the data to idenify the rows that are repeating
grouped = df.groupby(['Course Name', 'University', 'Difficulty Level', 'Course Rating',
       'Course URL', 'Course Description', 'Skills'])

# The column val will give us number of times each individual row is repeating. saving the dataframe to df1
df1 = grouped.sum()

# mapping 3-way records to 4-way
df1.loc[df1["val"] == 3, "val"] = 4

# Joining df with df1 so that we get the number of times a row is repeatig corresponding to each row and row number
df2 = df.merge(df1,on = ['Course Name', 'University', 'Difficulty Level', 'Course Rating','Course URL', 'Course Description', 'Skills'],how = 'outer')

# Creating a mapping file which hold the information of orw numbers and number of times the corresponding roe is repeating the data
df2[["Row_num","val_y"]].to_csv("mapping_file_row_to_group.csv")

# Writing the data to 'n_way_file.csv' after removing the duplicates
# here we append the row numbers where the rows where repeating in the column Row_num seperated by ','
for group_no in df2.val_y.unique():
    df3 = df2[df2['val_y']== group_no]
    df3 = df3.groupby(['Course Name', 'University', 'Difficulty Level', 'Course Rating', 'Course URL', 'Course Description', 'Skills'])['Row_num'].apply(','.join).reset_index()
    df3.to_csv(f'{group_no}_way_file.csv',sep = "|")
