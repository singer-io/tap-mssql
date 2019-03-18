#!/usr/bin/env python

# For https://stitchdata.atlassian.net/browse/SRCE-515

# password123!

##########################################################################
### Installation log
###
### https://www.microsoft.com/en-us/sql-server/developer-get-started/python/ubuntu/
##########################################################################

# root@taps-tvisher1:~# /opt/mssql/bin/mssql-conf setup
# Choose an edition of SQL Server:
#   1) Evaluation (free, no production use rights, 180-day limit)
#   2) Developer (free, no production use rights)
#   3) Express (free)
#   4) Web (PAID)
#   5) Standard (PAID)
#   6) Enterprise (PAID)
#   7) Enterprise Core (PAID)
#   8) I bought a license through a retail sales channel and have a product key to enter.

# Details about editions can be found at
# https://go.microsoft.com/fwlink/?LinkId=852748&clcid=0x409

# Use of PAID editions of this software requires separate licensing through a
# Microsoft Volume Licensing program.
# By choosing a PAID edition, you are verifying that you have the appropriate
# number of licenses in place to install and run this software.

# Enter your edition(1-8): 2
# The license terms for this product can be found in
# /usr/share/doc/mssql-server or downloaded from:
# https://go.microsoft.com/fwlink/?LinkId=855862&clcid=0x409

# The privacy statement can be viewed at:
# https://go.microsoft.com/fwlink/?LinkId=853010&clcid=0x409

# Do you accept the license terms? [Yes/No]:Yes

# Enter the SQL Server system administrator password:
# The specified password does not meet SQL Server password policy requirements because it is not complex enough. The password must be at least 8 characters long and contain characters from three of the following four sets: uppercase letters, lowercase letters, numbers, and symbols.
# Enter the SQL Server system administrator password:
# The specified password does not meet SQL Server password policy requirements because it is not complex enough. The password must be at least 8 characters long and contain characters from three of the following four sets: uppercase letters, lowercase letters, numbers, and symbols.
# Enter the SQL Server system administrator password:
# Confirm the SQL Server system administrator password:
# Configuring SQL Server...

# ForceFlush is enabled for this instance.
# ForceFlush feature is enabled for log durability.
# Created symlink from /etc/systemd/system/multi-user.target.wants/mssql-server.service to /lib/systemd/system/mssql-server.service.
# Setup has completed successfully. SQL Server is now starting.

# root@taps-tvisher1:~# sqlcmd -S localhost -U sa -P 'password123!'
# 1> select @@version
# 2> go
                                                                                                                                                                                                                                                                                                            
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Microsoft SQL Server 2017 (RTM-CU13) (KB4466404) - 14.0.3048.4 (X64) 
#         Nov 30 2018 12:57:58 
#         Copyright (C) 2017 Microsoft Corporation
#         Developer Edition (64-bit) on Linux (Ubuntu 16.04.4 LTS)                                                                                                            

# (1 rows affected)

import pyodbc
server = 'localhost'
database = 'SampleDB'
username = 'sa'
password = 'password123!'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

print ('Inserting a new row into table')
#Insert Query
tsql = "INSERT INTO Employees (Name, Location) VALUES (?,?);"
with cursor.execute(tsql,'Jake','United States'):
    print ('Successfuly Inserted!')


#Update Query
print ('Updating Location for Nikita')
tsql = "UPDATE Employees SET Location = ? WHERE Name = ?"
with cursor.execute(tsql,'Sweden','Nikita'):
    print ('Successfuly Updated!')


#Delete Query
print ('Deleting user Jared')
tsql = "DELETE FROM Employees WHERE Name = ?"
with cursor.execute(tsql,'Jared'):
    print ('Successfuly Deleted!')


#Select Query
print ('Reading data from table')
tsql = "SELECT Name, Location FROM Employees;"
with cursor.execute(tsql):
    row = cursor.fetchone()
    while row:
        print (str(row[0]) + " " + str(row[1]))
        row = cursor.fetchone()
