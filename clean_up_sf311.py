from __future__ import division
import csv
import re
import os
import psycopg2
from psycopg2.extensions import AsIs


# Get current working directory
cwd = os.getcwd()
# Path of the freshly downloaded file - assumes it's in the current directory
orig_path = cwd + '/Case_Data_from_San_Francisco_311__SF311_.csv'
# Path of the file we're creating
new_path = cwd + '/new_311.csv'
# Remove old copy of file
os.remove(new_path)
# Regex pattern for finding the latitude and longitude in the csv
pattern = '(-?\d+\.?\d+)'

# Read in the original file
with open(orig_path, 'r') as csvinput:
    # open the new file
    with open(new_path, 'w') as csvoutput:
        # create writer object
        writer = csv.writer(csvoutput, lineterminator='\n')
        # create reader object
        reader = csv.reader(csvinput)
        # pull out the 1st line of the csv for the header
        headers = reader.next()
        # add headers for latitude and longitude columns
        headers.extend(['Lat', 'Lon'])
        # create a list of headers
        all_rows = [headers]
        # For each row of the original csv
        for row in reader:
            # If the row contains human waste and it has a latitude & longitude
            if "human" in row[9].lower() and "waste" in row[9].lower() and row[13]:
                # look for the lat & lon in the row
                a = re.findall(pattern, row[13])
                # add the lat and lon to the end of the current row list
                row.extend([a[0], a[1]])
                # add the current list to the list of rows
                all_rows.append(row)
        # write the file to the new file
        writer.writerows(all_rows)

# Postgres connection
pg_conn = psycopg2.connect("dbname='sf311' user='aelenwe' host='localhost'")
# Postgres Cursor
pg_cur = pg_conn.cursor()

try:
    # Create table for new data
    pg_create = ("CREATE TABLE newsf311 ("
                 "CaseID bigint NOT NULL,"
                 "Opened timestamp,"
                 "Closed timestamp,"
                 "Updated timestamp,"
                 "Status varchar(10),"
                 "status_notes varchar(5000),"
                 "Responsible_Agency varchar(80),"
                 "Category varchar(80),"
                 "Request_Type varchar(100),"
                 "Request_Details varchar(100),"
                 "Address varchar(255),"
                 "Supervisor_District bigint,"
                 "Neighborhood varchar(80),"
                 "Location Point,"
                 "Source varchar(80),"
                 "Media_URL varchar(255),"
                 "Lat double precision,"
                 "Lon double precision,"
                 "PRIMARY KEY (CaseID));")

    # execute table build
    pg_cur.execute(pg_create)
    pg_conn.commit()
except psycopg2.Error, e:
    print e
    pass

copy_csv = ("COPY newsf311 FROM %s DELIMITER ',' CSV HEADER;")

# populate table
with open (new_path,'r') as f:
    pg_cur.execute(copy_csv, (new_path,))
    pg_conn.commit()

try:
    drop_old = "DROP TABLE sf311backup;"
    pg_cur.execute(drop_old)
    pg_conn.commit()
except psycopg2.Error, e:
    print e
    pass

rename_old = "ALTER TABLE sf311 RENAME TO sf311backup;"
pg_cur.execute(rename_old)
pg_conn.commit()
rename_new = "ALTER TABLE newsf311 RENAME TO sf311;"
pg_cur.execute(rename_new)
pg_conn.commit()

