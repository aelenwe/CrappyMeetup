from __future__ import division
import csv
import re
import os

# Get current working directory
cwd = os.getcwd()
# Path of the freshly downloaded file - assumes it's in the current directory
orig_path = cwd + '/Case_Data_from_San_Francisco_311__SF311_.csv'
# Path of the file we're creating
new_path = cwd + '/new_311.csv'

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
