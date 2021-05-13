####################################################################################################################
#
# File:        Scripting and Semi-Structured Data.py
# Description: A script that deals with Semi-Structured Data (JSON files),
#              performs simple ETL, reads JSON files from the source directory,
#              transforms it, and then loads it as CSV into a target directory.
#              The script takes 2 positional arguments (src_dir, dest_dir), and 1 optional argument (-u/--unix_time)
#              to maintain the UNIX format of timestamp, if not given you will get the normal time stamp.
# Author:      Mostafa Mamdouh
# Created:     Thu Apr 22 12:15:18 PDT 2021
#
####################################################################################################################

# import
import os
import sys
import argparse
from subprocess import PIPE, Popen
import time, json
import pandas as pd


def get_unique_json(src_dir):
    '''
    function to get unique and delete duplicates json files in a directory
    Parameters
    ----------
    src_dir : str
        DESCRIPTION. source directory that contain json file

    Returns
    -------
    unique_files : list
        DESCRIPTION. list of unique json files name
    '''
    # list all files in the directory
    try:
        files = [item for item in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, item))]
    except FileNotFoundError:
        print(f"the source directory {src_dir} is not found")
        # send local mail indicating the error
        os.popen(fr'echo the source directory "{src_dir}" is not found | mail -s "Error (source directory is not found)" $(whoami)')
        sys.exit() # terminate the program
    except PermissionError:
        print(f"Permission denied the source directory {src_dir} can not be accessed")
        # send local mail indicating the error
        os.popen(fr'echo Permission denied the source directory "{src_dir}" can not be accessed | mail -s "Error (source directory can not be accessed)" $(whoami)')
        sys.exit() # terminate the program
        
    # empty dict for checksum and file in a key and value format.
    checksums = {}
    # empty list for the duplicated checksums, and unique
    duplicates = []
    unique_files = []
    
    for filename in files:
        # only deal with .json files
        if(os.path.splitext(filename)[1] == ".json"):
            # Use Popen to call the sha256sum utility
            proc = Popen(['sha256sum', src_dir + '/' + filename], stdout=PIPE, stderr=PIPE)
            res = proc.stdout.read().split()
            if not res: # if list is empty >> permission denied on the file
                denied_file = src_dir + '/' + filename
                print(f"Permission denied the source file {denied_file} can not be accessed")
                # send local mail indicating the error
                os.popen(fr'echo Permission denied the source file "{denied_file}" can not be accessed | mail -s "Error (source file can not be accessed)" $(whoami)')
                #sys.exit() # terminate the program
            else :
                checksum = res[0] # value of hash
                # Append duplicate to a list if the checksum is found
                if checksum in checksums:
                    duplicates.append(filename)
                    os.remove(src_dir + '/' + filename) # delete duplicate json file 
                # save value of hash, incase needed later
                else:
                    unique_files.append(filename)
                    checksums[checksum] = filename
    print(f"Found Duplicates: {duplicates} and they are deleted")
    return(unique_files)



def etl(src_dir, dest_dir, filename, unix_time):
    '''
    function to perform ETL on json file, and load as csv file
    Parameters
    ----------
    src_dir : str
        DESCRIPTION. source directory that contain json file
    filename : str
        DESCRIPTION. name of the json file
    unix_time : bool
        DESCRIPTION. unix time or normal time, True >> unix time
    dest_dir : str, optional
        DESCRIPTION. destination directory that will contain csv file The default is './target'.

    Returns
    -------
    None.

    '''
    # change extension for target file
    nfilename = os.path.splitext(filename)[0] + ".csv"
    src_file_path = src_dir + '/' + filename
    dest_file_path = dest_dir + '/' + nfilename
    # read jsondir
    # >> extract
    try:
        records = [json.loads(line) for line in open(src_file_path)]
    except FileNotFoundError:
        print(f"the source directory {src_dir} is not found")
        # send local mail indicating the error
        os.popen(fr'echo the source directory "{src_dir}" is not found | mail -s "Error (source directory is not found)" $(whoami)')
        sys.exit() # terminate the program
    except PermissionError:
        print(f"Permission denied the source file {src_file_path} can not be accessed")
        # send local mail indicating the error
        os.popen(fr'echo Permission denied the source file "{src_file_path}" can not be accessed | mail -s "Error (source file can not be accessed)" $(whoami)')
        sys.exit() # terminate the program
        
    # >> transform
    # convert json to dataframe
    df = pd.json_normalize(records)
    
    # cleaning, and transormation
    # note: there are lots of mutation
    df['web_browser'] = df['a'].str.split(' ', None, expand=True)[0]
    df['operating_sys'] = df['a'].str.split(' ', None, expand=True)[1].str.extract(r'([-\w.]+)', expand=True)
    df['from_url'] = df['r'].str.split(r'/', None, expand=True)[2].fillna('direct')
    df['to_url'] = df['u'].str.split(r'/', None, expand=True)[2].fillna('direct')
    df[['longitude', 'latitude']] = df['ll'].apply(pd.Series)
    df.rename(columns={'cy': 'city', 'tz': 'time_zone', 't': 'time_in', 'hc': 'time_out'}, inplace=True)
    df = df[['web_browser', 'operating_sys', 'from_url', 'to_url', 'city', 'longitude', 'latitude', 'time_zone', 'time_in', 'time_out']]
    df.dropna(axis = 0, inplace=True) # drop na
    
    # convert to timestamp if according to the optional argument -u
    if not unix_time:
        df['time_in'] = pd.to_datetime(df['time_in'],unit='s')
        df['time_out'] = pd.to_datetime(df['time_out'],unit='s')
    
    # >> load
    try:
        df.to_csv(dest_file_path, index=False)  
    except FileNotFoundError:
        print(f"the target directory {dest_dir} is not found")
        # send local mail indicating the error
        os.popen(fr'echo the target directory "{dest_dir}" is not found | mail -s "Error (target directory is not found)" $(whoami)')
        sys.exit() # terminate the program
    except PermissionError:
        print(f"Permission denied the target directory {dest_dir} can not be accessed")
        # send local mail indicating the error
        os.popen(fr'echo Permission denied the target directory "{dest_dir}" can not be accessed | mail -s "Error (target directory can not be accessed)" $(whoami)')
        sys.exit() # terminate the program
     
    # mark json file as completed, only if they loaded successfully as csv
    os.rename(src_file_path, src_file_path + '.completed')  
    #Print a message after converting each file with the number of rows transformed and the path of this file
    rows_transformed = len(df.index)
    print(f"file : {src_file_path} is converted with number of rows transformed = {rows_transformed}")


def main():
    # get starting time
    tic = time.time()
    # create a parser instance using ArgumentParser()
    parser = argparse.ArgumentParser(description='A script that deal with Semi-Structured Data (json files), perform simple ETL, read json files from source directory, transform it, and then load it as csv into target directory. The script takes 2 positional arguments (src_dir, dest_dir), and 1 optional arguments (-u/--unix_time) to maintain the UNIX format of timpe stamp ')
    # add positional argument, i.e. it's mandatory to be passed for sourcce and target directory
    parser.add_argument("src_dir", help="Enter the source directory to read from it")
    parser.add_argument("dest_dir", help="Enter the target directory to write to it")
    # add a optional argument
    parser.add_argument("-u", "--unix_time", action="store_true", dest="unix_time", default=False, help="maintain the UNIX format of timpe stamp ")
    # parse all these args using parse_args() method
    args = parser.parse_args()
    # get unique json files
    unique_json = get_unique_json(args.src_dir)
    #print(unique_json)
    # perform etl for every json file
    for json_file in unique_json:
        etl(args.src_dir, args.dest_dir, json_file, args.unix_time)
    # get total excution time
    excution_time = time.time() - tic
    print(f"Total Excutoion time of the script = {excution_time}")
    

if __name__ == "__main__":
    main()
