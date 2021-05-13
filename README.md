# Scripting_and_Semi-Structured_Data

- A script that deals with Semi-Structured Data (JSON files), performs simple ETL, reads JSON files from the source directory, transforms it, and then loads it as CSV into a target directory.
- The script takes 2 positional arguments (src_dir, dest_dir), and 1 optional argument (-u/--unix_time) to maintain the UNIX format of timestamp, if not given you will get the normal time stamp.
- The script takes only the unique files from the source directory, and delete duplicate files.
- The script can be automated using Crontab, Airflow, or any scheduling tool to handle stream from spooling directory (the source directory).
- The script is applied to USA.gov dataset from Bitly but you can use the concept to apply it to any dataset.

## Installation

Check requirements.txt

## Usage

```bash
python Scripting_and_Semi-Structured_Data.py './source' './target'
python Scripting_and_Semi-Structured_Data.py './source' './target' -u
python Scripting_and_Semi-Structured_Data.py './source' './target' --unix_time
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

