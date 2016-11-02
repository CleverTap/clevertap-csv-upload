## CleverTap python csv upload tool

### Usage
to upload user profiles from a csv:
- git clone the repo
- cd to the python directory
- run csvupload.py passing your CleverTap Account ID and Passcode and the absolute path to your csv file.  Add -d true to do a dry run.
-  e.g. ./csvupload.py -a WWW-YYY-ZZZZ -c AAA-BBB-CCCC -p ~/Desktop/profileSample.csv

```
optional arguments:
  -h, --help            show this help message and exit
  -a ID, --id ID        CleverTap Account ID
  -c PASSCODE, --passcode PASSCODE
                        CleverTap Account Passcode
  -p PATH, --path PATH  Absolute path to the csv file
  -t TYPE, --type TYPE  The type of data, either profile or event, defaults to
                        profile
  -d DRYRUN, --dryrun DRYRUN
                        Do a dry run, process records but do not upload
```

NOTE:  you must include one of Identity, Email, FBID or GPID in your data.

### TODO
- add support for Events
