#!/usr/bin/python

from clevertap import CleverTap
import argparse
import csv
import sys
import ast

IDENTITY_FIELDS = ["identity", "FBID", "GPID", "objectId"]

MAX_BATCH_SIZE = 100
MAX_PROPS = 63

def process_raw_record(raw_record, type):
    identity = False
    
    record = {"type": type}

    record_data_dict = None

    if type == "profile":
        record['profileData'] = {}
        record_data_dict = record['profileData']

    if type == "event":
        record['evtData'] = {}
        record_data_dict = record['evtData']

    if record_data_dict is None:
        raise Exception("unknown record type %s" % type)
        return

    prop_count = 0
    for k,v in raw_record.iteritems():
        if k in IDENTITY_FIELDS:
            if not identity:
                identity = True
                record[k] = v 
            continue

        if (prop_count < MAX_PROPS):
            if "Phone" in k:  
                if k == "Phone" and not v.startswith("+"):
                    v = "+%s" % v
                record_data_dict[k] = v

            else:
                try:
                    record_data_dict[k] = ast.literal_eval(v)
                except Exception, e:
                    record_data_dict[k] = v
            prop_count += 1
        else:
            print "property count max of %s exceeded, skipping %s" % (MAX_PROPS,k)

    if not identity:
        print "no identity value found for %s" % raw_record
        record = None

    return record
    

def main(account_id, passcode, path, type, dryrun):
    clevertap = CleverTap(account_id, passcode)
    data = []

    print type
    if type not in ["event", "profile"]:
        raise Exception("unknown record type %s" % type)
        return

    if type == "event":
        raise NotImplementedError("Event handling not yet implemented")
        return

    f = open(path, 'rt')
    try:
        reader = csv.DictReader(f)
        for row in reader:
            record = process_raw_record(row, type)
            if record is not None:
                if dryrun:
                    print record
                data.append(record)
            else:
                print "unable to process row skipping: %s" % row

        count = len(data) 
        if count <= 0:
            print "no records to process"
            f.close()
            return

        if not dryrun:
            print "starting upload for %s records" % count

        processed = 0
        while processed < count:
            remaining = count - processed
            batch_size = MAX_BATCH_SIZE if remaining > MAX_BATCH_SIZE else remaining 
            batch = data[processed:processed+batch_size]
            if not dryrun: 
                res = clevertap.upload(batch)
                print res
            processed += batch_size

        print "processed %s records" % processed

    except Exception, e:
        print e

    finally:
	f.close()

if __name__ == "__main__":

    def str2bool(v):
        return v.lower() in ("yes", "true", "t", "1")

    parser = argparse.ArgumentParser(description='CleverTap CSV uploader')
    parser.register('type','bool',str2bool)
    parser.add_argument('-a','--id', help='CleverTap Account ID', required=True)
    parser.add_argument('-c','--passcode', help='CleverTap Account Passcode', required=True)
    parser.add_argument('-p','--path', help='Absolute path to the csv file', required=True)
    parser.add_argument('-t','--type', help='The type of data, either profile or event, defaults to profile', default="profile")
    parser.add_argument('-d','--dryrun', help='Do a dry run, process records but do not upload', default=False, type='bool')

    args = parser.parse_args()

    main(args.id, args.passcode, args.path, args.type, args.dryrun)
