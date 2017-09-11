#!/usr/bin/python

from clevertap import CleverTap
import argparse
import csv
import sys
import ast
import json

IDENTITY_FIELDS = ["identity", "FBID", "GPID", "objectId"]

MAX_BATCH_SIZE = 100
MAX_PROPS = {"event":31, "profile":63}

def process_raw_record(raw_record, type, mapping=None):
    if mapping is None:
        mapping = {}

    identity = False
    ts = 0
    evtName = None
    
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

    max = MAX_PROPS[type]
    prop_count = 0
    for k,v in raw_record.iteritems():
        if v in [None, "", "null"]:
            continue

        try:
            _v = ast.literal_eval(v)
            if not isinstance(_v, complex):
                v = _v
        except Exception, e:
            pass

        if k in IDENTITY_FIELDS:
            if not identity:
                identity = True
                record[k] = v
            continue

        try:
            k = mapping.get(k, k)
        except Exception, e:
            print e
            pass

        if k == "ts":
            try:
                ts = int(float(v))
                record[k] = ts 
                continue
            except Exception, e:
                print "invalid %s timestamp %s" % (type,v)
                return None

        if type == "event" and k == "evtName":
            evtName = k
            record[k] = v 
            continue

        if prop_count > max:
            print "property count max of %s exceeded, skipping %s" % (max,k)
            continue

        if k == "Phone" and not v.startswith("+"):
            v = "+%s" % v

        if isinstance(v, dict) or isinstance(v, list):
            try:
                v =  json.dumps(v)
            except Exception, e:
                print "unable to convert %s to json string, skipping" % v
                continue

        record_data_dict[k] = v
        prop_count += 1

    if not identity:
        print "no identity value found for %s" % raw_record
        return None

    if type == "event":
        if evtName is None:
            print "no event name value found for %s" % raw_record
            return None

        if ts <= 0:
            print "no timestamp provided, will default to current time for %s" % raw_record

    return record
    

def main(account_id, passcode, region, path, mapping_path, type, dryrun):
    clevertap = CleverTap(account_id, passcode, region=region)
    data = []

    if type not in ["event", "profile"]:
        raise Exception("unknown record type %s" % type)
        return

    mapping = None
    if mapping_path is not None:
        try:
            with open(mapping_path) as mapping_file:
                mapping = json.load(mapping_file)
        except Exception, e:
            print e
            pass

    f = open(path, 'rt')
    try:
        reader = csv.DictReader(f)
        print "reading csv file %s" % path
        for row in reader:
            try:
                record = process_raw_record(row, type, mapping=mapping)
            except Exception, e:
                print e

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
        batch_num = 0
        batches = (count/MAX_BATCH_SIZE) + (1 if (count % MAX_BATCH_SIZE > 0) else 0) 

        while processed < count:
            remaining = count - processed
            batch_size = MAX_BATCH_SIZE if remaining > MAX_BATCH_SIZE else remaining 
            batch = data[processed:processed+batch_size]
            batch_num += 1

            if not dryrun: 
                print "uploading %s records for batch num %s of %s" % (len(batch), batch_num, batches)
                res = clevertap.upload(batch)
                print res
                print "batch num %s uploaded" % batch_num

            processed += batch_size

        print "processed %s records of %s total records" % (processed, count)

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
    parser.add_argument('-r','--region', help='Your dedicated CleverTap Region', required=False)
    parser.add_argument('-p','--path', help='Absolute path to the csv file', required=True)
    parser.add_argument('-m','--mappingpath', help='Absolute path to a json key mapping', required=False)
    parser.add_argument('-t','--type', help='The type of data, either profile or event, defaults to profile', default="profile")
    parser.add_argument('-d','--dryrun', help='Do a dry run, process records but do not upload', default=False, type='bool')

    args = parser.parse_args()

    main(args.id, args.passcode, args.region, args.path, args.mappingpath, args.type, args.dryrun)
