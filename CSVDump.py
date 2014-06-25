import os
import sys
import optparse
import shutil
import csv
import hashlib
import datetime
import sqlite3

from Profile import Profile

from Blobber import CreateMemoryOnlyBlob,LoadSavedBlob

INCLUDE     =   {
                    "Groups":           ["Owner"],
                    "ProfileToFetish":  ["ProfileId"]
                }

EXCLUDE     =   {
                    "Fetishes":         ["Id"]
                }

NULL        =   {
                    "Profiles":         ["Name"],
                    "Groups":           ["Name"]
                }

def adapt_pass(ts):
    return "%s" % ts

#sqlite3.register_adapter(datetime.datetime,adapt_pass)
#sqlite3.register_adapter(datetime.date,adapt_pass)

if __name__ == "__main__":
    usage       =   "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-b', '--blob', help="blob to convert", action="store",default=None)

    options, args = parser.parse_args()

    if options.blob is None:
        parser.error("Blob value required")

    sys.stderr.write("Loading Blob [%s]\n" % options.blob)
    #profileDb   =   LoadSavedBlob(options.blob)

    #profileDb.InitIdentGroups(Profile.IDENTITY_GROUPS)
    conn    =   sqlite3.connect(options.blob)


    root    =   os.path.basename(options.blob)
    root,_  =   os.path.splitext(root)

    csvPath =   os.path.join("Report",root)
    sys.stderr.write("Saving to [%s]\n" % csvPath)
    if os.path.exists(csvPath):
        sys.stderr.write("Removing old directory\n")
        shutil.rmtree(csvPath)
    os.mkdir(csvPath)
    cursor  =   conn.cursor()
    cursor.execute("SELECT * FROM sqlite_master WHERE type=\"table\"")
    tableNames  =   [ x[1] for x in cursor.fetchall()]
    sys.stderr.write("TableNames [%s]\n" % tableNames)
    for tableName in tableNames:
        sys.stderr.write("Table [%s]\n" % tableName)
        with open(os.path.join(csvPath,"%s.csv" % tableName),"w") as fp:
            writer = csv.writer(fp,dialect=csv.excel)
            cursor.execute("PRAGMA table_info(%s)" % tableName)
            colNames    =   [x[1] for x in cursor.fetchall()]
            writer.writerow(colNames)
            hashMask    =   []
            nullMask    =   []
            for colName in colNames:
                if tableName in EXCLUDE and colName in EXCLUDE[tableName]:
                    hashMask.append(False)
                elif colName == "Id" or colName == "DstId":
                    hashMask.append(True)
                elif tableName in INCLUDE and colName in INCLUDE[tableName]:
                    hashMask.append(True)
                else:
                    hashMask.append(False)

                if tableName in NULL and colName in NULL[tableName]:
                    nullMask.append(True)
                else:
                    nullMask.append(False)

            for idx in range(len(colNames)):
                sys.stderr.write("[%16s][%8s][%8s]\n" % (colNames[idx],hashMask[idx],nullMask[idx]))

            cursor.execute("SELECT * FROM %s" % tableName)
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                line    =   []
                for idx in range(len(colNames)):
                    if hashMask[idx] is True:
                        line.append(hashlib.md5(b"%s" % row[idx]).hexdigest())
                    elif nullMask[idx] is True:
                        line.append("Redacted")
                    elif isinstance(row[idx],int):
                        line.append("%s" % row[idx])
                    elif row[idx] is None:
                        line.append("")
                    else:
                        #sys.stderr.write("[%s][%s]\n" % (type(row[idx]),row[idx]))
                        #line.append(row[idx].encode("ascii","replace"))
                        line.append(row[idx].encode("utf-8","replace"))
                writer.writerow(line)
