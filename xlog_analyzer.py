#!/usr/bin/env python

import argparse
import os
import sys
import subprocess
import re

DEFAULT_PG_XLOGDUMP = "pg_xlogdump"

ERROR_CODES = {
        "xlog-segment_not_file" : 1,
        "xlog-path_not_dir" : 2,
        "xlog_not_exe" : 3
    }


def setup_argparse():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('xlog_segment', nargs="*")
    parser.add_argument('--pg_xlogdump', help="path to pg_xlogdump")
    parser.add_argument('-v', '--verbose', action='count', help="verbose output")
    parser.add_argument('-t', '--top_relations', action='count', help="print top relations")
    parser.add_argument('-n', '--top_n_relations', type=int, default=10, help="number of relations to print")
    parser.add_argument('-s', '--summary', action='count', help="print a summary")
    parser.add_argument('-R', '--resolve_relation_names', action='count', help="Print relation names instead of relfilenodes")

    parser.add_argument('-U', '--user', help="PostgreSQL User name")
    parser.add_argument('-h', '--host', help="PostgreSQL Host or IP Address")
    parser.add_argument('-p', '--port', help="PostgreSQL Port")
    parser.add_argument('-d', '--dbname', help="PostgreSQL Connection Database")

    parser.add_argument('--help', action='count', help="Print help")
    return parser

def read_xlog_file(file_path, args):
    cmd = "%s %s" % (args.pg_xlogdump, file_path)

    if args.verbose > 2:
        print "Executing: %s" % (cmd)

    proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = proc.communicate()

    return (out, err)

def init_xlog_stats():
    xlog_stats = {
        "count"                      : 0,
        "n_heap"                     : 0,
        "n_heap2"                    : 0,
        "n_btree"                    : 0,
        "n_transaction"              : 0,
        "n_other"                    : 0,
        "n_relation"                 : 0,
        "n_page"                     : 0,
        "n_distinct_relation"        : 0,
        "n_distinct_page"            : 0,
        "n_bkp"                      : 0,
        "n_avg_page_per_relation"    : 0,
        "n_avg_page_per_transaction" : 0,
        "n_insert"                   : 0,
        "n_update"                   : 0,
        "n_hotupdate"                : 0,
        "n_delete"                   : 0,
        "n_commit"                   : 0,
        "n_abort"                    : 0,
        "relations"                  : {}
        }

    return xlog_stats

def print_xlog_stats(xlog, xlog_stats, args, dbconnection=None):
    print "XLOG Segment: %s\n" % (xlog)
    print "Overall Count:   %10d" % xlog_stats["count"]

    # Xlog type statistics
    print "\nXlog Record Types:"
    print "  Heap:          %10d" % xlog_stats["n_heap"]
    print "  Heap2:         %10d" % xlog_stats["n_heap2"]
    print "  Btree:         %10d" % xlog_stats["n_btree"]
    print "  Transaction:   %10d" % xlog_stats["n_transaction"]
    print "  Other:         %10d" % xlog_stats["n_other"]

    # Record type statistics
    print "\nRecord Type:"
    print "  INSERT:        %10d" % xlog_stats["n_insert"]
    print "  UPDATE:        %10d" % xlog_stats["n_update"]
    print "  HOTUPDATE:     %10d" % xlog_stats["n_hotupdate"]
    print "  DELETE:        %10d" % xlog_stats["n_delete"]
    print "  COMMIT:        %10d" % xlog_stats["n_commit"]
    print "  ABORT:         %10d" % xlog_stats["n_abort"]

    # Relation statistics:
    print "\nRelation:"
    print "  Total:         %10d" % xlog_stats["n_relation"]
    print "  Distinct:      %10d" % xlog_stats["n_distinct_relation"]

    # Page statistics:
    print "\nPage:"
    print "  Total:         %10d" % xlog_stats["n_page"]
    print "  Distinct:      %10d" % xlog_stats["n_distinct_page"]
    print "  Backup Pages:  %10d" % xlog_stats["n_bkp"]

    print "\nPage per Relation:"
    print "  Average        %10d" % xlog_stats["n_avg_page_per_relation"]

    print "\nPage per Transaction:"
    print "  Avegage        %10d" % xlog_stats["n_avg_page_per_transaction"]

    if args.top_relations:
        print_top_n_relations(\
                    xlog_stats["relations"], \
                    args.top_n_relations, \
                    args.resolve_relation_names, \
                    dbconnection)

def parse_xlogdump_output(output, xlog_stats=None):

    if xlog_stats is None:
        xlog_stats = init_xlog_stats()

    relations = xlog_stats["relations"]

    re_heap = re.compile(r"\ Heap\ ")
    re_heap2 = re.compile(r"\ Heap2")
    re_btree = re.compile(r"\ Btree")
    re_transaction = re.compile(r"\ Transaction")
    re_insert = re.compile(r"\ insert")
    re_update = re.compile(r"\ update")
    re_hotupdate = re.compile(r"\ hotupdate")
    re_delete = re.compile(r"\ delete")
    re_commit = re.compile(r"\ commit")
    re_abort = re.compile(r"\ abort")

    re_page = re.compile(r'.*rel\ [0-9]*\/[0-9]*\/([0-9]*).*tid\ ([0-9]*).*')

    re_bkp = re.compile(r'.*bkp:\ ([0-9])([0-9])([0-9])([0-9]).*')

    for line in output.split("\n"):
        xlog_stats["count"] += 1
        if re_heap.search(line):
            xlog_stats["n_heap"] += 1
        if re_heap2.search(line):
            xlog_stats["n_heap2"] += 1
        if re_btree.search(line):
            xlog_stats["n_btree"] += 1
        if re_transaction.search(line):
            xlog_stats["n_transaction"] += 1
        if re_insert.search(line):
            xlog_stats["n_insert"] += 1
        if re_update.search(line):
            xlog_stats["n_update"] += 1
        if re_hotupdate.search(line):
            xlog_stats["n_hotupdate"] += 1
        if re_delete.search(line):
            xlog_stats["n_delete"] += 1
        if re_commit.search(line):
            xlog_stats["n_commit"] += 1
        if re_abort.search(line):
            xlog_stats["n_abort"] += 1

        rel_match = re_page.match(line, re.M|re.I)

        if rel_match:
            relation = rel_match.group(1)
            page = rel_match.group(2)

            xlog_stats["n_relation"] += 1
            xlog_stats["n_page"] += 1

            if not relation in relations:
                relations[relation] = {}
                xlog_stats["n_distinct_relation"] += 1

            if not page in relations[relation]:
                relations[relation][page] = 0
                xlog_stats["n_distinct_page"] += 1

            relations[relation][page] += 1

        bkp_match = re_bkp.match(line, re.M|re.I)

        if bkp_match:
            for i in range(1, 5):
                if bkp_match.group(i) == "1":
                    xlog_stats["n_bkp"] += 1

    xlog_stats["n_other"] = xlog_stats["count"] - \
            (xlog_stats["n_heap"] + \
             xlog_stats["n_heap2"] + \
             xlog_stats["n_btree"] + \
             xlog_stats["n_transaction"])

    if xlog_stats["n_distinct_relation"]:
        xlog_stats["n_avg_page_per_relation"] = \
                xlog_stats["n_distinct_page"] / xlog_stats["n_distinct_relation"]
        xlog_stats["n_avg_page_per_transaction"] = \
                xlog_stats["n_page"] / xlog_stats["n_transaction"]

    return xlog_stats

def is_file(file_path):
    return os.path.isfile(file_path)

def is_executable(file_path):
    return is_file(file_path) and os.access(file_path, os.X_OK)

def is_directory(dir_path):
    return os.path.isdir(dir_path)

def check_arguments(args):
    if args.pg_xlogdump and not is_executable(args.pg_xlogdump):
        sys.stderr.write("\"%s\" is not present or not executable" % (args.pg_xlogdump))
        sys.exit(ERROR_CODES["xlog_not_exe"])

    #if args.xlog_path and not is_directory(args.xlog_path):
        #sys.stderr.write("\"%s\" is not a directory" % (args.xlog_path))
        #sys.exit(ERROR_CODES["xlog-path_not_dir"])

    if args.xlog_segment:
        for xlog in args.xlog_segment:
            if not is_file(xlog):
                sys.stderr.write("\"%s\" is not a file" % (xlog))
                sys.exit(ERROR_CODES["xlog-path_not_dir"])

def print_top_n_relations(relations, n, resolve_names=False, dbconnection=None):
    # Get a sorted list of Tuples, ordered by count pages.
    top_n_relations = \
            sorted(relations.items(), key=lambda x: len(x[1]), reverse=True)

    print "\nTop %d Relations:" % (n)

    if resolve_names:
        sql = "SELECT relname FROM pg_class WHERE relfilenode = %s"

    for i, (rel, pages) in enumerate(top_n_relations):
        if i > n:
            break
        if resolve_names and not dbconnection is None:
            dbcursor = dbconnection.cursor()
            dbcursor.execute(sql, (rel,))
            (relname) = dbcursor.fetchone()
            print "  Relation: %s (%s), number of Pages: %d" % (relname, rel, len(pages))
        else:
            print "  Relation: %s, number of Pages: %d" % (rel, len(pages))

def main():
    parser = setup_argparse()
    args = parser.parse_args()

    dbconnection = None

    if args.help:
        parser.print_help()
        sys.exit(0)

    check_arguments(args)

    if args.resolve_relation_names:
        import psycopg2
        connection_string = ""
        if args.dbname:
            connection_string += "dbname='%s'" % args.dbname
        if args.host:
            connection_string += "host='%s'" % args.host
        if args.port:
            connection_string += "port='%s'" % args.port
        if args.user:
            connection_string += "user='%s'" % args.user
        dbconnection = psycopg2.connect(connection_string)

    if not args.pg_xlogdump:
        args.pg_xlogdump = DEFAULT_PG_XLOGDUMP

    if args.summary:
        overall_xlog_stats = init_xlog_stats()

    if args.xlog_segment:
        for xlog in args.xlog_segment:
            if args.xlog_segment:
                (xlogdump_out, _) = read_xlog_file(xlog, args)

            xlog_stats = parse_xlogdump_output(xlogdump_out) 

            if args.summary:
                for entry in xlog_stats:
                    if isinstance(overall_xlog_stats[entry], dict):
                        overall_xlog_stats[entry].update(xlog_stats[entry])
                    else:
                        overall_xlog_stats[entry] += xlog_stats[entry]

            print_xlog_stats(xlog, xlog_stats, args, dbconnection)

            print ""

    if args.summary:
        overall_xlog_stats["n_avg_page_per_relation"] /= len(args.xlog_segment)
        overall_xlog_stats["n_avg_page_per_transaction"] /= len(args.xlog_segment)
        print_xlog_stats(\
                "Overall Statistics", overall_xlog_stats, args, dbconnection)

if __name__ == "__main__":
    main()
