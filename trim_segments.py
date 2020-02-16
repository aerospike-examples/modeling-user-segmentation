# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import aerospike
from aerospike import exception as e

try:
    from aerospike_helpers.operations import map_operations as mh
except:
    pass  # Needs Aerospike client >= 3.4.0
import datetime
import pprint
import random
import sys
import time

argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument(
    "--help", dest="help", action="store_true", help="Displays this message."
)
argparser.add_argument(
    "-U",
    "--username",
    dest="username",
    metavar="<USERNAME>",
    help="Username to connect to database.",
)
argparser.add_argument(
    "-P",
    "--password",
    dest="password",
    metavar="<PASSWORD>",
    help="Password to connect to database.",
)
argparser.add_argument(
    "-h",
    "--host",
    dest="host",
    default="127.0.0.1",
    metavar="<ADDRESS>",
    help="Address of Aerospike server.",
)
argparser.add_argument(
    "-p",
    "--port",
    dest="port",
    type=int,
    default=3000,
    metavar="<PORT>",
    help="Port of the Aerospike server.",
)
argparser.add_argument(
    "-n",
    "--namespace",
    dest="namespace",
    default="test",
    metavar="<NS>",
    help="Port of the Aerospike server.",
)
argparser.add_argument(
    "-s",
    "--set",
    dest="set",
    default="profiles",
    metavar="<SET>",
    help="Port of the Aerospike server.",
)
argparser.add_argument(
    "-i",
    "--interactive",
    dest="interactive",
    action="store_true",
    help="Interactive Mode",
)
options = argparser.parse_args()
if options.help:
    argparser.print_help()
    print()
    sys.exit(1)


def version_tuple(version):
    return tuple(int(i) for i in version.split("."))


def pause():
    input("Hit return to continue")


if options.namespace and options.namespace != "None":
    namespace = options.namespace
else:
    namespace = None
set = options.set if options.set and options.set != "None" else None

config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
    policy = {"key": aerospike.POLICY_KEY_SEND}
except e.ClientError as e:
    if not options.quiet:
        print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

version = client.info_all("version")
release = list(version.values())[0][1].split(" ")[-1]
if version_tuple(aerospike.__version__) < version_tuple("3.4.0") or version_tuple(
    release
) < version_tuple("4.6"):
    print(
        "\nPlease use Python client >= 3.4.0, ",
        "Aerospike database >= 4.6 for this example.",
    )
    sys.exit(3)

pp = pprint.PrettyPrinter(indent=2)
spacer = "=" * 30
epoch = datetime.datetime(2019, 1, 1)
now = datetime.datetime.now()
try:
    # Find all segments whose TTL is before this hour
    key = (namespace, set, "u3")
    current_hour = int((now - epoch).total_seconds() / 3600)
    print("\nCurrent hour is {} hours since epoch".format(current_hour))
    if options.interactive:
        pause()

    ops = [
        mh.map_get_by_value_range(
            "u",
            [0, aerospike.null()],
            [current_hour - 1, aerospike.null()],
            aerospike.MAP_RETURN_KEY,
            False,
        ),
        mh.map_size("u")
    ]
    _, _, b = client.operate_ordered(key, ops)
    stale_segments, total_segments = b
    print("This user has a total of {} segments".format(total_segments[1]))
    print("Of those, a total of {} segments should be cleaned".format(len(stale_segments[1])))
    print("Show all segments with a segment TTL before the current hour:")
    print(stale_segments)
    print(spacer)

    # Clean up the stale segments using a background scan with a transaction
    # attached to it
    print("Clean the stale segments from the entire namespace")
    if options.interactive:
        pause()
    ops = [
        mh.map_remove_by_value_range(
            "u",
            [0, aerospike.null()],
            [current_hour - 1, aerospike.null()],
            aerospike.MAP_RETURN_NONE,
            False,
        )
    ]
    #_, _, _ = client.operate_ordered(key, ops)
    scan = client.scan(namespace, set)
    scan.add_ops(ops)
    job_id = scan.execute_background()
    # wait for job to finish
    while True:
        response = client.job_info(job_id, aerospike.JOB_SCAN)
        if response["status"] != aerospike.JOB_STATUS_INPROGRESS:
            break
        time.sleep(0.25)

    ops = [
        mh.map_get_by_value_range(
            "u",
            [0, aerospike.null()],
            [current_hour - 1, aerospike.null()],
            aerospike.MAP_RETURN_KEY,
            False,
        ),
        mh.map_size("u")
    ]
    _, _, b = client.operate_ordered(key, ops)
    stale_segments, total_segments = b
    print("This user now has a total of {} segments".format(total_segments[1]))
    print("Of those, a total of {} segments should be cleaned".format(len(stale_segments[1])))
    print("Show all segments with a segment TTL before the current hour:")
    print(stale_segments)
    print(spacer)

except Exception as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
client.close()
