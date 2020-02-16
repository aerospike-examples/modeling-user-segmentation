# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import aerospike
from aerospike import exception as e

try:
    from aerospike_helpers.operations import list_operations as lh
    from aerospike_helpers.operations import map_operations as mh
    from aerospike_helpers import cdt_ctx as ctxh
except:
    pass  # Needs Aerospike client >= 3.4.0
import datetime
import pprint
import random
import sys

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
    # Upsert a single segment in the user profile
    key = (namespace, set, "u1")
    segment_id = random.randint(0, 81999)
    ttl_dt = now + datetime.timedelta(days=30)
    segment_ttl = int((ttl_dt - epoch).total_seconds() / 3600)
    ops = [
        mh.map_get_by_key("u", segment_id, aerospike.MAP_RETURN_KEY_VALUE),
        mh.map_put("u", segment_id, [segment_ttl, {}]),
        mh.map_get_by_key("u", segment_id, aerospike.MAP_RETURN_KEY_VALUE),
    ]
    print("\nUpsert segment {} => [{}] to user u1".format(segment_id, segment_ttl))
    if options.interactive:
        pause()
    _, _, b = client.operate_ordered(key, ops)
    segment_check, segment_count, new_segment = b
    print("Segment value before: {}".format(segment_check[1]))
    print("Number of segments after upsert: {}".format(segment_count[1]))
    print("Segment value after: {}".format(new_segment[1]))
    print(spacer)

    # Update multiple segments of a user profile
    segments = {}
    ttl_dt = now + datetime.timedelta(days=30)
    segment_ttl = int((ttl_dt - epoch).total_seconds() / 3600)
    for i in range(8):
        segment_id = random.randint(0, 81999)
        segments[segment_id] = [segment_ttl, {}]
    print("\nUpdating multiple segments for user u1")
    pp.pprint(segments)
    if options.interactive:
        pause()
    ops = [
        mh.map_put_items("u", segments),
        mh.map_get_by_value(
            "u", [segment_ttl, aerospike.CDTWildcard()], aerospike.MAP_RETURN_KEY_VALUE
        ),
    ]
    _, _, b = client.operate(key, ops)
    print("Show all segments with TTL {}:".format(segment_ttl))
    print(b["u"])
    print(spacer)

    # Update a segment's TTL by 5 extra hours
    # First, the context for the list increment (path to the TTL)
    ctx = [ctxh.cdt_ctx_map_key(segment_id)]
    ops = [
        lh.list_increment("u", 0, 5, ctx=ctx),
        mh.map_get_by_key("u", segment_id, aerospike.MAP_RETURN_KEY_VALUE),
    ]
    print("\nAdd 5 hours to the TTL of user u1's segment {}".format(segment_id))
    if options.interactive:
        pause()
    _, _, b = client.operate(key, ops)
    print(b["u"])
    print(spacer)

    # Fetch the user's segments that are not going to expire today
    today = datetime.datetime(now.year, now.month, now.day)
    end_ttl = int((today - epoch).total_seconds() / 3600)
    print("\nGet only user segments that are not going to expire today")
    if options.interactive:
        pause()
    ops = [
        mh.map_get_by_value_range(
            "u",
            [0, aerospike.null()],
            [end_ttl, aerospike.null()],
            aerospike.MAP_RETURN_KEY,
            True,
        )
    ]
    _, _, b = client.operate(key, ops)
    print("Show all segments not expiring today:".format(segment_ttl))
    print(b["u"])
    print(spacer)

    # Count all the segments in the segment ID range 8000-9000
    print("\nCount how many segments u1 has in the segment ID range 8000-9000")
    if options.interactive:
        pause()
    ops = [mh.map_get_by_key_range("u", 8000, 9000, aerospike.MAP_RETURN_COUNT)]
    _, _, b = client.operate(key, ops)
    print(b["u"])
    print(spacer)

    # Clear all the segments that expire before today from the user u1
    print("Clean the stale segments for user u1")
    if options.interactive:
        pause()
    ops = [
        mh.map_remove_by_value_range(
            "u",
            [0, aerospike.null()],
            [end_ttl, aerospike.null()],
            aerospike.MAP_RETURN_COUNT,
            False,
        ),
        mh.map_size("u"),
    ]
    _, _, b = client.operate_ordered(key, ops)
    stale_segments, segments_left = b
    print(
        "User u1 had {} stale segments trimmed, with {} segments remaining".format(
            stale_segments[1], segments_left[1]
        )
    )
    print(spacer)
except Exception as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
client.close()
