# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import aerospike
from aerospike import exception as e
import datetime
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
    "-f",
    "--from",
    dest="from_user",
    type=int,
    default=1,
    metavar="<FROM-USER>",
    help="Start with a specific user ID.",
)
argparser.add_argument(
    "-t",
    "--to",
    dest="to_user",
    type=int,
    metavar="<TO-USER>",
    help="End with a specific user ID.",
)
argparser.add_argument(
    "-q", "--quiet", dest="quiet", action="store_true", help="Quiet Mode"
)
options = argparser.parse_args()
if options.help:
    argparser.print_help()
    print()
    sys.exit(1)


def version_tuple(version):
    return tuple(int(i) for i in version.split("."))


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
) < version_tuple("4.0"):
    if not options.quiet:
        print(
            "\nPlease use Python client >= 3.4.0, ",
            "Aerospike database >= 4.0 for this example.",
        )
    sys.exit(3)

from_user = options.from_user
if not options.to_user:
    to_user = from_user + 100000
else:
    to_user = options.to_user
epoch = datetime.datetime(2019, 1, 1)
now = datetime.datetime.now()
low = now - datetime.timedelta(days=179)
low_hour = int((low - epoch).total_seconds() / 3600)
high = now + datetime.timedelta(days=28)
high_hour = int((high - epoch).total_seconds() / 3600)
segment_ids = [i for i in range(82000)]
# populate 500K user profiles
for i in range(from_user, to_user):
    uid = "u{}".format(i)
    key = (namespace, set, uid)
    segs = random.sample(segment_ids, 1000)
    user_profile = {}
    for s in segs:
        ttl = random.randrange(low_hour, high_hour)
        user_profile[s] = [ttl, {}]
    if not options.quiet:
        print(uid)
    try:
        client.put(key, {"u": user_profile})
    except Exception as e:
        if not options.quiet:
            print("Error: {0} [{1}]".format(e.msg, e.code))
client.close()
