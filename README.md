# Modeling IoT Sensors in Aerospike

This is companion code for a future article


## Populate Test Data

```
chmod +x run_workers.sh
./run_workers.sh
```

## Query the Data

```
python update_query_user_profile --help
python: can't open file 'update_query_user_profile': [Errno 2] No such file or directory
ArrrBook-Pro:modeling-user-segmentation rbotzer$ python update_query_user_profile.py --help
usage: update_query_user_profile.py [--help] [-U <USERNAME>] [-P <PASSWORD>]
                                    [-h <ADDRESS>] [-p <PORT>] [-n <NS>]
                                    [-s <SET>] [-i]

optional arguments:
  --help                Displays this message.
  -U <USERNAME>, --username <USERNAME>
                        Username to connect to database.
  -P <PASSWORD>, --password <PASSWORD>
                        Password to connect to database.
  -h <ADDRESS>, --host <ADDRESS>
                        Address of Aerospike server.
  -p <PORT>, --port <PORT>
                        Port of the Aerospike server.
  -n <NS>, --namespace <NS>
                        Port of the Aerospike server.
  -s <SET>, --set <SET>
                        Port of the Aerospike server.
  -i, --interactive     Interactive Mode
```
