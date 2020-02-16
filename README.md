# Modeling User Segmentation in Aerospike

This is companion code for my Medium article [Aerospike Modeling: User Profile Store](https://medium.com/@rbotzer/aerospike-modeling-user-profile-store-dc3c1464b60a)


## Populate Test Data

```
chmod +x run_workers.sh
./run_workers.sh
```

## Query the Data

```
python update_query_user_profiles.py --help
usage: update_query_user_profiles.py [--help] [-U <USERNAME>] [-P <PASSWORD>]
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
