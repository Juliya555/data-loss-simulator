#!/usr/bin/python3

"""Data-loss simulator

This script allows to count the probability(%) of data loss in 2 cases of database-sharding:
    -  random
    -  mirror

Example of scrpit usage:
./simulate_random_pairs.py -n 10 --random

Notice 1: current algorithm is strictly bound to amount of data replications. If there is going to be not 2,
          but more replications, the algorithm should be rewritten.
Notice 2: need to make script file executable (chmod +x simulate_random_pairs.py) before running as described above.
"""

import argparse
import random

shards = 100
replicas = 2  # don't change this because of Notice 1 described above
lost_servers_count = 2
seed = random.randint(0, 1000)
random.seed(seed)

# Initialize a parser to parse command-line parameters.
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-n', help='amount of virtual severs', type=int, default=10)
parser.add_argument('--random', help='random mode of data replication', action="store_true")
parser.add_argument('--mirror', help='mirror mode of data replication', action="store_true")
args = parser.parse_args()

# Assertions to check parameters inputted by user.
# It gives a specified message to the user if the given parameters won't allow to run the script properly.
assert args.random is True and args.mirror is False or args.random is False and args.mirror is True, \
    "Please provide either random or mirror flag"
assert args.n > 1, "You need at least 2 servers to run simulations ;)"
assert args.n < shards, "Too much servers as for amount of data (shards) = " + str(shards)
assert args.n % 2 == 0, "Quantity of servers should be an even number because of the mirror mode specifics " \
                        "and for random mode (for comparison purposes)"
if args.random is True:
    assert shards % args.n == 0, "Quantity of servers should allow to share shards among them evenly"


servers_quantity = args.n
server_capacity = int((shards * replicas) / servers_quantity)


def generate_random(srv_q, shrds, srv_cap):
    """Place shards (data fragments) on servers in mirror mode"""
    def clean_filled_servers_list(lst, srs, c):
        for i in lst:
            if len(srs[i]) >= c:
                lst.remove(i)
        return lst

    srvs = {i: list() for i in range(1, srv_q + 1)}
    data_input = [i for i in range(1, shrds + 1)]
    srvs_list = list(srvs.keys())

    random.shuffle(data_input)

    for item in data_input:
        random.shuffle(srvs_list)
        srvs_list = clean_filled_servers_list(srvs_list, srvs, srv_cap)
        found_even = False
        found_odd = False
        for srv_id in srvs_list:
            if found_even is False:
                if srv_id % 2 == 0:
                    srvs[srv_id].append(item)
                    found_even = True
            if found_odd is False:
                if srv_id % 2 == 1:
                    srvs[srv_id].append(item)
                    found_odd = True
            if found_even is True and found_odd is True:
                break
        srvs_list = clean_filled_servers_list(srvs_list, srvs, srv_cap)
    return srvs


def generate_mirror(srv_q, shrds, srv_cap):
    """Place shards (data fragments) on servers in mirror mode"""
    srvs = {i: list() for i in range(1, srv_q + 1)}
    data_input = [i for i in range(1, shrds + 1)]
    counter = 0
    for srv_id, srv_shards in srvs.items():
        if srv_id % 2 == 1:
            srvs[srv_id] = data_input[(counter * srv_cap):((counter * srv_cap) + srv_cap)]
            counter += 1
        else:
            srvs[srv_id] = srvs[srv_id - 1]
    return srvs


if args.random is True:
    servers_combination = generate_random(servers_quantity, shards, server_capacity)
else:
    servers_combination = generate_mirror(servers_quantity, shards, server_capacity)

# Part of script calculating and printing probability
combinations_quantity = 0
fails_quantity = 0
for i in range(1, servers_quantity + 1):
    for j in range(1, servers_quantity + 1):
        if i >= j:
            continue
        combinations_quantity += 1
        for shard_item in servers_combination[i]:
            if shard_item in servers_combination[j]:
                fails_quantity += 1
                break

data_loss_cases = int(round((fails_quantity * 100) / combinations_quantity))
print("Killing {0} arbitrary servers results in data loss in {1}% cases".format(lost_servers_count,
                                                                                data_loss_cases))
