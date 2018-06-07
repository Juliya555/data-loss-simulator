#!/usr/bin/python3

"""Data-loss simulator

This script allows to count the probability(%) of data loss in 2 cases of database-sharding:
    -  random
    -  mirror

You can define the following variables according to your needs to simulate different combinations:
    - shards -- the total number of data fragments
    - replicas -- the number of copies
    - lost_servers_count -- the number of simultaneously killed servers

Example of scrpit usage:
./simulate.py -n 10 --random

Notice: need to make script file executable (chmod +x simulate.py) before running as described above.
"""

import argparse
import random

shards = 100
replicas = 2
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


def try_generate_random(srv_q, shrds, srv_cap, repl):
    """Place shards (data fragments) on servers randomly"""
    srvs = {i: list() for i in range(1, srv_q + 1)}

    def get_available_servers(srvs, shrd, server_capacity):
        """Define a pool of available servers for placing shards"""
        available_srvs = []
        for srv_id, srv_shrds in srvs.items():
            if len(srv_shrds) >= server_capacity or shrd in srv_shrds:
                continue
            available_srvs.append(srv_id)
        return available_srvs

    for replica in range(1, repl + 1):
        for shard in range(1, shrds + 1):
            available_servers = get_available_servers(srvs, shard, srv_cap)
            # ugly solution: catching situation when last insert is blocked by data duplication check
            try:
                chosen_index = random.randint(0, len(available_servers) - 1)
            except ValueError:
                return None
            srv_id = available_servers[chosen_index]
            srvs[srv_id].append(shard)
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
    # ugly solution utilizing infinite loop with exit on successful filling servers with shards
    while True:
        servers_combination = try_generate_random(servers_quantity, shards, server_capacity, replicas)
        if servers_combination is None:
            seed_old = seed
            seed = random.randint(0, 1000)
            random.seed(seed)
            # random seeds debug print for which data filling appears to be unsuccessful if done randomly
            # print("Seed {0} is unsuccessful =( Using {1} =)".format(seed_old, seed))
        else:
            break
else:
    # request to fill servers with data in mirror mode
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
