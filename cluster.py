"""cluster.py to manage edht nodes
"""
# -*- coding: utf-8 -*-

import sys
import os
import configparser

from curio import subprocess
import curio


def generate_env_variables(client_port=6000, node_port=7000, total_nodes=3):
    """Generate and return environment variable for all nodes.
    """
    client_ports = [client_port + i for i in range(total_nodes)]
    node_ports = [node_port + i for i in range(total_nodes)]
    node_ips = ['127.0.0.1:{}'.format(node_ports[i]) for i in range(total_nodes)]
    # + 1 to start node name from 1
    node_names = ['edht-{}'.format(i+1) for i in range(total_nodes)]
    return {'client_ports': client_ports, 'node_ports': node_ports,
            'node_ips': node_ips, 'node_names': node_names,
            'replication': "3", 'node_timeout_ms': "10000"}


def create_config_files(variables, base_path='config'):
    """create a config file with the given variables inside `base_path`.
    """
    config = configparser.ConfigParser()
    files = []
    for i in range(len(variables['node_ips'])):
        config['DEFAULT']['client_port'] = str(variables['client_ports'][i])
        config['DEFAULT']['node_port'] = str(variables['node_ports'][i])
        config['DEFAULT']['node_ips'] = ','.join(variables['node_ips'])
        config['DEFAULT']['name'] = variables['node_names'][i]
        config['DEFAULT']['replication'] = variables['replication']
        config['DEFAULT']['node_timeout_ms'] = variables['node_timeout_ms']
        filename = os.path.join(base_path, 'config_{}.ini'.format(i + 1))
        with open(filename, 'w') as conf_file:
            config.write(conf_file)
            files.append(conf_file.name)
    return files


async def run_command(config_file, option="shell"):
    """Run the rebar3 compile command
    """
    # Setting environment variable via subprocess raises permission denied.
    # So generate a shell script and run.
    # Plain wicked though
    FORMAT = """
    export CONFIG_FILE="{}"
    rebar3 {}
    """.format(config_file, option)
    bash_file = config_file.split('.')[0] + '.sh'
    with open(bash_file, 'w') as f:
        f.write(FORMAT)
    try:
        process = subprocess.Popen(['bash', bash_file], stdout=subprocess.PIPE)
        async for line in process.stdout:
            print('{}:{}'.format(config_file, line.decode('ascii')), end='')
    except:
        print('Received KeyboardInterrupt, terminating {}'.format(process._popen.args))
        process.terminate()


async def main(total_nodes):
    """Co ordinate the process
    """
    # 1. Generate environment variable for each process
    variables = generate_env_variables(total_nodes=total_nodes)
    # 2. Create a new config file
    conf_files = create_config_files(variables)
    # 3. Set config file as env_variable and run the process
    tasks = []
    for i in range(total_nodes):
        print("Starting process {}".format(i))
        task = await curio.spawn(
            run_command(config_file=conf_files[i]))
        tasks.append(task)

    try:
        for task in tasks:
            await task.join()
    except:
        pass


if __name__ == "__main__":
    print('Current PID of the process: {}'.format(os.getpid()))
    if len(sys.argv) >=2:
        try:
            total_nodes = int(sys.argv[1])
        except ValueError:
            print('Total nodes should be int')
            exit(1)
    else:
        total_nodes = 3
    try:
        curio.run(main(total_nodes=total_nodes))
    except:
        print('shutting down')
