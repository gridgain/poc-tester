#!/usr/bin/env python3

import os
import sys
import poc_cfg
import tasks
import time
from subprocess import SubprocessError, CalledProcessError

if __name__ == '__main__':
    # Create parser
    parser = poc_cfg.create_parser()
    namespace = parser.parse_args(sys.argv[1:])

    # GLOBALS
    me = os.path.abspath(__file__)
    current_directory = os.path.dirname(me)
    print("Current dir is %s" % current_directory)
    auto_poc_parent_dir = os.path.dirname(current_directory)
    print("Auto poc dir %s" % auto_poc_parent_dir)
    utils_parent_dir = os.path.dirname(auto_poc_parent_dir)
    print("Utils dir %s" % utils_parent_dir)
    # poc_dir = os.path.join(utils_parent_dir, 'poc-tester')
    poc_dir = utils_parent_dir
    target_dir = os.path.join(poc_dir, 'target', 'assembly')

    yaml_file = namespace.params_yaml
    scenario_yaml = namespace.scenario_yaml

    (server_hosts, client_hosts, zk_hosts) = (None, None, None)
    if namespace.server_hosts:
        server_hosts = namespace.server_hosts.split(',')
    if namespace.client_hosts:
        client_hosts = namespace.client_hosts.split(',')
    if namespace.zk_hosts:
        zk_hosts = namespace.zk_hosts.split(',')

    hosts = {
        'serverHosts': server_hosts,
        'clientHosts': client_hosts,
        'zooKeeperHosts': zk_hosts,
    }

    hosts_csv = {}
    hosts_pool = {}

    for (k, v) in hosts.items():
        if v:
            hosts_csv[k] = ','.join(v)
            hosts_pool[k] = v.copy()
        else:
            hosts_csv[k] = v

    if not poc_dir or not os.path.exists(poc_dir):
        print("[ ERROR ] POC dir isn't exist: %s" % poc_dir)
        sys.exit(1)

    # Scenarios
    print("Reading scenario from %s" % scenario_yaml)
    scenario = poc_cfg.read_yaml(file=scenario_yaml)

    parameters = {}
    params_yamls = scenario.get('parameters', [])

    # Add one or more YAMLs passed via '-params_yaml' option
    params_yamls.extend(yaml_file)

    if not params_yamls:
        print("[ ERROR ] You must specify path to parameters file either in %s or via commandline option" % scenario_yaml)
        sys.exit(1)

    for yaml_file in params_yamls:
        print("Reading parameters from %s" % yaml_file)
        # Data from newer params_yaml updates existing parameters
        poc_cfg.merge_properties(parameters, poc_cfg.read_yaml(yaml_file))

    if scenario.get('parameters_override'):
        poc_cfg.merge_properties(parameters, scenario.get('parameters_override'))

    poc_cfg.merge_properties(parameters['prepare']['main_parameters'], hosts_csv)

    os.chdir(poc_dir)

    # Build or not / copy or not
    poc_cfg.check_was_built(target_dir)

    # Create properties for POC Tester tasks
    poc_cfg.prepare_properties(params_dict=parameters,
                               scenario_dict=scenario,
                               poc_dir=target_dir)

    print('Hosts distribution:')
    for host_type in hosts.keys():
        print(" - %s: %s" % (host_type, hosts[host_type]))

    os.chdir(target_dir)

    test_steps = scenario.get('test')
    if not test_steps:
        print("[ ERROR ] You must specify tasks in %s" % scenario_yaml)
        sys.exit(1)
    for step in test_steps:
        if not step in parameters:
            print("[WARN] The following key missing in properties and will be removed from scenario: %s" % step)
            test_steps.pop(step)

    # Distribute available hosts between tasks
    for step in test_steps + scenario.get('teardown', []):
        step_params = parameters[step]
        for step_key in step_params.keys():
            if step_key in ('override_clientHosts', 'override_serverHosts'):
                # ips_source_name is 'clientHosts', 'serverHosts'
                ips_source_name = step_params[step_key]['source']

                # POC Tester's parameter to override: either '--clientHosts' or '--serverHosts'
                param_to_override = step_key.split('_')[1]

                if not ips_source_name:
                    print(f"[WARN] No IPs source name is specified for step {step}. Will not override {param_to_override} IPs")
                    break

                if not ips_source_name in hosts.keys():
                    print(f"[WARN] Wrong IPs source for step {step}. Will not override {param_to_override} IPs")
                    break

                if not hosts[ips_source_name]:
                    print(f"[WARN] IPs source for step {step} is empty. Will not override {param_to_override} IPs")
                    break

                ips_pool = hosts_pool[ips_source_name]
                res_step_ips = []  # Result IP addresses which will be
                step_nodes = step_params[step_key].get('nodes', len(hosts[ips_source_name]))

                while len(ips_pool) < step_nodes:
                    ips_pool.extend(hosts[ips_source_name])

                # Pop used IPs from pool
                for i in range(step_nodes):
                    res_step_ips.append(ips_pool.pop(0))

                step_params['main_parameters'][param_to_override] = ','.join(res_step_ips.copy())

    # Save scenario with steps description in a JSON file
    poc_cfg.dump_scenarios_properties(params_dict=parameters,
                                      scenario_dict=scenario,
                                      target_dir=target_dir)

    # Scenario section: 'setup'
    setup_steps = scenario.get('setup')
    if setup_steps:
        print('=' * 20)
        print("Setup section")
        for step in setup_steps:
            print(f"Step: {step}")
            tasks.task_start(parameters[step]['main_parameters'], target_dir)

    print('=' * 20)
    print("Test section")

    # Run scenario tasks
    for step in test_steps:
        print(f"Step: {step}")
        tasks.task_start(main_params=parameters[step]['main_parameters'],
                         poc_dir=target_dir)

    # Maximum expected tasks running time based on 'timeToWork' property
    run_time_max = poc_cfg.calculate_max_task_time(tasks=scenario['test'], params=parameters)

    tasks_classes = poc_cfg.get_tasks_info(
        params_dict=parameters,
        scenario_dict=scenario['test'],
        property="MAIN_CLASS"
    )

    # Check tasks status
    check_interval = 60
    for i in range(0, int(run_time_max + 300), check_interval):
        print("Start check after %s seconds (%s minutes)" % (i, int(i/60)))
        try:
            stats_output = tasks.task_start(main_params={'script': 'bin/include/stats.sh'},
                                        poc_dir=target_dir)
            tasks_running = poc_cfg.check_stats(pool_list=tasks_classes,
                                            output_list=stats_output)
            if not tasks_running:
                print("No running tasks detected")
                break
        except CalledProcessError as e:
            print("Subprocess has finished with a non-zero exit status. Will retry.")
            print(e)
        except SubprocessError as e:
            print("Failed to gather stats. Will retry.")
        time.sleep(check_interval)

    # Scenario section: 'teardown'
    scenario_teardown = scenario.get('teardown')
    if scenario_teardown:
        print('='*20)
        print("Teardown section")
        for step in scenario_teardown:
            print(f"Step: {step}")
            tasks.task_start(parameters[step]['main_parameters'], target_dir)
