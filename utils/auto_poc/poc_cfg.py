import yaml
import argparse
import sys
from tempfile import mkstemp
import re
import os
import zipfile
import shutil
import json


# Command line parser
def create_parser():
    """

    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-params_yaml', action='append', default=[],
                        help='Path to additional params.yaml. More than one option may be passed')
    parser.add_argument('-scenario_yaml', default=os.path.join(os.getcwd(), 'res', 'scenario.yaml'),
                        help='Path to scenario.yaml')
    parser.add_argument('-server_hosts', default=None,
                        help='Comma-separated hosts for server nodes.')
    parser.add_argument('-client_hosts', default=None,
                        help='Comma-separated list of hosts for client nodes')
    parser.add_argument('-zk_hosts', default=None,
                        help='Comma-separated list of hosts for ZooKeeper nodes')
    return parser


def prepare_hosts_list(dt):
    some_list = []
    for item in dt:
        some_list.append(item['public_ip'])

    return some_list


def sort_servers(ip_list, serv_percent):
    """

    :param ip_list:
    :return:
    """

    n = len(ip_list)
    serv_num = int(n * serv_percent/100)
    s = ",".join(ip_list[0:serv_num])
    c = ",".join(ip_list[serv_num:])
    servers = {'servers': s,
               'clients': c}
    return servers


def find_min(dct):
    mn = min(dct, key=dct.get)
    return mn


def replace_in_file(file_path, pattern, subst):
    """
    Replase pattern to subst in file_path
    """
    # Create temp file
    fh, abs_path = mkstemp()
    with open(abs_path, 'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(re.sub(pattern, subst, line))
    os.close(fh)
    # Remove original file
    os.remove(file_path)
    # Move new file
    shutil.move(abs_path, file_path)


def read_yaml(file):
    data = None
    with open(file, 'r') as stream:
        try:
            data = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            sys.exit(1)
    return data


def convert_prop_to_dict(file):
    prop_dict = {}
    with open(file) as pf:
        data = pf.readlines()
    for line in data:
        line = line.strip()
        if re.match('^[a-zA-Z]+', line) is not None:
            splitted_line = line.split('=')
            prop_dict[splitted_line[0]] = splitted_line[1]
    return prop_dict


def save_dict_to_propfile(file, dict):
    # Backup original prop file
    file_bak = file + '.orig'
    if os.path.exists(file) and not os.path.exists(file_bak):
        try:
            shutil.copyfile(file, file_bak)
        except OSError as err:
            print(err)
            sys.exit(1)

    try:
        if not os.path.exists(os.path.dirname(file)):
            os.mkdir(os.path.dirname(file))
        with open(file, 'w') as pf:
            for k in dict.keys():
                pf.write("{0}={1}\n".format(k, dict[k]))
    except OSError as err:
        print(err)
        sys.exit(1)

    print("Task properties from YAML was saved to %s" % file)


def save_jvmopts_to_propfile(file, options):
    # Backup original file
    file_bak = file + '.orig'
    if os.path.exists(file) and not os.path.exists(file_bak):
        try:
            shutil.copyfile(file, file_bak)
        except OSError as err:
            print(err)
            sys.exit(1)

    if options.get('SERVER_JVM_OPTS_RANDOM'):
        random_opts = permute_jvm(options.pop('SERVER_JVM_OPTS_RANDOM'))

        for (i, opt) in enumerate(random_opts):
            options[f"SERVER_JVM_OPTS_RANDOM_{i}"] = opt

    try:
        with open(file, 'w') as pf:
            for (option, vals) in options.items():
                pf.write(f"{option}=\\\n    ")
                pf.write(" \\\n    ".join(vals))
                pf.write("\n\n")
    except OSError as err:
        print(err)
        sys.exit(1)

    print("JVM properties from YAML was saved to %s" % file)


def permute_jvm(opts):
    """
    :param opts: a list of dictionaries; the dictionary keys are JVM options
    :return: a list of all possible combinations of JVM options' values
    """
    (k1, v1s) = opts.pop(0).popitem()
    kv1_arr = []

    for v1 in v1s:
        kv1_arr.append([f"{k1}={v1}"])

    return permute_jvm_recursion(kv1_arr, opts)


def permute_jvm_recursion(kv1_arr, opts):
    """
    Recursive function to combine values of several JVM options
    :param kv1_arr: a list of already combined values
    :param opts: a list of values not yet combined
    :return:
    """
    if not opts:
        return kv1_arr

    perm = []
    (k2, v2s) = opts.pop(0).popitem()

    for kv1 in kv1_arr:
        for v2 in v2s:
            perm.append(kv1 + [f"{k2}={v2}"])

    return permute_jvm_recursion(perm, opts)


def compare_dicts(dict1, dict2):
    """
    :param dict1: dictionary from gg-qa yaml file
    :param dict2: dictionary that was received from current poc properties file
    :return:
    """
    # Missing keys
    yaml_misses = []
    prop_misses = []
    for k in dict1.keys():
        if not k in dict2.keys():
            prop_misses.append(k)
    for k in dict2.keys():
        if not k in dict1.keys():
            yaml_misses.append(k)

    if yaml_misses or prop_misses:
        print("[WARN] YAML lacks: ", yaml_misses)
        print("[WARN] Property file lacks: ", prop_misses)

def merge_properties(props1, props2):
    """
    :param props1: Target properties dictionary
    :param props2: Source properties dictionary, overwrites keys present in props2
    :return:
    """
    for k, v in props2.items():
        if props1.get(k):
            if isinstance(props1[k], dict) and isinstance(v, dict):
                props1[k] = merge_properties(props1[k], v)
                continue
        props1[k] = v
    return props1

def check_was_built(target_poc_dir):
    """
    Check build directory
    :param poc_dir: poc-tester/target/assembly
    """
    not_ex = 0
    for item in ['bin', 'config', 'libs', 'poc-tester-libs']:
        if not os.path.exists(os.path.join(target_poc_dir, item)):
            not_ex += 1
    if not_ex > 0:
        print("Build failed! Please check %s" % target_poc_dir)
        sys.exit(1)


def zipdir(path, ziph):
    """
    Pack directory
    :param path: directory for packing
    :param ziph: zip object
    """
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))


def zip_poc_dir(poc_dir_path):
    """
    :param poc_dir: target poc directory
    """
    os.chdir(poc_dir_path)
    zipf = zipfile.ZipFile('poc_dir.zip', mode='w')
    zipdir(os.getcwd(), zipf)
    zipf.close()


def remove_if_exist(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)

def prepare_properties(params_dict, scenario_dict, poc_dir):
    """
    :param params_dict: data that was read from yaml
    :param poc_dir: home poc directory
    :return:
    """
    if not isinstance(params_dict, dict) or not params_dict:
        sys.exit("Please, check params.yaml. It should contains dictionary with parameters.")

    compare_properties = False
    props_common = params_dict.get('common_properties')

    for stage in ('setup', 'test', 'teardown'):
        if scenario_dict.get(stage):
            for step in scenario_dict[stage]:
                step_main_params = params_dict[step]['main_parameters']
                step_props = params_dict[step].get('properties_params', {})

                if step_main_params.get('taskProperties'):
                    property_path = os.path.join(poc_dir, step_main_params['taskProperties'])

                    if props_common:
                        # Merge task properties with common
                        for k, v in props_common.items():
                            # If property is already defined, then do not overwrite
                            if step_props.get(k) is not None:
                                continue
                            step_props[k] = v

                    # Compare properties from file with properties from params_dict
                    if compare_properties:
                        if os.path.exists(property_path):
                            p_dict = convert_prop_to_dict(property_path)
                        else:
                            p_dict = {}
                        print("Comparing %s with parameters from YAML..." % property_path)
                        compare_dicts(dict1=params_dict[step]["properties_params"], dict2=p_dict)

                    save_dict_to_propfile(file=property_path, dict=params_dict[step]["properties_params"])

    # Save JVM options from YAML to .properties file
    if params_dict.get('jvm_opts'):
        file = os.path.join(poc_dir, 'config/jvm-opts/jvm-opts.properties')
        save_jvmopts_to_propfile(file, params_dict['jvm_opts'])


def calculate_max_task_time(tasks, params):
    work_times = []
    max_time = 0
    for task in tasks:
        if params[task].get('properties_params') and params[task]['properties_params'].get('timeToWork'):
            work_times.append(params[task]['properties_params']['timeToWork'])
    if work_times:
        max_time = max(work_times)
    return max_time

def get_tasks_info(params_dict, scenario_dict, property):
    properties_list = []
    print(scenario_dict)
    for t in scenario_dict:
        if t.endswith("_task"):
            properties_list.append(params_dict[t]['properties_params'][property])
    return properties_list

def check_stats(pool_list, output_list):
    alive = []
    ret_val = False
    for i in pool_list:
        for j in output_list:
            if i in str(j):
                alive.append(i)
                break
    if len(alive) > 0:
        ret_val = True
    return ret_val


def dump_scenarios_properties(params_dict, scenario_dict, target_dir):
    scenarios_tmp = {}

    scenarios_tmp['parameters'] = {}
    for param in ('common_properties', 'jvm_opts'):
        if params_dict.get(param):
            scenarios_tmp['parameters'][param] = params_dict[param]

    for stage in ('setup', 'test', 'teardown'):
        if scenario_dict.get(stage):
            scenarios_tmp[stage] = []
            for step in scenario_dict[stage]:
                scenarios_tmp[stage].append({step: params_dict[step]})

    dump_file = os.path.join(target_dir, 'config', 'poc-scenario.json')
    print("Writing scenarios information to %s" % dump_file)
    with open(dump_file, 'w') as f:
        json.dump(scenarios_tmp, f, indent=2)
