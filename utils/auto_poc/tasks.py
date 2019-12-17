#!/usr/bin/env python3
from subprocess import run, PIPE, STDOUT
import os


def task_start(main_params, poc_dir):
    args = []
    params_tmp = main_params.copy()
    script = params_tmp['script']
    params_tmp.pop('script', None)

    for k in params_tmp.keys():
        if not params_tmp[k] or str(params_tmp[k]) == 'None':
            continue
        if str(params_tmp[k]) == 'empty':
            args.append("--%s" % str(k))
        else:
            args.append("--%s" % k)
            args.append(str(params_tmp[k]))
    args = [os.path.join(poc_dir, script)] + args

    print("Command: %s" % ' '.join(args))
    proc = run(args, stdout=PIPE, stderr=STDOUT, encoding='utf-8', check=True)
    proc_stdout_list = proc.stdout.split('\n')
    print(" ------------> Command output: ---------------->")
    for line in proc_stdout_list:
        print(line)
    print(" <------------ Command output: <----------------")
    print("")

    return proc_stdout_list
