#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function, division

import argparse
import re
import sys
import threading
import time
try:
    import queue
except ImportError:
    import Queue as queue

from dfs_sdk import scaffold
from dvot.utils import exe, Parallel

SUCCESS = 0
FAILURE = 1
MAX_WORKERS = 20


VOL_SNAP_RE = re.compile("/app_instances/(?P<ai>.*)/storage_instances/"
                         "(?P<si>.*)/volumes/(?P<vol>.*)/snapshots/(?P<ts>.*)")
AI_SNAP_RE = re.compile("/app_instances/(?P<ai>.*)/snapshots/(?P<ts>.*)")


def run_health(api):
    config = scaffold.get_config()
    try:
        exe('ping -c 1 -w 1 {}'.format(config['mgmt_ip']))
    except EnvironmentError:
        print('Could not ping mgmt_ip:', config['mgmt_ip'])
        return False
    try:
        api.app_instances.list()
    except Exception as e:
        print("Could not connect to cluster", e)
        return False
    npass = True
    av = api.system.network.access_vip.get()
    for np in av['network_paths']:
        ip = np.get('ip')
        if ip:
            try:
                exe('ping -c 1 -w 1 {}'.format(ip))
            except EnvironmentError:
                print('Could not ping: {} {}'.format(np.get('name'), ip))
                npass = False
    if not npass:
        return False
    print("Health Check Completed Successfully")
    return True


def find_vol(api, name, oid):
    if (name and oid) or (not name and not oid):
        raise ValueError("Either --name or --id MUST be provided")

    def _vol_helper(q, found):
        while len(found) == 0:
            ai = q.get()
            for si in ai.storage_instances.list():
                for vol in si.volumes.list():
                    if vol['uuid'] == oid or vol['name'] == name:
                        found.append(vol)
            q.task_done()
    found = []
    q = queue.Queue()
    for ai in api.app_instances.list():
        q.put(ai)
    workers = max(q.qsize(), MAX_WORKERS)
    for _ in range(workers):
        thread = threading.Thread(target=_vol_helper, args=(q, found))
        thread.daemon = True
        thread.start()
    while not (q.unfinished_tasks == 0 or len(found) > 0):
        time.sleep(0.2)
    if found:
        return found[0]


def find_snap(api, ts):
    if not ts:
        raise ValueError("You must specify --id when using find-snap")

    def _snap_helper(q, found):
        while len(found) == 0:
            ai = q.get()
            for snap in ai.snapshots.list():
                if snap['utc_ts'] == ts or snap['uuid'] == ts:
                    found.append(snap)
                    q.task_done()
                    return
            for si in ai.storage_instances.list():
                for vol in si.volumes.list():
                    for snap in vol.snapshots.list():
                        if snap['utc_ts'] == ts or snap['uuid'] == ts:
                            found.append(snap)
            q.task_done()
    found = []
    q = queue.Queue()
    for ai in api.app_instances.list():
        q.put(ai)
    workers = max(q.qsize(), MAX_WORKERS)
    for _ in range(workers):
        thread = threading.Thread(target=_snap_helper, args=(q, found))
        thread.daemon = True
        thread.start()
    while not (q.unfinished_tasks == 0 or len(found) > 0):
        time.sleep(0.2)
    if found:
        return found[0]


def find_app(api, name, oid):
    if (name and oid) or (not name and not oid):
        raise ValueError("Either --name or --id MUST be provided")
    for ai in api.app_instances.list():
        if ai.name == name or ai.id == oid:
            return ai


def find_snaps(api):
    def _snap_helper(ai, app_snaps, vol_snaps):
        app_snaps.extend(ai.snapshots.list())
        for si in ai.storage_instances.list():
            for vol in si.volumes.list():
                vol_snaps.extend(vol.snapshots.list())
    app_snaps, vol_snaps = [], []
    args_list = [(ai, app_snaps, vol_snaps)
                 for ai in api.app_instances.list()]
    funcs = [_snap_helper] * len(args_list)
    p = Parallel(funcs,
                 args_list=args_list,
                 max_workers=max(len(funcs), MAX_WORKERS))
    p.run_threads()
    return app_snaps, vol_snaps


def make_snap(api, name, oid):
    if (name and oid) or (not name and not oid):
        raise ValueError("Either --name or --id MUST be provided")
    ai = find_app(api, name, oid)
    if ai:
        return ai.snapshots.create()
    vol = find_vol(api, name, oid)
    if vol:
        return vol.snapshots.create()


def restore(api, name, oid):
    if (name and oid) or (not name and not oid):
        raise ValueError("Either --name or --id MUST be provided")
    oid = name if not oid else oid
    snap = find_snap(api, oid)
    if not snap:
        print("No Snapshot found matching name {} or id {}".format(name, oid))
        return
    path = snap.path
    match = VOL_SNAP_RE.match(path)
    print("Restoring:", snap.path)
    if match:
        ai_id = match.group('ai')
        si_id = match.group('si')
        vol_id = match.group('vol')
        ts = match.group('ts')
        ai = api.app_instances.get(ai_id)
        ai.set(admin_state='offline')
        si = ai.storage_instances.get(si_id)
        vol = si.volumes.get(vol_id)
        vol.set(restore_point=ts)
        ai.set(admin_state='online')
        _obj_poll(si)
    else:
        match = AI_SNAP_RE.match(path)
        ai_id = match.group('ai')
        ts = match.group('ts')
        ai = api.app_instances.get(ai_id)
        ai.set(admin_state='offline')
        ai.set(restore_point=ts)
        ai.set(admin_state='online')
        # Nothing to poll on AppInstance level snapshots


def _obj_poll(obj):
    timeout = 10
    while True:
        obj = obj.reload()
        if obj['op_state'] == 'available':
            break
        if not timeout:
            raise EnvironmentError(
                "Polling ended before object {} was still "
                "unavailable".format(obj.path))
        time.sleep(1)
        timeout -= 1


def main(args):
    api = scaffold.get_api()
    print('Using Config:')
    scaffold.print_config()

    if args.op == 'health-check':
        run_health(api)
    elif args.op == 'list-snaps':
        app_snaps, vol_snaps = find_snaps(api)
        print("App Snaps")
        print("=========")
        for snap in app_snaps:
            print(snap.path, snap.op_state)
        print("Vol Snaps")
        print("=========")
        for snap in vol_snaps:
            print(snap.path, snap.op_state)
    elif args.op == 'make-snap':
        snap = make_snap(api, args.name, args.id)
        if snap:
            print("Created snapshot:", snap.path)
        else:
            print("No AppInstance or Volume found with name {} or id {}"
                  "".format(args.name, args.id))
            return FAILURE
    elif args.op == 'restore':
        restore(api, args.name, args.id)
    elif args.op == 'find-vol':
        vol = find_vol(api, args.name, args.id)
        if vol:
            print("Found volume:", vol['name'])
            print("=============")
            print(vol)
        else:
            print("No volume found matching name {} or id {}".format(
                args.name, args.id))
            return FAILURE
    elif args.op == 'find-app':
        ai = find_app(api, args.name, args.id)
        if ai:
            print("Found AppInstance:", ai['name'])
            print("=============")
            print(ai)
        else:
            print("No AppInstance found matching name {} or id {}".format(
                args.name, args.id))
            return FAILURE
    elif args.op == 'find-snap':
        snap = find_snap(api, args.id)
        if snap:
            print("Found Snapshot:", args.id)
            print("=============")
            print(snap)
        else:
            print("No Snapshot found matching name {} or id {}".format(
                args.name, args.id))
            return FAILURE

    return SUCCESS


if __name__ == '__main__':
    tparser = scaffold.get_argparser(add_help=False)
    parser = argparse.ArgumentParser(
        parents=[tparser], formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('op', choices=('health-check',
                                       'make-snap',
                                       'list-snaps',
                                       'find-vol',
                                       'find-app',
                                       'find-snap',
                                       'restore'))
    parser.add_argument('--name')
    parser.add_argument('--id')
    parser.add_argument('--no-multipath')

    args = parser.parse_args()
    sys.exit(main(args))
