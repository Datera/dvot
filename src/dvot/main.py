#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function, division

import argparse
import re
import sys
import textwrap
import threading
import time
import uuid
try:
    import queue
except ImportError:
    import Queue as queue

from dfs_sdk import scaffold
from dfs_sdk import exceptions as dexceptions
from dvot.utils import exe, Parallel
from dvot.mount import mount_volumes, clean_mounts

SUCCESS = 0
FAILURE = 1
MAX_WORKERS = 20


VOL_SNAP_RE = re.compile(
    r"/app_instances/(?P<ai>.*)/storage_instances/"
    r"(?P<si>.*)/volumes/(?P<vol>.*)/snapshots/(?P<ts>.*)")
AI_SNAP_RE = re.compile(r"/app_instances/(?P<ai>.*)/snapshots/(?P<ts>.*)")
IQN_RE = re.compile(r"(?P<iqn>iqn.2013-\d\d\.com\.daterainc:tc:\d\d:\w\w:"
                    r"[a-f0-9]+)-lun-(?P<lun>\d+)")


def hf(txt):
    return textwrap.fill(txt)


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


def _find_impl(api, func, args):
    found = []
    q = queue.Queue()
    for ai in api.app_instances.list():
        q.put(ai)
    workers = max(q.qsize(), MAX_WORKERS)
    for _ in range(workers):
        thread = threading.Thread(target=func, args=(q, found, args))
        thread.daemon = True
        thread.start()
    while not (q.unfinished_tasks == 0 or len(found) > 0):
        time.sleep(0.2)
    if found:
        return found[0]


def find_si(api, iqn):
    def _si_helper(q, found, args):
        iqn = args[0]
        while len(found) == 0:
            ai = q.get()
            for si in ai.storage_instances.list():
                if si.access['iqn'] == iqn:
                    found.append(si)
            q.task_done()
    return _find_impl(api, _si_helper, [iqn])


def find_vol(api, name, oid):
    if (name and oid) or (not name and not oid):
        raise ValueError("Either --name or --id MUST be provided")

    def _vol_helper(q, found, args):
        oid, name = args
        while len(found) == 0:
            ai = q.get()
            for si in ai.storage_instances.list():
                for vol in si.volumes.list():
                    if vol['uuid'] == oid or vol['name'] == name:
                        found.append(vol)
            q.task_done()
    return _find_impl(api, _vol_helper, (oid, name))


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


def find_snaps(api, name, oid):
    if (name and oid):
        raise ValueError("Only one of --name or --id can be provided")

    def _snap_helper(ai, vid, app_snaps, vol_snaps):
        if not vid:
            app_snaps.extend(ai.snapshots.list())
        for si in ai.storage_instances.list():
            for vol in si.volumes.list():
                if vid:
                    if vol.uuid == vid or vol.name == vid:
                        vol_snaps.extend(vol.snapshots.list())
                else:
                    vol_snaps.extend(vol.snapshots.list())
    oid = name if name else oid
    app_snaps, vol_snaps = [], []
    found = None
    vid = None
    if oid:
        try:
            found = api.app_instances.get(oid)
            _snap_helper(found, None, app_snaps, vol_snaps)
            return app_snaps, vol_snaps
        except dexceptions.ApiNotFoundError:
            vid = oid
    args_list = [(ai, vid, app_snaps, vol_snaps)
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


def rollback(api, name, oid, path, directory, multipath, fstype, fsargs,
             login):
    if not any((name, oid)) and not all((name, oid)):
        raise ValueError(
            "One of --name, --id of the snapshot MUST be provided")
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
        if path:
            clean_mounts(api, [ai], directory, 1)
        ai.set(admin_state='offline', force=True)
        si = ai.storage_instances.get(si_id)
        vol = si.volumes.get(vol_id)
        vol.set(restore_point=ts)
        ai.set(admin_state='online')
        _obj_poll(si)
        if path:
            mount_volumes(api, [ai], multipath, fstype, fsargs, directory,
                          1, login)
    else:
        match = AI_SNAP_RE.match(path)
        ai_id = match.group('ai')
        ts = match.group('ts')
        ai = api.app_instances.get(ai_id)
        if path:
            clean_mounts(api, [ai], directory, 1)
        ai.set(admin_state='offline', force=True)
        ai.set(restore_point=ts)
        ai.set(admin_state='online')
        if path:
            mount_volumes(api, [ai], multipath, fstype, fsargs, directory,
                          1, login)
        # Nothing to poll on AppInstance level snapshots


def new_app_from_snap(api, snap):
    print("Creating new AppInstance:", snap.path)
    name = 'from-snap-{}-{}'.format(snap['utc_ts'], str(uuid.uuid4())[:8])
    return api.app_instances.create(name=name,
                                    clone_snapshot_src={'path': snap.path})


def find_from_mount(api, mount):
    device = exe("df -P {} | tail -1 | cut -d' ' -f 1".format(mount)).strip()
    if not device:
        print("No device found for mount:", mount)
    return find_from_device_path(api, device)


def find_from_device_path(api, device_path):
    iqn, lun = iqn_lun_from_device(device_path)
    si = find_si(api, iqn)
    if not si:
        print("No StorageInstance found for device path", device_path)
        return
    return si.volumes.list()[lun]


def iqn_lun_from_device(device):
    links = exe("udevadm info --query=symlink --name={}".format(
        device)).split()
    links = filter(lambda x: 'by-path' in x, links)
    if len(links) == 0:
        print("No /dev/disk/by-path link found for device:", device)
        return None, None
    link = links[0]
    match = IQN_RE.search(link)
    if not match:
        print("No iqn found in link:", link)
        return None, None
    return match.group('iqn'), int(match.group('lun'))


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


def print_snaps(app_snaps, vol_snaps):
    print("App Snaps")
    print("=========")
    for snap in app_snaps:
        print(snap.path, snap.op_state)
    print("\nVol Snaps")
    print("=========")
    for snap in vol_snaps:
        print(snap.path, snap.op_state)


def print_pretty_snaps(api, app_snaps, vol_snaps):
    def _psnap_helper(api, snap, results):
        path = snap.path
        match = VOL_SNAP_RE.match(path)
        if match:
            ai_id = match.group('ai')
            si_id = match.group('si')
            vol_id = match.group('vol')
            ts = match.group('ts')
            ai = api.app_instances.get(ai_id)
            si = ai.storage_instances.get(si_id)
            vol = si.volumes.get(vol_id)
            s = '{} -- {} -- {} -- {}'.format(
                ai.name, si.name, vol.name, ts)
            results[1].append(s)
        else:
            match = AI_SNAP_RE.match(path)
            ai_id = match.group('ai')
            ts = match.group('ts')
            ai = api.app_instances.get(ai_id)
            s = '{} -- {}'.format(ai.name, ts)
            results[0].append(s)
    results = [[], []]
    funcs = [_psnap_helper] * (len(app_snaps) + len(vol_snaps))
    sn = app_snaps + vol_snaps
    args_list = [(api, snap, results) for snap in sn]
    p = Parallel(funcs, args_list=args_list,
                 max_workers=max(len(funcs), MAX_WORKERS))
    p.run_threads()
    na, nv = results
    print("App Snaps")
    print("=========")
    for snap in sorted(na):
        print(snap)
    print("\nVol Snaps")
    print("=========")
    for snap in sorted(nv):
        print(snap)


def main(args):
    api = scaffold.get_api()
    print('Using Config:')
    scaffold.print_config()

    found = None
    if args.op == 'health-check':
        run_health(api)
    elif args.op == 'list-snaps':
        app_snaps, vol_snaps = find_snaps(api, args.name, args.id)
        print_snaps(app_snaps, vol_snaps)
    elif args.op == 'list-snaps-pretty':
        app_snaps, vol_snaps = find_snaps(api, args.name, args.id)
        print_pretty_snaps(api, app_snaps, vol_snaps)
    elif args.op == 'make-snap':
        found = make_snap(api, args.name, args.id)
        if found:
            print("Created snapshot:", found.path)
        else:
            print("No AppInstance or Volume found with name {} or id {}"
                  "".format(args.name, args.id))
            return FAILURE
    elif args.op == 'rollback':
        rollback(api, args.name, args.id, args.path, args.directory,
                 not args.no_multipath, args.fstype, args.fsargs, args.login)
        return SUCCESS
    elif args.op == 'find-vol':
        found = find_vol(api, args.name, args.id)
        if found:
            print("Found volume:", found['name'])
            print("=============")
            print(found)
        else:
            print("No volume found matching name {} or id {}".format(
                args.name, args.id))
            return FAILURE
    elif args.op == 'find-app':
        found = find_app(api, args.name, args.id)
        if found:
            print("Found AppInstance:", found['name'])
            print("=============")
            print(found)
        else:
            print("No AppInstance found matching name {} or id {}".format(
                args.name, args.id))
            return FAILURE
    elif args.op == 'find-snap':
        found = find_snap(api, args.id)
        if found:
            print("Found Snapshot:", args.id)
            print("=============")
            print(found)
        else:
            print("No Snapshot found matching name {} or id {}".format(
                args.name, args.id))
            return FAILURE
    elif args.op == 'find-from-mount':
        if not args.path:
            raise ValueError("find-from-mount requires --path argument")
        found = find_from_mount(api, args.path)
        print("Found Volume:", found['name'])
        print("============")
    elif args.op == 'find-from-device-path':
        if not args.path:
            raise ValueError("find-from-device-path requires --path argument")
        found = find_from_device_path(args.path)
        print("Found Volume:", found['name'])
        print("============")
        print(found)

    if (args.mount or args.login) and found:
        ais = []
        if hasattr(found, 'utc_ts'):
            ai = new_app_from_snap(api, found)
        else:
            ai = api.app_instances.get(found.path.split('/')[2])
            if args.all_snaps:
                app_snaps, vol_snaps = find_snaps(api, None, ai.id)
                for snap in app_snaps + vol_snaps:
                    ais.append(new_app_from_snap(api, snap))
            else:
                ais.append(ai)
        mount_volumes(api, ais, not args.no_multipath, args.fstype,
                      args.fsargs, args.directory, 1, args.login)
    elif args.clean and found:
        if hasattr(found, 'utc_ts'):
            ai = new_app_from_snap(api, found)
        else:
            ai = api.app_instances.get(found.path.split('/')[2])
        clean_mounts(api, [ai], args.directory, 1)

    return SUCCESS


if __name__ == '__main__':
    tparser = scaffold.get_argparser(add_help=False)
    parser = argparse.ArgumentParser(
        parents=[tparser], formatter_class=argparse.RawTextHelpFormatter)
    op_help = """Operation to perform
* health-check
    basic health check to ensure everything is functional
* make-snap
    create a Snapshot for the specified Volume or AppInstance
* list-snaps
    list all Snapshots available to the current tenant
* list-snaps-pretty
    prettier output for list-snaps. Might take a long time
* find-vol
    finds a Volume with the specified name or id
* find-app
    find an AppInstance with the specified name or id
* find-from-mount
    find a Volume from the specified mount path
* find-from-device-path
    same as find-from-mount but with device-path
* rollback
    rollback a volume to a snapshot.
    """
    parser.add_argument('op', choices=('health-check',
                                       'make-snap',
                                       'list-snaps',
                                       'list-snaps-pretty',
                                       'find-vol',
                                       'find-app',
                                       'find-snap',
                                       'find-from-mount',
                                       'find-from-device-path',
                                       'rollback'), help=op_help)
    parser.add_argument('--name')
    parser.add_argument('--id')
    parser.add_argument('--path')
    parser.add_argument('--no-multipath', action='store_true')
    parser.add_argument('--login', action='store_true',
                        help='Login volumes (implied by --mount)')
    parser.add_argument('--logout', action='store_true',
                        help='Logout volumes (implied by --unmount)')
    parser.add_argument('--mount', action='store_true',
                        help='Mount volumes, (implies --login)')
    parser.add_argument('--unmount', action='store_true',
                        help='Unmount volumes only.  Does not delete volume')
    parser.add_argument('--clean', action='store_true',
                        help='Deletes volumes (implies --unmount and '
                             '--logout)')
    parser.add_argument('--fstype', default='xfs',
                        help='Filesystem to use when formatting devices')
    parser.add_argument('--fsargs', default='',
                        help=hf('Extra args to give formatter, eg "-E '
                                'lazy_table_init=1".  Make sure fstype matches'
                                ' the args you are passing in'))
    parser.add_argument('--directory', default='/mnt',
                        help='Directory under which to mount devices')
    parser.add_argument('--all-snaps', action='store_true',
                        help='For use with --mount/--login')

    args = parser.parse_args()
    sys.exit(main(args))
