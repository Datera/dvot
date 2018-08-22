# dvot
Datera Volume Operations Tool

## What For?

This is meant to ease several multi-step end-user operations for the Datera
system that encompass both Datera-side operations and client-side operations.
Most of the operations supported in this tool involve snapshots.

## What Do?

* Install prerequistes
    - Install ``python``
    - Install ``git``
    - Install ``open-iscsi`` (we need ``iscsiadm``)
    - Install ``multipath-tools`` (or whatever it is on your system)
    - Install ``mkfs.<your_favorite_format>``
    - Install ``fio`` (make sure it's accessible via $PATH)
* Clone the repository
    - ``git clone http://github.com/Datera/dvot``
    - ``cd dvot``
* Install
    - ``./install.py``
* Create Datera Universal Config File
    - ``vi datera-config.json``
    ```json
    {
        "username": "your_user",
        "password": "your_password",
        "tenant": "/root",
        "mgmt_ip": "1.1.1.1",
        "api_version": "2.2"
    }
    ```
* Use
    - ``./dvot --help``

## What Really Do?

Functionality can be broken down into a few categories

* Health Check
* Creating Snapshots
* Finding Resources
* Mounting Snapshots
* Restoring Snapshots

### Health Check

The health check is very basic.  It checks that the tool has API access and
then pings the Access IPs from the client.

```bash
$ ./dvot health-check
Health Check Completed Successfully
```

## Creating Snapshots

```bash
./dvot make-snap --id <my-vol-or-ai-id>
```
or
```bash
./dvot make-snap --name <my-vol-or-ai-name>
```
WARNING: Only use ``--name`` with a guaranteed unique name.  Use the UUID of
the Volume/AppInstance if unsure if the name is unique.

If a Volume name/id is given, then a Snapshot of that Volume will be created
individually

If an AppInstance name/id is given, then a Snapshot will be created of the
entire AppInstance (and all included Volumes)

## Finding Resources

### Volumes

```bash
./dvot find-vol --id <my-vol-id>
```
```bash
./dvot find-from-mount --path /mnt/my-mounted-volume
```
```bash
./dvot find-from-device-path --path /dev/my-device
```

### AppInstances

```bash
./dvot find-app --name <my-app-name>
```
```bash
./dvot find-app --id <my-app-id>
```

### Snapshots

```bash
./dvot find-snap --id <my-snap-uuid>
```
```bash
./dvot find-snap --name <my-snap-timestamp>
```

## Mounting Snapshots

Snapshots that are requested for mounting will first be cloned into a new
AppInstance, then all volumes within that AppInstance (one for a single-Volume
Snapshot or multiple for an AppInstance Snapshot) will be logged-in/mounted

### Mounting a single snapshot
```bash
./dvot find-snap --id <my-snap-uuid> --login
```
```bash
./dvot find-snap --id <my-snap-uuid> --mount --fstype xfs --directory /mnt
```

### Mounting all snapshots in an AppInstance

```bash
./dvot find-app --id <my-snap-uuid> --mount --all-snaps
```

## Restoring Snapshots

### Restoring an Unmounted Volume or AppInstance

```bash
./dvot rollback --id <snap-uuid>
```
```bash
./dvot rollback --name <snap-timestamp>
```

### Restoring a Mounted Volume or AppInstance

In this case mounts will be unmounted, the device will be logged out a rollback
of the Volume/AppInstance is completed, then the device is logged back in
and remounted (if it started as a mount)

```bash
./dvot rollback --name <snap-ts> --path <mount-or-device-path>
```
