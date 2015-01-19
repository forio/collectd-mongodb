#!/usr/bin/env python
#
# Plugin to collectd statistics from MongoDB
#

import collectd
import pymongo
from pymongo import Connection
from distutils.version import StrictVersion as V

def set_read_preference(db):
    if pymongo.version >= "2.1":
        db.read_preference = pymongo.ReadPreference.SECONDARY

def mongo_connect(host=None, port=None, ssl=False, user=None, passwd=None, replica=None):
    try:
        # ssl connection for pymongo > 2.3
        if pymongo.version >= "2.3":
            if replica is None:
                con = pymongo.MongoClient(host, port)
            else:
                con = pymongo.Connection(host, port, read_preference=pymongo.ReadPreference.SECONDARY, ssl=ssl, replicaSet=replica, network_timeout=10)
        else:
            if replica is None:
                con = pymongo.Connection(host, port, slave_okay=True, network_timeout=10)
            else:
                con = pymongo.Connection(host, port, slave_okay=True, network_timeout=10)
                #con = pymongo.Connection(host, port, slave_okay=True, replicaSet=replica, network_timeout=10)

        if user and passwd:
            db = con["admin"]
            if not db.authenticate(user, passwd):
                sys.exit("Username/Password incorrect")
    except Exception, e:
        if isinstance(e, pymongo.errors.AutoReconnect) and str(e).find(" is an arbiter") != -1:
            # We got a pymongo AutoReconnect exception that tells us we connected to an Arbiter Server
            # This means: Arbiter is reachable and can answer requests/votes - this is all we need to know from an arbiter
            print "OK - State: 7 (Arbiter)"
            sys.exit(0)
        return exit_with_general_critical(e), None
    return 0, con

def replication_get_time_diff(con):
    col = 'oplog.rs'
    local = con.local
    ol = local.system.namespaces.find_one({"name": "local.oplog.$main"})
    if ol:
        col = 'oplog.$main'
    firstc = local[col].find().sort("$natural", 1).limit(1)
    lastc = local[col].find().sort("$natural", -1).limit(1)
    first = firstc.next()
    last = lastc.next()
    tfirst = first["ts"]
    tlast = last["ts"]
    delta = tlast.time - tfirst.time
    return delta


class MongoDB(object):

    def __init__(self):
        self.plugin_name = "mongo"
        self.servers = []


    def submit(self, conf, type, instance, value, db=None):
        if db:
            plugin_instance = '%s-%s' % (conf['instance'] if conf['instance'] else conf['mongo_port'], db)
        else:
            plugin_instance = str(conf['instance'] if conf['instance'] else conf['mongo_port'])
        v = collectd.Values()
        v.plugin = conf['plugin_name']
        v.plugin_instance = plugin_instance
        v.type = type
        v.type_instance = instance
        v.values = [value, ]
        v.dispatch()


    def do_server_status(self, conf):
        host = conf['mongo_host']
        port = conf['mongo_port']
        user = conf['mongo_user']
        passwd = conf['mongo_password']
        perf_data = False
        con = Connection(host=host, port=port, slave_okay=True)
        if not conf['mongo_db']:
            conf['mongo_db'] = con.database_names()
        db = con[conf['mongo_db'][0]]
        if user and passwd:
            db.authenticate(user, passwd)
        server_status = db.command('serverStatus')

        version = server_status['version']
        at_least_2_4 = V(version) >= V('2.4.0')

        # operations
        for k, v in server_status['opcounters'].items():
            self.submit(conf, 'total_operations', k, v)

        # memory
        for t in ['resident', 'virtual', 'mapped']:
            self.submit(conf, 'memory', t, server_status['mem'][t])

        # connections
        self.submit(conf, 'connections', 'connections', server_status['connections']['current'])

        # locks
        if conf['lockTotalTime'] is not None and conf['lockTime'] is not None:
            if conf['lockTime'] == server_status['globalLock']['lockTime']:
                value = 0.0
            else:
                value = float(server_status['globalLock']['lockTime'] - conf['lockTime']) * 100.0 / float(server_status['globalLock']['totalTime'] - conf['lockTotalTime'])
            self.submit(conf, 'percent', 'global_lock', value)

        # TODO: Check to see if this is broken - probably need to save to the object's conf array rather than the local copy
        conf['lockTotalTime'] = server_status['globalLock']['totalTime']
        conf['lockTime'] = server_status['globalLock']['lockTime']

        self.submit(conf, 'percent', 'index_missing', server_status['indexCounters']['missRatio'])

        for mongo_db in conf['mongo_db']:
            db = con[mongo_db]
            if user and passwd:
                db.authenticate(user, passwd)
            db_stats = db.command('dbstats')

            # stats counts
            self.submit(conf, 'gauge', 'object_count', db_stats['objects'], mongo_db)
            self.submit(conf, 'gauge', 'collections', db_stats['collections'], mongo_db)
            self.submit(conf, 'gauge', 'num_extents', db_stats['numExtents'], mongo_db)
            self.submit(conf, 'gauge', 'indexes', db_stats['indexes'], mongo_db)

            # stats sizes
            self.submit(conf, 'file_size', 'storage', db_stats['storageSize'], mongo_db)
            self.submit(conf, 'file_size', 'index', db_stats['indexSize'], mongo_db)
            self.submit(conf, 'file_size', 'data', db_stats['dataSize'], mongo_db)

        # Replica check

        rs_status = {}
        slaveDelays = {}
        try:
            # Get replica set status
            try:
                rs_status = con.admin.command("replSetGetStatus")
            except pymongo.errors.OperationFailure, e:
                if e.code == None and str(e).find('failed: not running with --replSet"'):
                    print "OK - Not running with replSet"
                    con.disconnect()
                    return 0

            rs_conf = con.local.system.replset.find_one()
            for member in rs_conf['members']:
                if member.get('slaveDelay') is not None:
                    slaveDelays[member['host']] = member.get('slaveDelay')
                else:
                    slaveDelays[member['host']] = 0

            # Find the primary and/or the current node
            primary_node = None
            host_node = None

            for member in rs_status["members"]:
                if member["stateStr"] == "PRIMARY":
                    primary_node = member
                if member["name"].split(':')[0] == host and int(member["name"].split(':')[1]) == port:
                    host_node = member

            # Check if we're in the middle of an election and don't have a primary
            if primary_node is None:
                print "WARNING - No primary defined. In an election?"
                con.disconnect()
                return 1

            # Check if we failed to find the current host
            # below should never happen
            if host_node is None:
                print "CRITICAL - Unable to find host '" + host + "' in replica set."
                con.disconnect()
                return 2
            # Is the specified host the primary?
            if host_node["stateStr"] == "PRIMARY":
                if max_lag == False:
                    print "OK - This is the primary."
                    con.disconnect()
                    return 0
                else:
                    #get the maximal replication lag
                    data = ""
                    maximal_lag = 0
                    for member in rs_status['members']:
                        if not member['stateStr'] == "ARBITER":
                            lastSlaveOpTime = member['optimeDate']
                            replicationLag = abs(primary_node["optimeDate"] - lastSlaveOpTime).seconds - slaveDelays[member['name']]
                            data = data + member['name'] + " lag=%d;" % replicationLag
                            maximal_lag = max(maximal_lag, replicationLag)

                    self.submit(conf, 'time_offset', 'maximal-lag-seconds', str(maximal_lag))

                    # send maximal lag in percentage
                    err, con = mongo_connect(primary_node['name'].split(':')[0], int(primary_node['name'].split(':')[1]), False, user, passwd)
                    if err != 0:
                        con.disconnect()
                        return err
                    primary_timediff = replication_get_time_diff(con)
                    maximal_lag = int(float(maximal_lag) / float(primary_timediff) * 100)
                    self.submit(conf, 'time_offset', 'maximal-lag-percentage', str(maximal_lag))
                    con.disconnect()
                    return str(maximal_lag)
            elif host_node["stateStr"] == "ARBITER":
                print "OK - This is an arbiter"
                con.disconnect()
                return 0

            # Find the difference in optime between current node and PRIMARY

            optime_lag = abs(primary_node["optimeDate"] - host_node["optimeDate"])
            if host_node['name'] in slaveDelays:
                slave_delay = slaveDelays[host_node['name']]
            elif host_node['name'].endswith(':27017') and host_node['name'][:-len(":27017")] in slaveDelays:
                slave_delay = slaveDelays[host_node['name'][:-len(":27017")]]
            else:
                raise Exception("Unable to determine slave delay for {0}".format(host_node['name']))

            try:  # work starting from python2.7
                lag = optime_lag.total_seconds()
            except:
                lag = float(optime_lag.seconds + optime_lag.days * 24 * 3600)

            self.submit(conf, 'time_offset', 'lag-seconds', str(lag))

            # send message with lag in percentage
            err, con = mongo_connect(primary_node['name'].split(':')[0], int(primary_node['name'].split(':')[1]), False, user, passwd)
            if err != 0:
                con.disconnect()
                return err
            primary_timediff = replication_get_time_diff(con)
            if primary_timediff != 0:
                lag = int(float(lag) / float(primary_timediff) * 100)
            else:
                lag = 0
            self.submit(conf, 'percent', 'lag-percentage', str(lag))
            con.disconnect()
            return str(lag)
            #return check_levels(lag, warning + slaveDelays[host_node['name']], critical + slaveDelays[host_node['name']], message)

        except Exception, e:
            con.disconnect()
            return e


    def config(self, obj):
        instance = None
        plugin_name = 'mongodb'
        mongo_port = None
        mongo_host = None
        mongo_user = None
        mongo_password = None
        mongo_db = None

        for node in obj.children:

            if node.key == 'Instance':
                instance = node.values[0]
            elif node.key == 'Name':
                plugin_name = node.values[0]
            elif node.key == 'Port':
                mongo_port = int(node.values[0])
            elif node.key == 'Host':
                mongo_host = node.values[0]
            elif node.key == 'User':
                mongo_user = node.values[0]
            elif node.key == 'Password':
                mongo_password = node.values[0]
            elif node.key == 'Database':
                mongo_db = node.values
            else:
                collectd.warning("mongodb plugin: Unkown configuration key %s" % node.key)
                continue
        self.servers.append({
            'mongo_host': mongo_host,
            'mongo_port': mongo_port,
            'mongo_user': mongo_user,
            'mongo_password': mongo_password,
            'mongo_db': mongo_db,
            'instance': instance,
            'plugin_name': plugin_name,
            'lockTime': None,
            'lockTotalTime': None,
        })

    def read(self):
        for conf in self.servers:
            self.do_server_status(conf)


mongodb = MongoDB()
collectd.register_read(mongodb.read)
collectd.register_config(mongodb.config)
