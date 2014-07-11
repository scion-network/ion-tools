#!/usr/bin/env python

"""Tool to diagnose issues with an OOINet system instance"""

__author__ = 'Michael Meisinger'

import argparse
import datetime
import json
import pprint
import os
import shutil
import time
import threading
import yaml
import sys
import Queue

SERVICE_NAMES = ['agent_management', 'catalog_management', 'conversation_management', 'data_acquisition_management',
    'data_process_management', 'data_product_management', 'data_retriever', 'dataset_management', 'directory',
    'discovery', 'event_management', 'exchange_management', 'identity_management', 'index_management',
    'ingestion_management', 'instrument_management', 'object_management', 'observatory_management', 'org_management',
    'policy_management', 'preservation_management', 'process_dispatcher', 'provisioner', 'pubsub_management',
    'resource_management', 'resource_registry', 'scheduler', 'service_gateway', 'service_management',
    'system_management', 'user_notification', 'visualization', 'workflow_management']


class IonDiagnose(object):

    def __init__(self):
        self.sysinfo = {}
        self.msgs = []

    def init_args(self):
        print "====================================================="
        print "OOINet iondiag -- Diagnostics and Operations Analysis"
        print "====================================================="
        parser = argparse.ArgumentParser(description="OOINet iondiag")
        parser.add_argument('-c', '--config', type=str, help='File path to config file', default="")
        parser.add_argument('-d', '--info_dir', type=str, help='System info directory')
        parser.add_argument('-l', '--load_info', action='store_true', help="Load from system info directory")
        parser.add_argument('-L', '--level', type=str, help='Minimum warning level', default="WARN")
        parser.add_argument('-n', '--no_save', action='store_true', help="Don't store system info")
        parser.add_argument('-i', '--interactive', action='store_true', help="Drop into interactive shell")
        parser.add_argument('-q', '--quick', action='store_true', help="Quick mode - only retrieve basic info")
        parser.add_argument('-v', '--verbose', action='store_true', help="Verbose output")
        parser.add_argument('-R', '--only_retrieve', type=str, help='Restict retrieve to D, R, C', default="rdc")
        parser.add_argument('-O', '--only_do', type=str, help='Restict diag to D, R, C', default="rdc")
        parser.add_argument('-H', '--hide_prefix', type=str, help='Hide warning prefix', default="")
        self.opts, self.extra = parser.parse_known_args()

    def read_config(self, filename=None):
        if self.opts.config:
            filename = self.opts.config
        else:
            filename = "iondiag.cfg"
        print "Loading config from %s" % filename
        self.cfg = None
        if filename and os.path.exists(filename):
            with open(filename, "r") as f:
                cfg_str = f.read()
            self.cfg = yaml.load(cfg_str)
        if not self.cfg:
            self._errout("No config")
        self.sysname = self.cfg["system"]["name"]
        self._hide_warn_pre_list = []
        if self.opts.hide_prefix:
            self._hide_warn_pre_list = self.opts.hide_prefix.split(",")

    # -------------------------------------------------------------------------

    def get_system_info(self):
        print "Retrieving system information from operational servers"
        # Read rabbit
        if "R" in self.opts.only_retrieve.upper():
            self._get_rabbit_info()

        # Read resources from postgres
        if "D" in self.opts.only_retrieve.upper():
            self._get_db_info()

        # Read info from CEIctrl
        if "C" in self.opts.only_retrieve.upper():
            self._get_cei_info()

    def _get_rabbit_info(self):
        import requests
        from requests.auth import HTTPBasicAuth
        mgmt = self.cfg["container"]["exchange"]["management"]
        url_prefix = "http://%s:%s" % (mgmt["host"], mgmt["port"])
        print " Getting RabbitMQ info from %s" % url_prefix
        rabbit_info = self.sysinfo.setdefault("rabbit", {})
        url1 = url_prefix + "/api/overview"
        resp = requests.get(url1, auth=HTTPBasicAuth(mgmt["username"], mgmt["password"]))
        rabbit_info["overview"] = resp.json()
        print "  ...retrieved %s overview entries" % (len(rabbit_info["overview"]))

        url2 = url_prefix + "/api/queues"
        resp = requests.get(url2, auth=HTTPBasicAuth(mgmt["username"], mgmt["password"]))
        rabbit_info["queues"] = resp.json()
        print "  ...retrieved %s queues" % (len(rabbit_info["queues"]))

        url3 = url_prefix + "/api/connections"
        resp = requests.get(url3, auth=HTTPBasicAuth(mgmt["username"], mgmt["password"]))
        rabbit_info["connections"] = resp.json()
        print "  ...retrieved %s connections" % (len(rabbit_info["connections"]))

        url4 = url_prefix + "/api/exchanges"
        resp = requests.get(url4, auth=HTTPBasicAuth(mgmt["username"], mgmt["password"]))
        rabbit_info["exchanges"] = resp.json()
        print "  ...retrieved %s exchanges" % (len(rabbit_info["exchanges"]))

        if not self.opts.quick:
            url5 = url_prefix + "/api/bindings"
            resp = requests.get(url5, auth=HTTPBasicAuth(mgmt["username"], mgmt["password"]))
            rabbit_info["bindings"] = resp.json()
            print "  ...retrieved %s bindings" % (len(rabbit_info["bindings"]))

            url6 = url_prefix + "/api/channels"
            resp = requests.get(url6, auth=HTTPBasicAuth(mgmt["username"], mgmt["password"]))
            rabbit_info["channels"] = resp.json()
            print "  ...retrieved %s channels" % (len(rabbit_info["channels"]))

        rabbit_info["queue_details"] = {}
        queue_names = {q["name"] for q in rabbit_info["queues"]}
        svc_queue_names = [sn for sn in SERVICE_NAMES if self.sysname + "." + sn in queue_names]
        queue_urls = [url_prefix + "/api/queues/%2F/" + self.sysname + "." + sn for sn in svc_queue_names]

        num_threads = 5
        res_info, threads = [], []
        for i in range(num_threads):
            begin_range = ((len(queue_urls) / num_threads) + 1) * i
            end_range = ((len(queue_urls) / num_threads) + 1) * (i+1)
            work_urls = dict(zip(queue_urls[begin_range:end_range], svc_queue_names[begin_range:end_range]))
            th_info = {}
            res_info.append(th_info)
            t = threading.Thread(target=self._rabbit_get_thread, args=(work_urls, th_info, i))
            t.daemon = True
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        queue_details = {}
        for q_info in res_info:
            queue_details.update(q_info)
        rabbit_info["queue_details"] = queue_details
        print "  ...retrieved %s queue details" % (len(rabbit_info["queue_details"]))

    def _rabbit_get_thread(self, urls, th_info, num):
        import requests
        from requests.auth import HTTPBasicAuth
        mgmt = self.cfg["container"]["exchange"]["management"]
        url_prefix = "http://%s:%s" % (mgmt["host"], mgmt["port"])
        for url, reskey in urls.iteritems():
            try:
                resp = requests.get(url, auth=HTTPBasicAuth(mgmt["username"], mgmt["password"]))
                th_info[reskey] = resp.json()
            except Exception as ex:
                print " Error getting rabbit URL %s: %s" % (url, ex)

    def _get_db_info(self):
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        conn, dsn = self._get_db_connection()
        db_info = self.sysinfo.setdefault("db", {})
        try:
            print " Getting DB info from PostgreSQL as:", dsn.rsplit("=", 1)[0] + "=***"
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                if not self.opts.quick:
                    cur.execute("SELECT id,doc FROM ion_resources")
                    rows = cur.fetchall()
                    resources = {}
                    for row in rows:
                        res_id, res_doc = row[0], row[1]
                        resources[res_id] = res_doc
                    print "  ...retrieved %s resources" % (len(resources))
                    db_info["resources"] = resources

                cur.execute("SELECT id,doc FROM ion_resources_dir")
                rows = cur.fetchall()
                dir_entries = {}
                for row in rows:
                    dir_id, dir_doc = row[0], row[1]
                    if dir_id in dir_entries:
                        self._warn("dir.dup", 2, "Directory entry with id=%s duplicate", dir_id)
                    else:
                        dir_entries[dir_id] = dir_doc
                print "  ...retrieved %s directory entries" % (len(dir_entries))
                db_info["directory"] = dir_entries
        finally:
            conn.close()

    def _get_db_connection(self):
        import psycopg2

        pgcfg = self.cfg["server"]["postgresql"]
        db_name = "%s_%s" % (self.sysname.lower(), pgcfg["database"])

        dsn = "host=%s port=%s dbname=%s user=%s password=%s" % (pgcfg["host"], pgcfg["port"], db_name, pgcfg["username"], pgcfg["password"])
        return psycopg2.connect(dsn), dsn

    prefix_blacklist = ["/dtrs",
                        "/epum/elections",
                        "/pd/elections",
                        "/provisioner"]

    def _get_cei_info(self):
        cei_info = self.sysinfo.setdefault("cei", {})

        print " Getting CEI info from:", self.cfg["server"]["zookeeper"]["hosts"]
        zk = self._get_zoo_connection()
        start_node = "/" + self.sysname
        if not zk.exists(start_node):
            self._errout("Cannot find start node %s" % start_node)
        zk.stop()

        self.queue = Queue.Queue()
        self.queue.put(start_node)
        num_threads = 40 if self.opts.quick else 25
        res_info, threads = [], []
        for i in range(num_threads):
            th_info = {}
            res_info.append(th_info)
            t = threading.Thread(target=self._zoo_get_thread, args=(th_info, i))
            t.daemon = True
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        zoo_info = {}
        for th_info in res_info:
            zoo_info.update(th_info)
        cei_info["zoo"] = zoo_info
        print "  ...retrieved %s CEI nodes" % (len(zoo_info))

    def _get_zoo_connection(self):
        from kazoo.client import KazooClient
        zkcfg = self.cfg["server"]["zookeeper"]
        zk = KazooClient(hosts=zkcfg["hosts"])
        zk.start()
        return zk

    def _zoo_get_thread(self, th_info, num):
        zk = self._get_zoo_connection()
        try:
            node = self.queue.get(True, 2)
            while node:
                data, stats = zk.get(node)
                try:
                    th_info[node] = json.loads(data) if data else {}
                except Exception:
                    th_info[node] = dict(error=True, value=data)
                ch_nodes = zk.get_children(node)
                for ch in ch_nodes:
                    ch_node = node + "/" + ch
                    if self.opts.quick and any([pre in ch_node for pre in self.prefix_blacklist]):
                        continue
                    self.queue.put(ch_node)
                node = self.queue.get(True, 0.5)
        except Queue.Empty:
            pass

        zk.stop()

    # -------------------------------------------------------------------------

    def save_info_files(self):
        if self.opts.info_dir:
            path = self.opts.info_dir
        else:
            dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
            path = "sysinfo_%s" % dtstr
        if not os.path.exists(path):
            os.makedirs(path)

        print "Saving system info files into", path

        rabbit_pre = "%s/%s" % (path, "rabbit")
        rabbit_info = self.sysinfo.get("rabbit", None)
        self._save_file(rabbit_pre, "overview", rabbit_info)
        self._save_file(rabbit_pre, "queues", rabbit_info)
        self._save_file(rabbit_pre, "queue_details", rabbit_info)
        self._save_file(rabbit_pre, "connections", rabbit_info)
        self._save_file(rabbit_pre, "exchanges", rabbit_info)
        self._save_file(rabbit_pre, "bindings", rabbit_info)
        self._save_file(rabbit_pre, "channels", rabbit_info)

        db_pre = "%s/%s" % (path, "db")
        db_info = self.sysinfo.get("db", None)
        self._save_file(db_pre, "resources", db_info)
        self._save_file(db_pre, "directory", db_info)

        cei_pre = "%s/%s" % (path, "cei")
        cei_info = self.sysinfo.get("cei", None)
        self._save_file(cei_pre, "zoo", cei_info)

    def _save_file(self, prefix, part, content):
        if not content:
            return
        if part not in content:
            return
        part_content = content[part]
        filename = "%s_%s.json" % (prefix, part)
        json_content = json.dumps(part_content)
        with open(filename, "w") as f:
            f.write(json_content)
        print " ...saved %s (%s bytes)" % (filename, len(json_content))

    def read_info_files(self):
        path = self.opts.info_dir
        print "Reading system info files from", path

        rabbit_pre = "%s/%s" % (path, "rabbit")
        rabbit_info = self.sysinfo.setdefault("rabbit", {})
        self._read_file(rabbit_pre, "overview", rabbit_info)
        self._read_file(rabbit_pre, "queues", rabbit_info)
        self._read_file(rabbit_pre, "queue_details", rabbit_info)
        self._read_file(rabbit_pre, "connections", rabbit_info)
        self._read_file(rabbit_pre, "exchanges", rabbit_info)
        self._read_file(rabbit_pre, "bindings", rabbit_info)
        self._read_file(rabbit_pre, "channels", rabbit_info)

        db_pre = "%s/%s" % (path, "db")
        db_info = self.sysinfo.setdefault("db", {})
        self._read_file(db_pre, "resources", db_info)
        self._read_file(db_pre, "directory", db_info)

        cei_pre = "%s/%s" % (path, "cei")
        cei_info = self.sysinfo.setdefault("cei", {})
        self._read_file(cei_pre, "zoo", cei_info)

    def _read_file(self, prefix, part, content):
        if content is None:
            return
        if part in content and content[part]:
            # We don't overwrite retrieved info
            return
        filename = "%s_%s.json" % (prefix, part)
        if not os.path.exists(filename):
            return
        with open(filename, "r") as f:
            json_content = f.read()
            content[part] = json.loads(json_content)
        print " ...loaded %s (%s bytes)" % (filename, len(json_content))

    # -------------------------------------------------------------------------

    def diagnose(self):
        print "-----------------------------------------------------"
        self._analyze()

        if "R" in self.opts.only_do.upper():
            self._diag_rabbit()

        if "C" in self.opts.only_do.upper():
            self._diag_cei()

        if "D" in self.opts.only_do.upper():
            self._diag_db()


    def _analyze(self):
        print "Analyzing system info"
        self._res_by_type = {}
        self._res_by_id = self.sysinfo.get("db", {}).get("resources", {})
        if self._res_by_id:
            for res in self._res_by_id.values():
                self._res_by_type.setdefault(res.get("type_", "?"), []).append(res)
        self._services = {str(res["name"]) for res in self._res_by_type.get("ServiceDefinition", {})}
        print " ...found %s services in RR" % len(self._services)

        self._agents = {}
        self._agentdup = []
        self._agent_by_resid = {}
        self._agentdup_by_resid = {}
        self._agent_by_type = {}
        directory = self.sysinfo.get("db", {}).get("directory", None)
        if directory:
            for de in directory.values():
                # Extract agent info
                if de["parent"] == "/Agents":
                    attrs = de["attributes"]
                    agent_id, agent_name, agent_type = de["key"], attrs.get("name", ""), "?"
                    resource_id = attrs.get("resource_id", "")
                    if agent_name.startswith("eeagent"):
                        agent_type = "EEAgent"
                    elif agent_name.startswith("haagent"):
                        agent_type = "HAAgent"
                    elif "ExternalDatasetAgent" in agent_name:
                        agent_type = "DatasetAgent"
                    elif "InstrumentAgent" in agent_name:
                        agent_type = "InstrumentAgent"
                    elif "PlatformAgent" in agent_name:
                        agent_type = "PlatformAgent"
                    else:
                        print "  Cannot categorize agent:", agent_id
                    if agent_id in self._agents:
                        self._warn("dir", 2, "Agent id=%s multiple times in directory", agent_id)
                        self._agentdup.append(agent_id)
                    agent_entry = dict(key=agent_id, agent_name=agent_name,
                                       agent_type=agent_type,
                                       resource_id=resource_id)
                    self._agents[agent_id] = agent_entry
                    self._agent_by_type.setdefault(agent_type, []).append(agent_id)
                    if resource_id and resource_id in self._res_by_id:
                        if resource_id in self._agent_by_resid:
                            self._agentdup_by_resid.setdefault(resource_id, set()).add(agent_id)
                        else:
                            self._agent_by_resid[resource_id] = agent_id
            print " ...found %s agents in directory (%s for resources)" % (len(self._agents), len(self._agent_by_resid))

            if self._agentdup_by_resid:
                for resource_id, ag_list in self._agentdup_by_resid.iteritems():
                    ag_list.add(self._agent_by_resid[resource_id])
                    res_obj = self._res_by_id.get(resource_id, None)
                    self._warn("db.dir_resagent", 2, "Resource %s (%s) has multiple agents in dir: \n   %s", resource_id,
                               res_obj["name"] if res_obj else "ERR", "\n   ".join(aid for aid in ag_list))


    def _diag_rabbit(self):
        print "-----------------------------------------------------"
        print "Analyzing RabbitMQ info..."
        rabbit = self.sysinfo.get("rabbit", {})
        self._rabbit = rabbit
        if not rabbit:
            return

        self._rabbit_max_ts = rabbit.get("overview", {}).get("message_stats", {}).get("deliver_details", {}).get("last_event", 0)
        conns = rabbit.get("connections", [])
        self._active_conns = [c for c in conns if c["recv_oct_details"]["last_event"] >= (self._rabbit_max_ts - 60*60*1000)]
        conn_hosts = {c["peer_address"] for c in self._active_conns}
        print " ...found %s connections, %s active, to %s hosts" % (len(conns), len(self._active_conns), len(conn_hosts))
        if self.opts.verbose:
            inactive_conns = [c for c in conns if c["recv_oct_details"]["last_event"] < (self._rabbit_max_ts - 60*60*1000)]
            inactive_hosts = {c["peer_address"] for c in inactive_conns}
            print "  Inactive hosts:", ", ".join(c for c in inactive_hosts)

        # NOTE: Consumers for queues is overapproximation. Not all consumers may be active anymore

        queues = rabbit.get("queues", {})
        named_queues = [q for q in queues if not q["name"].startswith("amq")]
        named_queues_cons = [q for q in named_queues if q["consumers"]]
        print " ...found %s named queues (%s with consumers)" % (len(named_queues), len(named_queues_cons))
        self._named_queues = {q["name"].split(".", 1)[-1]:q for q in named_queues}

        anon_queues = [q for q in queues if q["name"].startswith("amq")]
        anon_queues_cons = [q for q in anon_queues if q["consumers"]]
        print " ...found %s anonymous queues (%s with consumers)" % (len(anon_queues), len(anon_queues_cons))

        # Check service queues
        service_queues = [q for q in named_queues if q["name"].split(".", 1)[-1] in self._services]
        self._service_queues = {q["name"].split(".", 1)[-1]:q for q in service_queues}
        service_queues_cons = [q for q in service_queues if q["consumers"]]
        print " ...found %s service queues (%s with consumers)" % (len(service_queues), len(service_queues_cons))
        for q in service_queues:
            if not q["consumers"]:
                self._err("rabbit.svc_queue", 2, "service queue %s has %s consumers", q["name"], q["consumers"])
            elif self.opts.verbose:
                print "  service queue %s: %s consumers" % (q["name"], q["consumers"])

        # Check agent process id queues
        agent_queues = [q for q in named_queues if q["name"].split(".", 1)[-1] in self._agents]
        agent_queues_cons = [q for q in agent_queues if q["consumers"]]
        print " ...found %s agent pid queues (%s with consumers)" % (len(agent_queues), len(agent_queues_cons))
        for q in agent_queues:
            if not q["consumers"]:
                agent_key = q["name"].split(".", 1)[-1]
                self._warn("rabbit.apid_queue", 2, "agent pid queue %s (%s, %s) has %s consumers", q["name"],
                           self._agents[agent_key]["agent_type"], self._agents[agent_key]["agent_name"], q["consumers"])

        # Check agent device id queues
        agent_queues = [q for q in named_queues if q["name"].split(".", 1)[-1] in self._agent_by_resid]
        agent_queues_cons = [q for q in agent_queues if q["consumers"]]
        print " ...found %s agent rid queues (%s with consumers)" % (len(agent_queues), len(agent_queues_cons))
        for q in agent_queues:
            if not q["consumers"]:
                agent_key = self._agent_by_resid[q["name"].split(".", 1)[-1]]
                self._warn("rabbit.arid_queue", 2, "agent rid queue %s (%s, %s) has %s consumers", q["name"],
                           self._agents[agent_key]["agent_type"], self._agents[agent_key]["agent_name"], q["consumers"])

        #pprint.pprint(sorted(q["name"] for q in named_queues))

        total_messages = 0
        for q in self._named_queues.values():
            if q["messages"] > 2:
                self._warn("rabbit.qu.msgs.named", 2, "Queue %s has unconsumed messages: %s (idle since %s), %s consumers",
                           q["name"], q["messages"], q.get("idle_since", ""), q["consumers"])
            total_messages += q["messages"]
        for q in anon_queues:
            if q["messages"] > 2:
                self._warn("rabbit.qu.msgs.anon", 2, "Queue %s has unconsumed messages: %s (idle since %s), %s consumers",
                           q["name"], q["messages"], q.get("idle_since", ""), q["consumers"])
            total_messages += q["messages"]
        if total_messages > 200:
            self._warn("rabbit.waiting_msgs", 1, "System has %s unconsumed messages", total_messages)

    def _get_cei_ts(self, cei_ts):
        sec_ts = cei_ts.split(".",1)[0]
        sec_ts = sec_ts.split("+",1)[0]
        t1 = time.mktime(datetime.datetime.strptime(sec_ts, "%Y-%m-%dT%H:%M:%S").timetuple())
        t2 = t1 - 7*60*60   # TODO: Compensate for time difference UTC vs. PDT
        return t2

    def _diag_cei(self):
        print "-----------------------------------------------------"
        print "Analyzing CEI info..."
        self._zoo_parents = {}
        self._epus = {}
        self._epuis, self._epuis_bad = {}, {}
        self._ees, self._ees_bad = {}, {}
        self._procs, self._allprocs, self._oldprocs, self._badprocs = {}, {}, {}, {}
        self._proc_by_type, self._proc_by_epu, self._proc_by_epui = {}, {}, {}
        zoo = self.sysinfo.get("cei", {}).get("zoo", None)
        self._zoo = zoo
        if not zoo:
            return
        zoo_parents = {}
        for key, entry in zoo.iteritems():
            par, loc = key.rsplit("/", 1)
            zoo_parents.setdefault(par, []).append(key)
        self._zoo_parents = zoo_parents

        # ---------- EPU + EPU instances
        print " Analyzing EPU and VM state..."
        sys_key = "/" + self.sysname
        epum_key = sys_key + "/epum/domains/cc"
        for epu in self._zoo_parents.get(epum_key, []):
            epu_data = zoo[epu]
            epu_name = epu.rsplit("/", 1)[-1]
            epu_entry = dict(name=epu_name,
                             zoo=epu,
                             num_vm=epu_data.get("engine_conf", {}).get("preserve_n", 0),
                             num_cc=epu_data.get("engine_conf", {}).get("provisioner_vars", {}).get("replicas", 0),
                             num_proc=epu_data.get("engine_conf", {}).get("provisioner_vars", {}).get("slots", 0))
            epu_entry["max_slots"] = epu_entry["num_vm"] * epu_entry["num_cc"] * epu_entry["num_proc"]
            self._epus[epu_name] = epu_entry

            epui_num_ok, epui_num_term, epui_num_bad = 0, 0, 0
            for epui in self._zoo_parents.get(epu + "/instances", []):
                epui_data = zoo[epui]
                epui_name = epui.rsplit("/", 1)[-1]
                epui_entry = dict(name=epui_name,
                                  zoo=epui,
                                  epu=epu_name,
                                  public_ip=epui_data.get("public_ip", "ERR"),
                                  hostname=epui_data.get("hostname", "ERR"),
                                  state=epui_data.get("state", "ERR"),
                                  max_slots=epu_entry["num_cc"]*epu_entry["num_proc"])
                #print "  EPUI %s: %s %s" % (epui_entry["name"], epui_entry["public_ip"], epui_entry["state"], )
                if epui_entry["state"] == "600-RUNNING":
                    self._epuis[epui_name] = epui_entry
                    epu_entry.setdefault("instances", {})[epui_name] = epui_entry
                    epui_num_ok += 1
                else:
                    if epui_entry["state"] == "900-TERMINATED":
                        epui_num_term += 1
                    else:
                        epui_num_bad += 1
                    self._epuis_bad[epui_name] = epui_entry
                    self._warn("cei.epu_state", 3, "EPU instance %s (%s) state: %s", epui_name, epui_entry["hostname"], epui_entry["state"])

            print "  EPU %s (%s VM x %s CC x %s slots = %s total / %s avail): %s running, %s bad, %s terminated instances." % (
                epu_entry["name"], epu_entry["num_vm"], epu_entry["num_cc"], epu_entry["num_proc"], epu_entry["max_slots"],
                epu_entry["num_cc"] * epu_entry["num_proc"] * epui_num_ok,
                epui_num_ok, epui_num_bad, epui_num_term)

        print "  ...found %s EPUs, %s EPUIs, %s bad EPUIs" % (len(self._epus), len(self._epuis), len(self._epuis_bad))

        # ---------- Execution Engines
        print " Analyzing Execution Engines (containers)..."
        total_ee_procs = 0
        procs_in_ee = []
        pd_ee_key = sys_key + "/pd/resources"
        ee_no_proc = []
        ee_proc_bad = []
        for ee in self._zoo_parents.get(pd_ee_key, []):
            ee_data = zoo[ee]
            ee_name = ee_data["resource_id"]
            epui_data = self._epuis.get(ee_data["node_id"], {})
            ee_entry = dict(name=ee_name, node_id=ee_data["node_id"], state=ee_data["state"], zoo=ee,
                            epu=epui_data.get("epu", "ERR"),
                            hostname=epui_data.get("hostname", "ERR"),
                            last_heartbeat=ee_data["last_heartbeat"],
                            last_heartbeat_ts=self._get_cei_ts(ee_data["last_heartbeat"]),
                            num_procs=len(ee_data["assigned"]))
            self._ees[ee_name] = ee_entry

            num_consumers, num_msgs = -1, -1
            if hasattr(self, "_named_queues"):
                queue = self._named_queues.get(ee_name, None)
                if queue:
                    num_consumers = queue["consumers"]
                    num_msgs = queue["messages"]

            ee_ok = True
            if not epui_data:
                self._warn("cei.ee_state", 2, "EE %s %s/%s (%s) state=%s references bad EPU instance - %s processes, %s consumers, %s msgs",
                           ee_name, ee_entry["epu"], ee_entry["node_id"], ee_entry["hostname"], ee_entry["state"], len(ee_data["assigned"]),
                           num_consumers, num_msgs)
                ee_ok = False
            elif ee_entry["state"] != "OK":
                self._warn("cei.ee_state", 2, "EE %s %s/%s (%s) in bad state: %s", ee_name,
                           ee_entry["epu"], ee_entry["node_id"], ee_entry["hostname"], ee_entry["state"])
                ee_ok = False
            else:
                if ee_entry["last_heartbeat_ts"] + 60*10 < (self._rabbit_max_ts / 1000):
                    self._warn("cei.ee_ts", 2, "EE %s %s/%s (%s) heartbeat overdue: %s (vs %s)", ee_name,
                               ee_entry["epu"], ee_entry["node_id"], ee_entry["hostname"],
                               self.ts(ee_entry["last_heartbeat_ts"]), self.ts(self._rabbit_max_ts / 1000))
                    ee_ok = False

            # Check EE agent / messaging
            if epui_data and num_consumers == 0:
                self._warn("cei.ee_agent_cons", 2, "EE %s %s/%s (%s) agent has NO consumer (%s msgs)", ee_name,
                           ee_entry["epu"], ee_entry["node_id"], ee_entry["hostname"], num_msgs)
                ee_ok = False
            elif epui_data and num_msgs > 1:
                self._warn("cei.ee_agent_msg", 2, "EE %s %s/%s (%s) agent backlogged: %s msgs", ee_name,
                           ee_entry["epu"], ee_entry["node_id"], ee_entry["hostname"], num_msgs)
                #ee_ok = False

            if not ee_data["assigned"]:
                #self._info("cei.ee_assign", 2, "EE %s %s/%s (%s) has no processes (state %s)", ee_name,
                #           ee_entry["epu"], ee_entry["node_id"], ee_entry["hostname"], ee_entry["state"])
                ee_no_proc.append(ee_name)
            total_ee_procs += len(ee_data["assigned"])
            procs_in_ee.extend(x[1] for x in ee_data["assigned"])

            if not ee_ok:
                self._ees_bad[ee_name] = ee_entry
                ee_proc_bad.extend(x[1] for x in ee_data["assigned"])

        print "  ...found %s EE. Of these %s empty EE, %s bad EEs with %s total assigned processes" % (
            len(self._ees), len(ee_no_proc), len(self._ees_bad), total_ee_procs)
        if len(procs_in_ee) != len(set(procs_in_ee)):
            self._warn("cei.ee_procs", 1, "Process to EE assignment not unique")
        if ee_proc_bad:
            self._err("cei.ee_badprocs", 1, "Found %s processes on bad EEs", len(ee_proc_bad))

        # ---------- Processes (generic)
        print " Analyzing Processes..."
        pd_procs_key = sys_key + "/pd/processes"
        suspect_ees = set()  # All EPUIs that have not-OK, not-terminated processes
        for proc in self._zoo_parents.get(pd_procs_key, []):
            proc_data = zoo[proc]
            proc_id = proc_data["upid"]
            ee_data = self._ees.get(proc_data["assigned"], {})
            proc_entry = dict(upid=proc_id, name=proc_data["name"] or "", zoo=proc,
                              num_starts=proc_data["starts"],
                              restart_mode=proc_data["restart_mode"], queueing_mode=proc_data["queueing_mode"],
                              state=proc_data["state"],
                              ee=proc_data["assigned"],
                              node_id=ee_data.get("node_id", ""),
                              epu=ee_data.get("epu", ""),
                              epui=ee_data.get("epui", ""),
                              hostname=ee_data.get("hostname", ""))
            if proc_entry["state"] == "500-RUNNING":
                self._procs[proc_id] = proc_entry
                self._proc_by_epui.setdefault(ee_data["node_id"], []).append(proc_id)
                self._proc_by_epu.setdefault(ee_data["epu"], []).append(proc_id)
            elif proc_entry["state"] in {"700-TERMINATED", "800-EXITED"}:
                self._oldprocs[proc_id] = proc_entry
            else:
                self._badprocs[proc_id] = proc_entry
                if proc_entry["ee"]:
                    suspect_ees.add(proc_entry["ee"])
                elif proc_entry["state"] not in {"100-UNSCHEDULED", "200-REQUESTED", "300-WAITING"}:
                    self._warn("cei.proc_ee_assign", 3, "Process %s has non existing EE assignment", proc_id)
            self._allprocs[proc_id] = proc_entry

            # Categorize process
            if proc_entry["name"].startswith("haagent"):
                proc_entry["proc_type"] = "ha_agent"
            elif proc_entry["name"].startswith("ingestion_worker_process"):
                proc_entry["proc_type"] = "ingest_worker"
            elif proc_entry["name"].startswith("qc_post_processor"):
                proc_entry["proc_type"] = "qc_worker"
            elif proc_entry["name"].startswith("lightweight_pydap"):
                proc_entry["proc_type"] = "pydap"
            elif proc_entry["name"].startswith("vis_user_queue_monitor"):
                proc_entry["proc_type"] = "vis_user_queue_monitor"
            elif proc_entry["name"].startswith("registration_worker"):
                proc_entry["proc_type"] = "registration_worker"
            elif proc_entry["name"].startswith("event_persister"):
                proc_entry["proc_type"] = "event_persister"
            elif proc_entry["name"].startswith("notification_worker_process"):
                proc_entry["proc_type"] = "notification_worker"
            elif proc_entry["name"].startswith("HIGHCHARTS"):
                proc_entry["proc_type"] = "rt_viz"
            elif proc_entry["name"].split("-", 1)[0] in self._services:
                proc_entry["proc_type"] = "svc_worker"
            elif "InstrumentAgent" in proc_id:
                proc_entry["proc_type"] = "instrument_agent"
            elif "PlatformAgent" in proc_id:
                proc_entry["proc_type"] = "platform_agent"
            elif "ExternalDatasetAgent" in proc_id:
                proc_entry["proc_type"] = "dataset_agent"
            elif "bootstrap" in proc_entry["name"]:
                proc_entry["proc_type"] = "bootstrap"
            else:
                print "  Cannot categorize process %s %s" % (proc_id, proc_entry["name"])
                proc_entry["proc_type"] = "unknown"

            self._proc_by_type.setdefault(proc_entry["proc_type"], []).append(proc_id)

        print "  ...found %s OK processes, %s bad, %s terminated, of %s process types" % (
            len(self._procs), len(self._badprocs), len(self._oldprocs), len(self._proc_by_type))
        if suspect_ees:
            for ee_name in sorted(suspect_ees):
                ee_data = self._ees[ee_name]
                bad_ee_procs = [p for p in self._badprocs.values() if p["ee"] == ee_name]
                self._warn("cei.susp_ee", 2, "EE has non-OK processes: %s on %s/%s (%s). %s bad, %s total", ee_name,
                           ee_data["epu"], ee_data["node_id"], ee_data["hostname"],
                           len(bad_ee_procs), ee_data["num_procs"])

        unaccounted_procs = set(procs_in_ee) - set(self._procs.keys()) - set(self._badprocs.keys())
        if unaccounted_procs:
            self._warn("cei.pd_procs", 2, "Unaccounted for processes: %s", unaccounted_procs)

        for ptype in sorted(self._proc_by_type.keys()):
            procs = self._proc_by_type[ptype]
            ok_procs = [True for pid in procs if pid in self._procs]
            proc_by_state = {}
            [proc_by_state.setdefault(self._badprocs[pid]["state"], []).append(pid) for pid in procs if pid in self._badprocs]
            [proc_by_state.setdefault(self._oldprocs[pid]["state"], []).append(pid) for pid in procs if pid in self._oldprocs]
            proc_state = ", ".join(["%s %s" % (len(proc_by_state[pst]), pst) for pst in sorted(proc_by_state.keys())])
            print "  Process type %s: %s 500-RUNNING, %s (%s total)" % (ptype, len(ok_procs), proc_state, len(procs))
            for pst in sorted(proc_by_state.keys()):
                if pst in {"500-RUNNING", "700-TERMINATED", "800-EXITED"}:
                    continue
                procs1 = proc_by_state[pst]
                for pid in procs1:
                    proc_data = self._allprocs[pid]
                    if self.opts.verbose:
                        self._warn("cei.proc_state", 3, "Proc %s on %s/%s (%s) state: %s", pid,
                                   proc_data["epu"], proc_data["node_id"], proc_data["hostname"], pst)

        # ---------- EPU Usage
        print " Analyzing EPU allocation..."
        tainted_epuis = set()
        for epu in sorted(self._proc_by_epu.keys()):
            epu_procs = self._proc_by_epu[epu]
            epu_data = self._epus.get(epu, {})
            print "  EPU %s: %s total (%s VM x %s CC x %s slots), %s used." % (epu, epu_data.get("max_slots", "ERR"),
                    epu_data.get("num_vm", "ERR"), epu_data.get("num_cc", "ERR"), epu_data.get("num_proc", "ERR"),
                    len(epu_procs))

            for epui in sorted(epu_data.get("instances", [])):
                epui_data = self._epuis[epui]
                epui_ip = epui_data["public_ip"]
                host_conns = [c for c in self._active_conns if c["peer_address"] == epui_ip] if hasattr(self, "_active_conns") else -1
                epui_procs = self._proc_by_epui.get(epui, {})
                print "   EPU instance %s (%s, %s): %s total, %s used, %s rabbit connections" % (epui,
                            epui_data["hostname"], epui_data["public_ip"], epui_data["max_slots"], len(epui_procs), len(host_conns))

                if not host_conns:
                    self._warn("cei.epui_conns", 4, "EPU instance %s (%s, state=%s) has no active rabbit connections", epui,
                           epui_data["hostname"], epui_data["state"])
                    tainted_epuis.add(epui)
                if epui not in self._proc_by_epui:
                    pass
                    #self._info("cei.epui_procs", 4, "EPU instance %s (%s, state=%s) has no processes", epui,
                    #           epui_data["hostname"], epui_data["state"])

        if tainted_epuis:
            self._err("cei.epui_bad", 2, "Found %s bad EPU instances: %s", len(tainted_epuis), ", ".join(e for e in sorted(tainted_epuis)))

        # Check HA Agents
        self._ha_agents = {}
        running_ha = [self._zoo[self._procs[x]["zoo"]] for x in self._proc_by_type["ha_agent"] if x in self._procs]
        all_bad_conn = set()
        print " Analyzing HA-Agents: %s active..." % (len(running_ha))
        for ha_proc in running_ha:
            ha_cfg = ha_proc["configuration"]["highavailability"]
            ha_name = ha_cfg["process_definition_name"]
            ha_entry = dict(zoo=self._procs[ha_proc["upid"]]["zoo"], name=ha_name,
                            npreserve=ha_cfg["policy"]["parameters"]["preserve_n"])
            self._ha_agents[ha_name] = ha_entry

            # Check OK worker processes for HA
            ha_workers = [pid for pid in self._procs if pid.startswith(ha_name)]
            ha_entry["num_workers"] = len(ha_workers)
            ha_entry["workers"] = ha_workers

            # TODO: HA agent queue itself

            num_consumers = -2
            if hasattr(self, "_service_queues"):
                queue = self._service_queues.get(ha_name, None)
                num_consumers = queue["consumers"] if queue else -1
            if len(ha_workers) < ha_entry["npreserve"]:
                self._warn("cei.ha.worker.cnt.low", 2, "HA %s missing workers: preserve_n=%s, running=%s", ha_name,
                           ha_entry["npreserve"], len(ha_workers))
            elif len(ha_workers) > ha_entry["npreserve"]:
                self._warn("cei.ha.worker.cnt.high", 2, "HA %s has unexpected workers: preserve_n=%s, running=%s", ha_name,
                           ha_entry["npreserve"], len(ha_workers))
            elif self.opts.verbose:
                print "  HA %s: preserve_n=%s, running=%s, consumers=%s" % (ha_name, ha_entry["npreserve"],
                                                                            len(ha_workers), num_consumers)
            if num_consumers != -2 and num_consumers != ha_entry["npreserve"]:
                if num_consumers == 0:
                    self._err("cei.ha_worker", 2, "HA %s has NO consumers: preserve_n=%s, running=%s, consumers=%s", ha_name,
                               ha_entry["npreserve"], len(ha_workers), num_consumers)
                elif num_consumers == -1:
                    if ha_name not in {"vis_user_queue_monitor", "event_persister", "lightweight_pydap"}:
                        self._warn("cei.ha_worker", 2, "HA %s cannot determine consumers: preserve_n=%s, running=%s, consumers=%s", ha_name,
                               ha_entry["npreserve"], len(ha_workers), num_consumers)
                elif num_consumers < ha_entry["npreserve"]:
                    self._warn("cei.ha_worker", 2, "HA %s missing consumers: preserve_n=%s, running=%s, consumers=%s", ha_name,
                               ha_entry["npreserve"], len(ha_workers), num_consumers)
                elif num_consumers > ha_entry["npreserve"]:
                    self._warn("cei.ha_worker", 2, "HA %s unexpected consumers: preserve_n=%s, running=%s, consumers=%s", ha_name,
                               ha_entry["npreserve"], len(ha_workers), num_consumers)

                for wpid in ha_entry["workers"]:
                    hazoo = self._zoo[self._allprocs[wpid]["zoo"]]
                    wcons = -1
                    if hasattr(self, "_named_queues"):
                        proc_queue = self._named_queues.get(wpid)
                        if proc_queue:
                            wcons = proc_queue["consumers"]
                    #print "  ", wpid, hazoo["state"], self.ts(hazoo["dispatch_times"][-1]), wcons
                    if not wcons:
                        wproc_entry = self._allprocs[wpid]
                        self._warn("cei", 3, "Worker %s has no pid consumer (%s/%s %s)", wpid, wproc_entry["epu"],
                                   wproc_entry["node_id"], wproc_entry["hostname"])

            queue_detail = self.sysinfo.get("rabbit", {}).get("queue_details", {}).get(ha_name, None)
            if queue_detail:
                if queue_detail.get("messages_unacknowledged", 0) > 1:
                    self._warn("cei.ha.queue.unack", 2, "HA %s has %s un-ACK'ed messages, %s ready",
                               ha_name, queue_detail["messages_unacknowledged"], queue_detail["messages_ready"])
                deliveries = queue_detail.get("deliveries", [])
                consumer_details = queue_detail.get("consumer_details", [])
                #print "   HA %s: %s del, %s cons" % (ha_name, len(deliveries), len(consumer_details))
                bad_conn, bad_chan = set(), set()
                for ddetail in deliveries:
                    stats = ddetail.get("stats", {})
                    if stats.get("ack", 0) != stats.get("deliver", 0):
                        bad_conn.add(ddetail.get("channel_details", {}).get("connection_name", "?"))
                        all_bad_conn.add(ddetail.get("channel_details", {}).get("connection_name", "?"))
                        bad_chan.add(ddetail.get("channel_details", {}).get("name", "?"))
                if bad_conn:
                    for pconn in sorted(bad_conn):
                        self._warn("cei.ha.queue.badconn", 3, "HA %s has unresponsive workers at connection: %s (%s channels)",
                                   ha_name, pconn, len(bad_chan))

        if all_bad_conn:
            for pconn in sorted(all_bad_conn):
                self._warn("cei.ha.badconn", 2, "Unresponsive workers at connection: %s", pconn)


        # Check missing HA based on defined service queues
        if hasattr(self, "_service_queues"):
            ha_queues = set(self._service_queues.keys())
            unaccounted_ha = ha_queues - set(self._ha_agents.keys()) - {"process_dispatcher", "provisioner"}
            for missing_ha in sorted(unaccounted_ha):
                self._err("cei.ha_missing", 2, "HA-Agent missing: %s", missing_ha)

        # Check ingestion
        running_ingest = [self._zoo[self._procs[x]["zoo"]] for x in self._proc_by_type["ingest_worker"] if x in self._procs]
        print " Analyzing ingestion workers: %s active..." % (len(running_ingest))
        for ing_proc in running_ingest:
            ing_pid = ing_proc["upid"]
            num_consumers = -1
            if hasattr(self, "_named_queues"):
                queue = self._named_queues.get(ing_pid, None)
                if queue:
                    num_consumers = queue["consumers"]
            if num_consumers == 0:
                iproc_entry = self._allprocs[ing_pid]
                self._warn("cei.ing_worker", 2, "Ingestion worker %s has NO consumer (%s/%s %s)", ing_pid, iproc_entry["epu"],
                                   iproc_entry["node_id"], iproc_entry["hostname"])

        # Check dataset, instrument, platform agents
        agent_pids = self._proc_by_type.get("instrument_agent", []) + self._proc_by_type.get("platform_agent", []) + self._proc_by_type.get("dataset_agent", [])
        running_agent = [self._zoo[self._procs[x]["zoo"]] for x in agent_pids if x in self._procs]
        print " Analyzing agents: %s active..." % (len(running_agent))
        for ag_proc in running_agent:
            ag_pid = ag_proc["upid"]
            num_consumers = -1
            if hasattr(self, "_named_queues"):
                queue = self._named_queues.get(ag_pid, None)
                if queue:
                    num_consumers = queue["consumers"]
            if num_consumers == 0:
                aproc_entry = self._allprocs[ag_pid]
                self._warn("cei.agent_proc", 2, "Agent %s has NO consumer (%s/%s %s)", ag_pid, aproc_entry["epu"],
                                   aproc_entry["node_id"], aproc_entry["hostname"])
            if ag_pid not in self._agents:
                aproc_entry = self._allprocs[ag_pid]
                self._warn("cei.agent_stale", 2, "Agent %s is not listed in directory  (%s/%s %s)", ag_pid, aproc_entry["epu"],
                                   aproc_entry["node_id"], aproc_entry["hostname"])

        bad_agent = [self._zoo[self._allprocs[x]["zoo"]] for x in agent_pids if x in self._badprocs]
        if bad_agent:
            self._warn("cei.agent_bad", 2, "Found %s agent processes in bad state", len(bad_agent))
            #if self.opts.verbose:
            #    self._warn("cei.agent_bad", 2, "Bad agent procs: \n   %s", "\n   ".join(["%s - %s" % (a["name"], a["state"]) for a in bad_agent]))


    def _diag_db(self):
        print "-----------------------------------------------------"
        print "Analyzing DB info..."

        # Check directory agents vs. running procs
        print " Analyzing directory listed agents..."
        num_ag, num_ag_ok, num_ag_bad, num_ag_term, num_ag_noproc = 0, 0, 0, 0, 0
        for ag_entry in self._agents.values():
            if ag_entry["agent_type"] not in {"DatasetAgent", "InstrumentAgent", "PlatformAgent"}:
                # also: "EEAgent", "HAAgent"
                continue
            agent_id = ag_entry["key"]
            res_obj = self._res_by_id.get(ag_entry["resource_id"], None)
            num_ag += 1

            if agent_id in self._procs:
                num_ag_ok += 1
            elif agent_id in self._oldprocs:
                self._warn("db.dir_agent_oldproc", 2, "Agent %s (for %s %s) CEI process terminated", agent_id,
                           ag_entry["resource_id"], res_obj["name"] if res_obj else "ERR")
                num_ag_term += 1
            elif agent_id in self._badprocs:
                self._warn("db.dir_agent_badproc", 2, "Agent %s (for %s %s) CEI process in bad state", agent_id,
                           ag_entry["resource_id"], res_obj["name"] if res_obj else "ERR")
                num_ag_bad += 1

            if agent_id not in self._allprocs:
                self._warn("db.dir_agent_noproc", 2, "Agent %s (for %s %s) has no CEI process", agent_id,
                           ag_entry["resource_id"], res_obj["name"] if res_obj else "ERR")
                num_ag_noproc += 1

        print "  ...found %s agents: %s OK, %s with bad proc, %s with terminated proc" % (
            num_ag, num_ag_ok, num_ag_bad, num_ag_term)

    # -------------------------------------------------------------------------

    def print_summary(self):
        print "-----------------------------------------------------"
        print "SUMMARY"
        print " Number of ERR:  %s" % len([m for m in self.msgs if m[2] == "ERR"])
        print " Number of WARN: %s" % len([m for m in self.msgs if m[2] == "WARN"])


    def ts(self, val):
        val = int(val)
        if len(str(val)) == 13:
            val = val / 1000
        dt = datetime.datetime.fromtimestamp(val)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def interactive(self):
        ts = self.ts

        from IPython import embed
        embed()

    def _debug(self, category, indent, msg, *args, **kwargs):
        self._logmsg(category, indent, "DEBUG", msg, *args, **kwargs)

    def _info(self, category, indent, msg, *args, **kwargs):
        self._logmsg(category, indent, "INFO", msg, *args, **kwargs)

    def _warn(self, category, indent, msg, *args, **kwargs):
        self._logmsg(category, indent, "WARN", msg, *args, **kwargs)

    def _err(self, category, indent, msg, *args, **kwargs):
        self._logmsg(category, indent, "ERR", msg, *args, **kwargs)

    COLOR_MAP = {"ERR": 31, "WARN": 33, "INFO": 32}

    def _logmsg(self, category, indent, level, msg, *args, **kwargs):
        if level and level in {"WARN", "ERR", "INFO"}:
            prefix = (" "*indent) + level + ": "
        else:
            prefix = " "*indent
        if "%" in msg:
            msgstr = prefix + msg % args
        elif args:
            msgstr = prefix + msg + " " + " ".join(args)
        else:
            msgstr = prefix + msg
        self.msgs.append((category, indent, level, msgstr))


        # Print output
        color = self.COLOR_MAP.get(level, "")
        if color:
            msgstr = "\033[1m\033[%sm%s\033[0m" % (color, msgstr)

        if self.opts.level == "CRIT":
            if level in ("ERR", "CRIT"):
                print msgstr
        elif self.opts.level == "ERR":
            if level == "ERR":
                print msgstr
        else:
            hide_it = False
            for hide_pre in self._hide_warn_pre_list:
                if hide_pre and category.startswith(hide_pre):
                    hide_it = True
                    break
            if not hide_it:
                print msgstr

    def _errout(self, msg=None):
        if msg:
            print "FAIL:", msg
        sys.exit(1)

    def start(self):
        self.init_args()
        self.read_config()
        if self.opts.load_info:
            if not self.opts.info_dir or not os.path.exists(self.opts.info_dir):
                self._errout("Path %s does not exist" % self.opts.info_dir)
            self.read_info_files()

        else:
            self.get_system_info()
            # Fill any gaps with existing info
            if self.opts.info_dir and os.path.exists(self.opts.info_dir):
                self.read_info_files()

            if not self.opts.no_save:
                self.save_info_files()

        self.diagnose()

        self.print_summary()

        if self.opts.interactive:
            self.interactive()

def entry():
    diag = IonDiagnose()
    diag.start()

if __name__ == '__main__':
    entry()
