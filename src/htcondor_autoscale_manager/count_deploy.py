
import subprocess
import json

import classad
import htcondor

def count_deploy(query, constraints, pool=None):

    count = subprocess.run(["/app/kubectl", "get", "pods", "-o", "json", "-l", query], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    count.check_returncode()

    count = json.loads(count.stdout)

    pods = set()
    costs = {}
    for pod in count['items']:
        name = pod['spec']['nodeName']  if pod['spec'].get('hostNetwork', False) else pod['metadata']['name']
        pods.add(name)
        costs[name] = \
            pod['metadata'].get('annotations', {}) \
            .get("controller.kubernetes.io/pod-deletion-cost")

    coll = htcondor.Collector(pool)
    pslots = coll.query(htcondor.AdTypes.Startd,
        constraint="%s && PartitionableSlot && Offline =!= true" % \
                   constraints,
        projection=["TotalCPUs", "CPUs", "Name", "UtsnameNodename"])

    online_pods = set()
    idle_pods = set()
    for slot in pslots:
        name = slot.get("UtsnameNodename")
        if not name:
            continue
        online_pods.add(name)
        if slot.get("CPUs") == slot.get("TotalCpus"):
            idle_pods.add(name)

    return {"pods": pods,
            "idle_pods": idle_pods,
            "online_pods": online_pods,
            "total": len(pods),
            "idle": len(idle_pods),
            "offline_pods": pods.difference(online_pods),
            "costs": costs,
           }
