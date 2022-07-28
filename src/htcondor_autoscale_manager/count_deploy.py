
import subprocess
import json

import classad
import htcondor

def count_deploy(query, constraints, pool=None):

    count = subprocess.run(["/app/kubectl", "get", "pods", "-o", "json", "-l", query,
        "--field-selector", "status.phase==Running"], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    count.check_returncode()

    count = json.loads(count.stdout)

    pods = set()
    costs = {}
    for pod in count['items']:
        pods.add(pod['metadata']['name'])
        costs[pod['metadata']['name']] = \
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
        # nodename to podname
        # in case when the condor worker pod uses hostnetwork, the UtsnameNodename field is the hostname 
        # translate the hostname to pod name
        if name in pods:
            podname = name
        else:
            podname = next((pod['metadata']['name'] for pod in count['items'] if pod['spec'].get('hostNetwork', False) == True and pod['spec']['nodeName'] == name), None)
        if not podname:
            continue

        online_pods.add(podname)
        if slot.get("CPUs") == slot.get("TotalCpus"):
            idle_pods.add(podname)

    return {"pods": pods,
            "idle_pods": idle_pods,
            "online_pods": online_pods,
            "total": len(pods),
            "idle": len(idle_pods),
            "offline_pods": pods.difference(online_pods),
            "costs": costs,
           }
