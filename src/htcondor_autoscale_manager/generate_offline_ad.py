
import random
import time

import classad
import htcondor

def get_offline_ads(constraints, pool=None):
    coll = htcondor.Collector(pool)

    pslots = list(coll.query(htcondor.AdTypes.Startd,
               constraint="%s && PartitionableSlot && Offline" % constraints))

    if pslots:
        return pslots


def generate_offline_ad(constraints, resource="autoscale", pool=None):
    coll = htcondor.Collector(pool)

    pslots = list(coll.query(htcondor.AdTypes.Startd,
             constraint="%s && PartitionableSlot && Offline =!= true" % constraints))

    if not pslots:
       return False

    target_ad = random.choice(pslots)
    target_ad = classad.parseOne(target_ad.printOld())

    # These changes are inspired by src/condor_collector.V6/offline_plugin.cpp
    target_ad["State"] = "Unclaimed"
    target_ad["Activity"] = "Idle"
    target_ad["EnteredCurrentState"] = 0
    target_ad["EnteredCurrentActivity"] = 0

    now = int(time.time())
    target_ad["MyCurrentTime"] = now
    target_ad["LastHeardFrom"] = now

    target_ad["LoadAvg"] = 0.0
    target_ad["CondorLoadAvg"] = 0.0
    target_ad["TotalLoadAvg"] = 0.0
    target_ad["TotalCondorLoadAvg"] = 0.0

    target_ad["CpuIsBusy"] = False
    target_ad["CpuBusyTime"] = 0

    target_ad["KeyboardIdle"] = int(2**31-1)
    target_ad["ConsoleIdle"] = int(2**31-1)

    # Reset resources to "pretend" this is an idle p-slot
    target_ad["Cpus"] = target_ad["TotalSlotCpus"]
    target_ad["Memory"] = target_ad["TotalSlotMemory"]
    target_ad["Disk"] = target_ad["TotalSlotDisk"]
    target_ad["GPUs"] = target_ad["TotalSlotGPUs"]
    target_ad["RecentJobDurationCount"] = 0

    # Generate a nice-looking name
    target_ad["Name"] = f"slot1@{resource}-offline-0"
    target_ad["Machine"] = f"{resource}-offline-0"

    # Make sure the lifetime is not infinite...
    target_ad["ClassAdLifetime"] = 3600
    target_ad["Offline"] = True

    return target_ad
