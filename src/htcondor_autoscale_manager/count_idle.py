
import classad
import htcondor


def count_idle(constraints, pool=None):
    coll = htcondor.Collector(pool)

    pslots = coll.query(htcondor.AdTypes.Startd,
               constraint="%s && PartitionableSlot && Offline =!= true" % constraints,
               projection=["TotalCPUs", "CPUs", "Name"])

    return {"total": len(pslots), "idle": sum(1 for slot in pslots if slot.get("CPUs") == slot.get("TotalCpus"))}
