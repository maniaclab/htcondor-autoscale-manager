
from . import get_offline_ads, count_idle, generate_offline_ad
from . import count_deploy

import htcondor

import time

def occupancy_metric(query, constraints, pool=None):
    now = time.time()

    counts = count_deploy(query, constraints, pool=pool)

    print(f"There are {counts['idle']} idle startds in the resource out of {counts['total']} total.")

    ads = get_offline_ads(constraints, pool=pool)
    good_ads = []
    if not ads:
        ads = []
    for ad in ads:
        if (ad.get("LastHeardFrom", 0) + ad.get("ClassAdLifetime") >= now + 20*60):
            good_ads.append(ad)
    if not good_ads:
        ad = generate_offline_ad(constraints, pool=pool)
        if ad:
            coll = htcondor.Collector(pool)
            coll.advertise([ad], command="UPDATE_STARTD_AD")
            good_ads.append(ad)

    if not good_ads:
        print("Unable to generate an offline ad for this resource.  Is it running?")
    else:
        print(f'Have offline ad with name: {good_ads[0]["Name"]}')

    useful_offline_ads = 0
    for ad in good_ads:
        if ad.get("MachineLastMatchTime") and (ad["MachineLastMatchTime"] > now - 5*60):
            useful_offline_ads += 1

    print(f"There were {useful_offline_ads} offline ads marked as useful.")

    slots_needed = useful_offline_ads - counts['idle'] - len(counts['offline_pods'])
    target_slots = counts['total'] + slots_needed
    metric = target_slots / counts['total']
    print(f"Current occcupancy metric value: {metric}")

    return metric, counts
