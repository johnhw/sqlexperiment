import logging
import time
import math

# local NTP server
#default_ntp_servers = ["ntp0.dcs.gla.ac.uk", "ntp1.dcs.gla.ac.uk", "ntp2.dcs.gla.ac.uk"]
default_ntp_servers = ["1.pool.ntp.org","2.pool.ntp.org","3.pool.ntp.org"]

def check_time_sync(n_queries=3, servers=None):
    """Use NTP to find the offset from the real time, by querying NTP servers.
    Queries each server n_queries times"""
    if servers is None:
        servers = default_ntp_servers
    try:
        import ntplib
    except ImportError:
        logging.warn("No NTPLib installed; proceeding *without* real synchronisation.\n 'pip install ntplib' will install NTPLib")
        return 0
    c = ntplib.NTPClient()
    for server in servers:
        # synchronise to each server
        logging.debug("Synchronising to NTP server %s" % server)
        offsets = []
        try:
            # make a bunch of requests from this server, and record the offsets we got back
            for j in range(n_queries):
                response = c.request(server, version=3)
                offsets.append(response.offset)
                time.sleep(0.05)
        except ntplib.NTPException, e:
            logging.debug("Request to %s failed with %s" % (server, e))

    # if we got some times, compute the median time and return it (and record some status logs)
    if len(offsets)>0:
        mean = sum(offsets) / float(len(offsets))
        std = math.sqrt(sum(((o-mean)**2 for o in offsets)) / float(len(offsets)))
        median = sorted(offsets)[len(offsets)//2]
        logging.debug("Time offset %.4f (median: %.4f) seconds (%.4f seconds std. dev.)" % (mean, median, std))
        return median
    return 0