from microquake.core import read_events

cat = read_events('../../data/nll_processed.xml')

res = []
for event in cat:
    for arrival in event.preferred_origin().arrivals:
        res.append(arrival.time_residual)
