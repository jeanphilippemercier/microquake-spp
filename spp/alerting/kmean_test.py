from spp.clients.api_client import get_catalog
from microquake.core.settings import settings
from datetime import datetime
import pandas as pd
from sklearn.cluster import (KMeans, AffinityPropagation, MeanShift,
                             estimate_bandwidth)
from sklearn.mixture import GaussianMixture as GMM
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score
import numpy as np
from loguru import logger

api_base_url = settings.get('api_base_url')

start_time = datetime(2019, 12, 14, 10, 23)
end_time = datetime(2019, 12, 14, 11, 23)

start_time = datetime(2019, 9, 1)
end_time = datetime.now()

res = get_catalog(api_base_url, start_time, end_time,
                  event_type='seismic event', status='accepted')

x = []
y = []
z = []
for re in res:
    x.append(re.x)
    y.append(re.y)
    z.append(re.z)

df = pd.DataFrame({'x': x, 'y': y, 'z': z})

bandwidth = estimate_bandwidth(df, quantile=0.2)
ms = MeanShift(bandwidth=bandwidth)
ms.fit(df)
labels = ms.predict(df)
centroids = ms.cluster_centers_

logger.info(f'{len(np.unique(labels))} cluster found')

df['label'] = labels
df['magnitude'] = mag
df['ev_time'] = ev_time

df_label = df.groupby('label')


min_centroid_distance = 150  # minimum distance between centroid in m

df = pd.DataFrame({'x': x, 'y': y, 'z': z})

sil = []
ks = []
min_dists = []
for k in range(2, 20):
    kmeans = KMeans(n_clusters=k).fit(df)
    labels = kmeans.predict(df)
    centroids = kmeans.cluster_centers_
    dists = []
    for i, centroid_i in enumerate(centroids):
        for j, centroid_j in enumerate(centroids[i+1:]):
            dists.append(np.linalg.norm(centroid_i - centroid_j))

    min_dists.append(np.min(dists))
    # sil.append(silhouette_score(df, labels=labels, metric='euclidean'))
    ks.append(k)

for i, k in enumerate(range(2, 20)):
    if min_dists[i] < min_centroid_distance:
        break

if k != 0:
    k -= 1


# i = np.argmax(sil)
# k = ks[i]

kmeans = KMeans(n_clusters=k).fit(df)
labels = kmeans.predict(df)
centroids = kmeans.cluster_centers_

plt.clf()
plt.scatter(x, y, c=labels)
plt.scatter(centroids[:,0], centroids[:,1], marker='x')
plt.savefig('gugu.png')



# gmm = GMM(n_components=4, covariance_type='full', random_state=42).fit(df)
# labels = gmm.predict(df)
# # centroids = gmm.cluster_centers_

# plt.clf()
# plt.scatter(x, y, c=labels)
# # plt.plot(centroids[:,0], centroids[:,1], 'x')
# plt.savefig('gugu.png')

#
#
# min_distance_centroid = 50  # distance in the unit of measure
#                             # (the unit of measure is likely to be meter)
#
# n_clusters = 20
# kmeans = KMeans(n_clusters=n_clusters)
# kmeans.fit(df)
#
# ap = AffinityPropagation(preference=-50).fit(df)
#
# bandwidth = estimate_bandwidth(df, quantile=0.1)
# ms = MeanShift(bandwidth=bandwidth)
# ms.fit(df)
# labels = ms.predict(df)



