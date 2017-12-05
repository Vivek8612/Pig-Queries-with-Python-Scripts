import os
import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn import metrics


#df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../data', 'Grainger_AgentLines_2Weeks.csv'))
#df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../data', 'Grainger2WeeksAgentsLine_Input.csv'))
#df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../data', 'test_50_clusters_masked_output.csv'))
df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../data', 'Grainger_masked_50_clusters_30_sub_output.csv'))

#df2 = df.ix[df.main_cluster.isin([14, 15, 18, 19, 26, 28, 30, 43])]
df2 = df.ix[df.sub_cluster.isin([6, 7, 13, 16, 17, 26, 27])]

documents = df2.lineText.tolist()

vectorizer = TfidfVectorizer(stop_words=None)
X = vectorizer.fit_transform(documents)

range_n_clusters = [10,15,20,25,30]

for n_clusters in range_n_clusters:
    model = KMeans(n_clusters=n_clusters, init='k-means++', random_state=2)
    model.fit(X)

    silhouette_score = metrics.silhouette_score(X, model.labels_)
    print "Cluster Size: ", n_clusters
    print "Silhouette Score: ", silhouette_score


#model = KMeans(n_clusters=true_k, init='k-means++', random_state=2)
#model.fit(X)

#print("Top terms per cluster:")
#order_centroids = model.cluster_centers_.argsort()[:, ::-1]
#terms = vectorizer.get_feature_names()
#for i in range(true_k):
#    print "Cluster %d:" % i,
#    for ind in order_centroids[i, :10]:
#        print ' %s' % terms[ind],
#    print

#df['docNum'] = df.index + 1
#df2['sub_cluster'] = model.labels_

#silhouette_score = metrics.silhouette_score(X, model.labels_, metric='euclidean')
#print "Silhouette Score: ", silhouette_score

#print df2.groupby(["sub_cluster"])["lineText"].agg(["count"])
#import pdb; pdb.set_trace()
#df2 = df.ix[(df.main_cluster==7)]

#df2 = df2[['sessionId', 'lineNum', 'sub_cluster']]


#df = pd.merge(df, df2, on=['sessionId','lineNum'], how='left')

#df.to_csv('Grainger_maksed_50_clusters_20_sub_output.csv', index=False, header=True)

#print df.groupby(['cluster']).agg(['count'])
#import pdb; pdb.set_trace()
