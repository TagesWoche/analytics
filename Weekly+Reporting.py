
# coding: utf-8

# In[3]:

from datetime import datetime

import os

import glob
import pandas as pd
import numpy as np

from pandas.io.json import json_normalize

import urllib.request
import json
import codecs
import facebook


# In[28]:

token = 'EAACEdEose0cBAHccUgjgrDnLQ0pIZAlHU2Kvhy3CMOpwRvZAxYi1OXWZAjf672lHZAxridZCwyapaD7DpZC6lYTDZBh4t1UcVITEOEs7IvUuRBfwuP4TuDx9Eoc1BTU3Pe6kC3SCJjZCjEiYFZCwM6l0b5rr6Wiv9G0OH6z2pN9MsSE9DrIKJC0iHX7jQdYatvxoZD'
graph = facebook.GraphAPI(access_token=token, version='2.2')


# In[63]:

## variables

kennzahl_shares = 66.8
kennzahl_pageviews = 2045.5
kennzahl_kommentare = 6.6
kennzahl_readtime = 186.6


# ## Setup metaInfo

# In[64]:

week = 'KW 2'
id = '2'


# In[66]:

metaInfo_current = pd.DataFrame(columns=['id', 'week', 'multisession', 'nutzer', 'published_articles', 
                                 'pageviews_news', 'pageviews_newswire', 'pageviews_total', 'facebook_interaktionen'])

metaInfo_new = ['', '', '', '', '', '', '', '', '']  
metaInfo_current.loc[len(metaInfo_current)] = metaInfo_new


# In[67]:

metaInfo_current['week'] = week
metaInfo_current['id'] = id
metaInfo_current['multisession'] = 2726
metaInfo_current['nutzer'] = 78513

metaInfo_current


# ## Load and prepare google stats

# In[68]:

## load google analyctics data

ga_stats = 'weekly_report/weekly_ga_stats_kw2.csv'

stats_weekly = pd.read_csv(ga_stats)

stats_weekly['url'] = stats_weekly['Seite'].str.split('/')

for i in range(0, len(stats_weekly['url'])):   
    try:
        stats_weekly.loc[i, 'article_id'] = stats_weekly['url'][i][4]
    except:
        stats_weekly.loc[i, 'article_id'] = ''
        
stats_weekly['article_id'] = pd.to_numeric(stats_weekly['article_id'], errors='coerce')
stats_weekly['Seitenaufrufe'] = pd.to_numeric(stats_weekly['Seitenaufrufe'], errors='coerce')

stats_weekly = stats_weekly.dropna()

stats_weekly.head()


# In[70]:

stats_weekly['time'] = pd.DatetimeIndex(stats_weekly['Durchschn Besuchszeit auf Seite'])
stats_weekly['readtime'] = stats_weekly['time'].dt.minute * 60 + stats_weekly['time'].dt.second


# In[71]:

stats_readtime = stats_weekly.groupby(by=['article_id']).readtime.mean()
stats_readtime = stats_readtime.to_frame(name = 'readtime')
stats_readtime = stats_readtime.reset_index()

stats_readtime.head()


# In[73]:

stats_pageviews = stats_weekly.groupby(by=['article_id']).Seitenaufrufe.sum()
stats_pageviews = stats_pageviews.to_frame(name = 'Seitenaufrufe')
stats_pageviews = stats_pageviews.reset_index()

stats_pageviews.head()


# In[74]:

stats = stats_pageviews.merge(stats_readtime, left_on='article_id', right_on='article_id')


# ## Get meta data of articles

# In[75]:

def parse_articles(id):
    meta = pd.DataFrame(columns=['type', 'article_id'])
    url = 'http://www.tageswoche.ch/content-api/articles/'
    article_name = url + str(id)
    #print(id)
    try:
        df = pd.read_json(article_name, lines=True)
        meta['type'] = df['type']
        meta['published'] = df['published']
        meta['body'] = df['fields'][0]['body']
        meta['title'] = df['title']
        meta['url'] = df['url']
        meta['comments'] = df['comments_count']
    except:
        pass
    meta['article_id'] = id
    return meta

filepath = 'weekly_report/tawo_stats_meta_' + week + '.csv'

if os.path.exists(filepath):
    df = pd.read_csv(filepath, usecols=[1, 2, 3, 4, 5])
else:
    df = pd.concat([parse_articles(id) for id in stats['article_id']])

df.head()


# In[12]:

df['published'] = df['published'].str.extract('(\d\d\d\d-\d\d-\d\d)')

df = df[df.published != '0001-11-30']

df['published'] = pd.to_datetime(df['published'], format='%Y-%m-%d')

df.head()


# In[9]:

df.to_csv(filepath)


# In[13]:

# merge google stats and article meta data

df = df.merge(stats_weekly, left_on='article_id', right_on='article_id')

df.head()


# ## Prepare subsets for news and newswire

# In[14]:

df['type'] = df['type'].astype('category')

df_news = df[(df.type == 'news')]

df_news.head()


# In[15]:

data_subset_news = df_news.set_index(['published'])

data_subset_news_week = data_subset_news.loc['2017-01-09':'2017-01-15']


# In[16]:

# Save metainfos

metaInfo_current['pageviews_news'] = data_subset_news_week['Seitenaufrufe'].sum()


# In[17]:

data_subset_news_week = data_subset_news_week.reset_index()


# In[18]:

# Save metainfos

metaInfo_current['published_articles'] = data_subset_news_week['article_id'].nunique()


# In[19]:

df_newswire = df[(df.type == 'newswire')]

data_subset_newswire = df_newswire.set_index(['published'])

data_subset_newswire_week = data_subset_newswire.loc['2017-01-09':'2017-01-15']


# In[20]:

# Save metainfos

metaInfo_current['pageviews_newswire'] = data_subset_newswire_week['Seitenaufrufe'].sum()


# In[21]:

# Save metainfos

metaInfo_current['pageviews_total'] = metaInfo_current['pageviews_news'] + metaInfo_current['pageviews_newswire']


# ## Get Facebook Stats

# In[22]:

url = data_subset_news_week['url'][0]


# In[23]:

post = graph.get_object(id=url)

post


# In[61]:

def get_facebook_shares(url):
    shares = pd.DataFrame(columns=['url', 'share_count'])
    shares_new = ['', '']  
    shares.loc[len(shares)] = shares_new
    try:
        post = graph.get_object(id=url)
        shares['share_count'] = post['share']['share_count']
        shares['url'] = url
    except:
        pass
        
    return shares

share_stats = pd.concat([get_facebook_shares(url) for url in data_subset_news_week['url']])

share_stats = share_stats.reset_index()
share_stats = share_stats[[1,2]]

share_stats.head()


# In[62]:

share_stats


# In[19]:

# Show metainfos

metaInfo_old = pd.read_csv('weekly_report/metainfo.csv', usecols=[1,2,3,4,5,6,7])

metaInfo = metaInfo_old.append(metaInfo_current, ignore_index=True)

metaInfo_current

metaInfo.to_csv('weekly_report/metainfo.csv')

metaInfo


# In[20]:

top5 = data_subset_news_week.sort_values('Seitenaufrufe', ascending=False).head(5)


# In[21]:

top5['week'] = week


# In[22]:

top5_series = (top5.groupby(['week'])
       .apply(lambda x: x[['Seitenaufrufe','article_id', 'title', 'url']].to_dict('r'))
       .rename(columns={0:'top'}))
top5_series


# In[23]:

flop5 = data_subset_news_week.sort_values('Seitenaufrufe', ascending=True).head(5)


# In[24]:

flop5['week'] = week


# In[25]:

flop5_series = (flop5.groupby(['week'])
       .apply(lambda x: x[['Seitenaufrufe','article_id', 'title', 'url']].to_dict('r'))
       .rename(columns={0:'flop'}))
flop5_series


# In[26]:

cols = "id week multisession	nutzer	published_articles	pageviews_news	pageviews_newswire	pageviews_total".split()
j = metaInfo_current[cols].groupby("week").first()
       #.to_json(orient='records', date_format='iso'))
j['top'] = top5_series
j['flop'] = flop5_series
j = j.reset_index()
j = j.to_json(orient='records')


# In[27]:

with open('weekly_report/weekly_report.json', 'w') as fp:
    json.dump(json.loads(j), fp, indent=2, sort_keys=True)

print(json.dumps(json.loads(j), indent=2, sort_keys=True))


# In[36]:

data_subset_topics = df.set_index(['published'])

data_subset_topics = data_subset_topics.loc['2017-01-23':'2017-01-29']

data_subset_topics = data_subset_topics.reset_index()


# In[37]:

data_subset_topics


# In[38]:

def get_topics(id):
    result = pd.DataFrame(columns=['topic', 'article_id'])
    i = 0;
    url = 'http://www.tageswoche.ch/content-api/articles/' + str(id)
    try:
        response = urllib.request.urlopen(url)
        reader = codecs.getreader("utf-8")
        obj = json.load(reader(response))
        for i in range(0, len(obj['topics'])):
            result.loc[i] = obj['topics'][i]['title']
    except:
        result['topic'] = ''
        pass
    print(id)
    result['article_id'] = id
    return result #create a series

topics = pd.concat([get_topics(id) for id in data_subset_topics['article_id']])

topics


# In[39]:

topics = topics[['topic', 'id']]

topics.head()


# In[40]:

topics = topics.reset_index()

topics


# In[41]:

topics = topics[['topic', 'id']]


# In[42]:

topics_stats = topics.merge(stats_weekly, left_on='id', right_on='article_id')

topics_stats.head()


# In[43]:

pageviews_topics = topics_stats.groupby(by=['topic'])['Seitenaufrufe'].sum().sort_values(ascending=False)


# In[44]:

pageviews_topics

