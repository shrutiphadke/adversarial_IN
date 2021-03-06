import json
from pytangle.api import API
api = API()

# get all posts from a list from Jan until June 2020
list_ids = [1657193] # ids of the lists of interest


startends = [('2021-01-01', '2021-02-01'), ('2021-02-01', '2021-03-01'), ('2021-03-01', '2021-04-01'), ('2021-04-01', '2021-05-01'), ('2021-05-01', '2021-06-01'), ('2021-06-01', '2021-07-01'), ('2021-07-01', '2021-08-01'), ('2021-08-01', '2021-09-01'), ('2021-09-01', '2021-10-01'), ('2021-10-01', '2021-11-01'), ('2021-11-01', '2021-12-01')]
#, ('2018-03-01', '2018-05-01'), ('2018-05-01', '2018-07-01'), ('2018-07-01', '2018-09-01'), ('2018-09-01', '2018-11-01'), ('2018-11-01', '2019-01-01'), ('2019-01-01', '2019-03-01'), ('2019-03-01', '2019-05-01'), ('2019-05-01', '2019-07-01'), ('2019-07-01', '2019-09-01'), ('2019-09-01', '2019-11-01'), ('2019-11-01', '2020-01-01')]

for s in startends:
    fname = "/data/shruti/ONR/big_data/facebook/groups/re_hindi/" + s[0] + "_" + s[1] + ".json"
    with open(fname, 'w', encoding='utf-8') as output_file:
        for n, a_post in enumerate(api.posts(listIds=list_ids,
                                             count=-1,
                                             batchSize=100,
                                             sortBy='date',
                                             startDate=s[0],
                                             endDate=s[1],
                                             timeframe=None,
                                             types='link,status,tweet'
                                             )):
            # do something with the post
            json.dump(a_post, output_file)
            output_file.write("\n")
            if not n % 1000:                
                print(type(a_post))
                print(n)