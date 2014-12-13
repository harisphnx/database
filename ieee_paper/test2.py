import json

with open('relation_reduce.json', 'r+') as f:    ### contais the reduction fraction of tables when semi-joins is applied ###
    relation_reduce = json.load(f)
temp = min(relation_reduce, key=relation_reduce.get)
print temp
relation_reduce.pop(temp, None)
temp = min(relation_reduce, key=relation_reduce.get)
print temp
relation_reduce.pop(temp, None)
temp = min(relation_reduce, key=relation_reduce.get)
print temp
relation_reduce.pop(temp, None)
temp = min(relation_reduce, key=relation_reduce.get)
print temp

