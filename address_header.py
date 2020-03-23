import pymssql
import re
from collections import namedtuple
import pandas as pd


Pair = namedtuple(
    'Pair',
    [
        'SourceHeader',
        'RSPHeader',
    ]
)


CONN = pymssql.connect(
    '10.10.201.17',
    # '10.55.5.7',
    'tw_user',
    '123456',
    'TWEstate',
)
cursor = CONN.cursor(as_dict=True)


def address_header(address):
    return re.match('[\u4e00-\u9fa5]*', address).group()


sql = """
SELECT
       RoomId SourceRoomId,
       PropertyAddress SourcePropertyAddress
    FROM TWEstate.[by].[byRoom]
    WHERE PropertyAddress IS NOT NULL
    AND DeleteFlag = 0
"""
cursor.execute(sql)
source_data = cursor.fetchall()

source_header = set(
    address_header(item['SourcePropertyAddress'])
    for item in source_data
)

source_header = sorted(list(source_header))
if '' in source_header:
    source_header.remove('')

with open('data/source_header.txt', 'w') as fp:
    fp.write('\n'.join(source_header))



sql = """
SELECT
        RoomId RSPRoomId,
        PropertyAddress RSPPropertyAddress
    FROM TWEstate.dbo.Room 
    WHERE PropertyAddress IS NOT NULL
    AND DeleteFlag = 0
"""
cursor.execute(sql)
RSP_data = cursor.fetchall()

RSP_header = set(
    address_header(item['RSPPropertyAddress'])
    for item in RSP_data
)

RSP_header = sorted(list(RSP_header))
if '' in RSP_header:
    RSP_header.remove('')

with open('data/RSP_header.txt', 'w') as fp:
    fp.write('\n'.join(RSP_header))

source2rsp = []
rsp2source = []

for source in source_header:
    if len(source) <= 2:
        continue

    for rsp in RSP_header:
        if source == rsp:
            continue
        if len(rsp) <= 2:
            continue

        if source in rsp:
            rsp2source.append(Pair(SourceHeader=source, RSPHeader=rsp))
        if rsp in source:
            source2rsp.append(Pair(SourceHeader=source, RSPHeader=rsp))

pd.DataFrame(source2rsp).to_csv('data/source2rsp.csv', index=None)
pd.DataFrame(rsp2source).to_csv('data/rsp2source.csv', index=None)
