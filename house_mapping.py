"""
by 地址名降至 tw 地址名

"""

import pymssql
import re
import pandas as pd
from collections import namedtuple
from copy import copy


STRA_ID = [2, 3]

LETTER2NUMBER = {
    'A': '1',
    'B': '2',
    'C': '3',
    'D': '4',
    'E': '5',
    'F': '6',
    'G': '7',
    'H': '8',
    'I': '9',
    'J': '10',
    'K': '11',
    'L': '12',
    'M': '13',
    'N': '14',
    'O': '15',
    'P': '16',
    'Q': '17',
    'R': '18',
    'S': '19',
    'T': '20',
    'U': '21',
    'V': '22',
    'W': '23',
    'X': '24',
    'Y': '25',
    'Z': '26',
}


MappingResult = namedtuple(
    'MappingResult',
    [
        'RSPRoomId',
        'StrategyId',
        'RoomId',
        'SourcePropertyAddress',
        'RSPPropertyAddress',
        'BuildingRSPId',
        'EstateRSPId',
    ]
)


table = pd.read_csv('data/rsp2source.csv')
RSP2SOURCE = {}
for _, item in table.iterrows():
    if item['RSPHeader'] not in RSP2SOURCE.keys():
        RSP2SOURCE[item['RSPHeader']] = []

    RSP2SOURCE[item['RSPHeader']].append(item['SourceHeader'])

table = pd.read_csv('data/source2rsp.csv')
SOURCE2RSP = {}
for _, item in table.iterrows():
    if item['SourceHeader'] not in SOURCE2RSP.keys():
        SOURCE2RSP[item['SourceHeader']] = []

    SOURCE2RSP[item['SourceHeader']].append(item['RSPHeader'])

RSP_DATA = {}
SOURCE_DATA = {}

CONN = pymssql.connect(
    '10.10.201.17',
    # '10.55.5.7',
    'bigdata_user',
    'ulyhx3rxqhtw',
    'TWEstate2',
)


def address_header(address):
    return re.match('[\u4e00-\u9fa5]*', address).group()


def clean(matched):
    value = int(matched.group('value'))
    return str(value)


def letter2number(matched):
    return LETTER2NUMBER[matched.group('value')]


def addsuffix(matched):
    return ''.join([matched.group('value'), '室'])


def strategy3(address):
    """
    字符串数字去零，字母转为数字，加室
    """
    address = address.upper()
    address = re.sub(r'(?P<value>\d+)', clean, address)  # 字符串数字去零
    address = re.sub(r'(?P<value>[A-Z])', letter2number, address)  # 字母转为数字
    address = re.sub(r'(?P<value>\d+$)', addsuffix, address)  # 加室

    return address


def splitaddress(address, matched):

    _min, _max = matched.group().split('-')

    split_list = []
    for i in range(int(_min), int(_max)+1, 1):
        split_list.append( address.replace( matched.group(), str(i) ) )

    return split_list


def splithouse(houselist, addressname, idname, headername):

    full_data = []

    for item in houselist:
        matched = re.search(r'\d+-\d+', item[addressname])

        """不可分裂"""
        if not matched:
            full_data.append(item)
            continue

        split_list = splitaddress(item[addressname], matched)

        for address in split_list:
            full_data.append({
                headername: item[headername],
                addressname: address,
                idname: item[idname],
            })

    return full_data

def get_source_data():
    cursor = CONN.cursor(as_dict=True)
    sql = """
    SELECT
           RoomId SourceRoomId,
           PropertyAddress SourcePropertyAddress
      FROM TWEstate2.[by].[byRoom]
     WHERE RoomRspId is NULL
       AND PropertyAddress IS NOT NULL
       AND DeleteFlag = 0
    """
    cursor.execute(sql)
    source_data = cursor.fetchall()

    full_data = []
    for item in source_data:
        SOURCE_DATA[item['SourceRoomId']] = item['SourcePropertyAddress']
        item['SourceHeader'] = address_header(item['SourcePropertyAddress'])
        full_data.append(item)

        """有对等地址名"""
        equal_headers = SOURCE2RSP.get(item['SourceHeader'])
        if equal_headers:
            for header in equal_headers:
                tmp = copy(item)
                origin = tmp['SourceHeader']
                tmp['SourceHeader'] = header
                tmp['SourcePropertyAddress'] = tmp['SourcePropertyAddress'].replace(origin, header)
                full_data.append(tmp)

    full_data = splithouse(full_data, 'SourcePropertyAddress', 'SourceRoomId', 'SourceHeader')

    for item in full_data:
        item[3] = strategy3(item['SourcePropertyAddress'])

    return full_data


def get_rsp_data():
    cursor = CONN.cursor(as_dict=True)
    sql = """
    SELECT
           RoomId RSPRoomId,
           PropertyAddress RSPPropertyAddress,
           BuildingId RSPBuildingId,
           EstateId RSPEstateId
      FROM TWEstate2.dbo.Room 
     WHERE RoomId NOT IN (
         SELECT DISTINCT RoomRspId
                    FROM TWEstate2.[by].[byRoom]
                   WHERE RoomRspId IS NOT NULL)
       AND PropertyAddress IS NOT NULL
       AND DeleteFlag = 0
    """
    cursor.execute(sql)
    rsp_data = cursor.fetchall()

    full_data = []
    for item in rsp_data:
        RSP_DATA[item['RSPRoomId']] = item['RSPPropertyAddress']
        item['RSPHeader'] = address_header(item['RSPPropertyAddress'])
        full_data.append(item)

        """有对等地址名"""
        equal_headers = RSP2SOURCE.get(item['RSPHeader'])
        if equal_headers:
            for header in equal_headers:
                tmp = copy(item)
                origin = tmp['RSPHeader']
                tmp['RSPHeader'] = header
                tmp['RSPPropertyAddress'] = tmp['RSPPropertyAddress'].replace(origin, header)
                full_data.append(tmp)

    full_data = splithouse(full_data, 'RSPPropertyAddress', 'RSPRoomId', 'RSPHeader')

    for item in full_data:
        item['RSPPropertyAddress'] = strategy3(item['RSPPropertyAddress'])

    return full_data


def house_mapping():
    source_data = get_source_data()
    print( 'source data fetched! %d rows.' % len(source_data) )

    rsp_data = get_rsp_data()
    print( 'rsp data fetched! %d rows.' % len(rsp_data) )

    header_list = set(item['SourceHeader'] for item in source_data)

    source_header_data = {}
    rsp_header_data = {}
    for header in header_list:
        source_header_data[header] = []
        rsp_header_data[header]    = {}

    for item in source_data:
        header = item['SourceHeader']
        source_header_data[header].append(item)

    print('source header finished!')

    for item in rsp_data:
        header = item['RSPHeader']
        if header in header_list:
            rsp_header_data[header][item['RSPPropertyAddress']] = item

    print('rsp header finished!')

    # source_header_data['张杨路'][0][2] = list(rsp_header_data['张杨路'].keys())[0]

    mapping_result = []
    source_id_list = set()
    rsp_id_list = set()
    count = 0
    for header, source_data in source_header_data.items():
        rsp_mapping = rsp_header_data[header]

        for item in source_data:
            
            if item['SourceRoomId'] in source_id_list:
                continue

            for strategy_id in STRA_ID:
                if strategy_id == 2: address = item['SourcePropertyAddress']
                else:                address = item[strategy_id]

                matched = rsp_mapping.get(address)

                if matched:
                    if matched['RSPRoomId'] in rsp_id_list:
                        continue

                    mapping_result.append(
                        MappingResult(
                            RSPRoomId             = matched['RSPRoomId'],
                            StrategyId            = strategy_id,
                            RoomId                = item['SourceRoomId'],
                            SourcePropertyAddress = SOURCE_DATA[item['SourceRoomId']],
                            RSPPropertyAddress    = RSP_DATA[matched['RSPRoomId']],
                            BuildingRSPId         = matched['RSPBuildingId'],
                            EstateRSPId           = matched['RSPEstateId'],
                        )
                    )
                    source_id_list.add(item['SourceRoomId'])
                    rsp_id_list.add(matched['RSPRoomId'])
                    break  # 只匹配一次

        count += 1
        print( 'header %s is mapped! (%d - %d)' % ( header, count, len(source_header_data) ) )

    print('mapping is finished')

    mapping_table_result = pd.DataFrame(mapping_result)
    mapping_table_result.to_csv('mapping_table_result.csv', index=False)
    print('csv result saved')

    """上传结果数据"""

    cursor = CONN.cursor()
    update_sql = """
    UPDATE TWEstate2.[by].[byRoom]
       SET RoomRspId = %s, StrategyId = %s, BuildingRSPId = %s, EstateRSPId = %s
     WHERE RoomId = %s
    """
    update_list = [(item.RSPRoomId, item.StrategyId, item.BuildingRSPId, item.EstateRSPId, item.RoomId) for item in mapping_result]
    print('ready to update')

    while update_list:
        tmp_list = []
        while update_list and len(tmp_list) < 1000:
                tmp_list.append(update_list.pop())

        cursor.executemany(update_sql, tmp_list)
        CONN.commit()
        print( 'update % rows, %d remain' % ( len(tmp_list), len(update_list) ) )

    print('result saved to database')

if __name__ == '__main__':
    house_mapping()
