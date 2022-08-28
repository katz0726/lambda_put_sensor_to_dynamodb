from __future__ import print_function
import base64
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import os
import traceback

#-----Dynamo Info change here------
TABLE_NAME = os.environ.get('TABLE_NAME', "default")
DDB_PRIMARY_KEY = "deviceid"
DDB_SORT_KEY = "updated_at"
DDB_ATTR = ["temperature", "humidity", "moisture"]
#-----Dynamo Info change here------

dynamodb = boto3.resource('dynamodb')
table  = dynamodb.Table(TABLE_NAME)

'''
This Kinesis data(Json Image) is below
{
    "DEVICE_NAME": $device_name,
    "UPDATED_AT": $TimeStamp(yyyy-mm-ddThh:MM:SS),
    "MOISTURE" : int
}
'''
def checkItem(str_data):
    try:
        #String to Json object
        json_data = json.loads(str_data)
        # adjust your data format
        resDict = {
            DDB_PRIMARY_KEY:json_data['DEVICE_NAME'],
            "TEMPERATURE": json_data['TEMPERATURE'],
            "HUMIDITY": json_data['HUMIDITY'],
            "MOISTURE": json_data['MOISTURE'],
            "CREATED_AT": json_data['CREATED_AT'],
            DDB_SORT_KEY:json_data['UPDATED_AT']

        }
        print("resDict:{}".format(resDict))
        return resDict

    except Exception as e:
        print(traceback.format_exc())
        return None

def writeItemInfo(datas):
    ItemInfoDictList = []
    try:
        for data in datas:
            itemDict = checkItem(data)
            if None != itemDict:
                ItemInfoDictList.append(itemDict)
            # if data does not have key info, just pass
            else:
                print("Error data found:{}".format(data))
                pass

    except Exception as e:
        print(traceback.format_exc())
        print("Error on writeItemInfo")

    return ItemInfoDictList

def DynamoBulkPut(datas):
    try:
        putItemDictList = writeItemInfo(datas)
        with table.batch_writer() as batch:
            for putItemDict in putItemDictList:
                batch.put_item(Item = putItemDict)
        return

    except Exception as e:
        print("Error on DynamoBulkPut()")
        raise e

def decodeKinesisData(dataList):
    decodedList = []
    try:
        for data in dataList:
            payload =  base64.b64decode(data['kinesis']['data'])
            print("payload={}".format(payload))
            decodedList.append(payload)

        return decodedList

    except Exception as e:
        print("Error on decodeKinesisData()")
        raise e

#------------------------------------------------------------------------
# call by Lambda here.
#------------------------------------------------------------------------
def lambda_handler(event, context):
    print("lambda_handler start")

    try:
        print("---------------json inside----------------")
        print(json.dumps(event))
        encodeKinesisList = event['Records']
        print(encodeKinesisList)
        decodedKinesisList = decodeKinesisData(encodeKinesisList)
        # Dynamo Put
        if 0 < len(decodedKinesisList):
            DynamoBulkPut(decodedKinesisList)
        else:
            print("there is no valid data in Kinesis stream, all data passed")

        return

    except Exception as e:
        print(traceback.format_exc())
        # This is sample source. When error occur this return success and ignore the error.
        raise e
