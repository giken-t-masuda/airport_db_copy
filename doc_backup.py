import os
import requests
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import datetime

"""
AIRPORT v1 v2 を version3にあげるためのスクリプト
ファイル入力として、以下の記述内容とする

sourceid , destinationid

例） ClientIDが1000123からClientID1000234にDBをコピーしたい場合
1000123,1000234
と記載する。

"""

# Global API
url = "http://localhost:8888/"

# ClientIDからcustomerIDを取得
def get_customer_id(client_id):
    # DynamoDBへの接続を確立
    dynamodb = boto3.resource('dynamodb')
    # 対象のテーブルを指定
    table = dynamodb.Table('Client_Info')
    # レスポンス
    response = None
    try:
        # ClientIDが特定の値（ここでは1000123）であるアイテムを取得
        response = table.get_item(Key={'ClientID': int(client_id)})

        # アイテムが存在するか確認
        if 'Item' in response:
            item = response['Item']
            customer_id = item.get('CustomerID', None)
            response = customer_id
    except NoCredentialsError:
        print("Credentials not available")
    except PartialCredentialsError:
        print("Incomplete credentials")
    except Exception as e:
        print(e)
    finally:
        return response


# バックアップ / 復元 API処理
def post_db_backup_restore(url, endpoint:str = "backup",data=None, json=None):
    """
    指定されたURLに対してPOSTリクエストを行う関数。

    Args:
        url (str): リクエストを送信するURL
        data (dict, optional): フォームデータ
        json (dict, optional): JSONデータ

    Returns:
        dict: レスポンスのJSONデータ
    """
    try:
        response = requests.post(f'{url}/{endpoint}', data=data, json=json, timeout=3600)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

# パラメータ作成　自己バックアップ
def create_param( clientid, customerid:str = None ):
    today = datetime.datetime.now().strftime("%Y%m%d")

    _customerid = get_customer_id(clientid)
    _final_customerid = f"no_data_for_{clientid}" if _customerid == None else _customerid

    backup_param = {
        "src_uri": "mongodb://giken:gikentrastem@documentdb-4.cluster-czpctwlkkfcu.ap-northeast-1.docdb.amazonaws.com:27017",
        "src_db": f"{clientid}_DB",
        "src_addition": [
            "replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
        ],
        "file_type": "gz",
        "s3_bucket": "passer-cloud-customerdata",
        "s3_path": f"{_final_customerid}/{today}_backup"
    }
    return backup_param

# 復元パラメータの作成
def create_param_restore( clientid, targetid:str = None ):
    """
    clientid: 復元元のClientID --- s3のファイル指定で必要
    targetid: 復元先のClientID --- customeridを求める必要あり
    """
    today = datetime.datetime.now().strftime("%Y%m%d")

    _targetid = get_customer_id(targetid)
    
    # debug目的で、DBはgiken-devとする
    restore_param = {
        "dst_uri": "mongodb://giken:gikentrastem@dev.cluster-ctvfgbdpqg6c.ap-northeast-1.docdb.amazonaws.com:27017",
        "dst_db": f"{targetid}_DB",
        "dst_addition": [
            "authSource=admin"
        ],
        "s3_bucket": "passer-cloud-customerdata",
        "s3_key": f"{_targetid}/{clientid}_backup/{clientid}_DB.gz"
    }
    return restore_param

# パラメータ作成　移行先S3へのバックアップ
def create_param_to_target( clientid, target_id:str = None ):
    today = datetime.datetime.now().strftime("%Y%m%d")

    _customerid = get_customer_id(target_id)

    backup_param = {
        "src_uri": "mongodb://giken:gikentrastem@documentdb-4.cluster-czpctwlkkfcu.ap-northeast-1.docdb.amazonaws.com:27017",
        "src_db": f"{clientid}_DB",
        "src_addition": [
            "replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
        ],
        "file_type": "gz",
        "s3_bucket": "passer-cloud-customerdata",
        "s3_path": f"{target_id}/{clientid}_backup"
    }
    return backup_param


# ファイルを読みこんでループ処理する関数
def read_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                list = line.strip().split(",")
                _customer_id = get_customer_id(list[0])
                _destinatin_id = get_customer_id(list[1])
                # ソース・デスティネーション双方が揃う場合に処理を開始
                if (_customer_id and _destinatin_id) != None:
                    #print("処理開始")
                    # データのバックアップを移行先S3に保存
                    _json = create_param_to_target(clientid=list[0], target_id=_destinatin_id)
                    #print(_json)
                    _res = post_db_backup_restore(url, json=_json)
                    _res["ClientID"] = list[0]
                    _res["TargetID"] = list[1]
                    print(_res)
                    if "Exception" not in _res["message"]:
                        print("DocDBへ復元開始")
                        _param_restore = create_param_restore(list[0],list[1])
                        _res_restore = post_db_backup_restore(url, endpoint="restore", json=_param_restore)
                        print(_res_restore)
                else:
                    print("処理しない")

                
    except FileNotFoundError:
        print(f"ファイルが見つかりません: {file_path}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")


# backupURL


#print(create_param(clientid=1000740))

read_file("/home/giken/Develop/airportv3-pj/task_list3")
