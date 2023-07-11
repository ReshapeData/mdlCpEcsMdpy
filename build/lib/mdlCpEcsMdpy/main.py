#!/usr/bin/env python
# -*- coding: utf-8 -*-
from . import material
from . import supplier
from . import customer
import pandas as pd
from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk
import json

def FBillNo_sync(token, category, FNumber,FName="赛普集团新账套"):
    '''
    按照单据编号同步
    :param app2:
    :param app3:
    :param option:
    :param category:
    :param FNumber:
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    # app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        # "app_sec": 'd019b038bc3c4b02b962e1756f49e179',
        "app_sec": key[0]["app_sec"],
        # "server_url": 'http://192.168.1.13/K3Cloud',
        "server_url": key[0]["server_url"],
    }

    data = []

    if category == '客户':

        # 客户的名称
        l = []
        l.append(FNumber)

        data=customer.CUSTOMERNAME_get_ECS(app2=app2,app3=app3,option1=option,CUSTOMERNAMES=l)

        pass
    if category == '物料':
        # 列表格式

        l=[]
        l.append(FNumber)

        data=material.performFNumber(app2=app2,app3=app3,option1=option,codeList=l)
        pass
    if category == '供应商':
        # 列表格式
        l=[]
        l.append(FNumber)
        data=supplier.FNAME_get_supplier(app2=app2,app3=app3,option1=option,fname_list=l)
        pass

    return data


def FBillNo_sync_byDate(token, category, FDate,FName="赛普集团新账套"):
    '''
    按照日期同步
    :param app2:
    :param app3:
    :param option:
    :param category:
    :param FNumber:
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    # app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        # "app_sec": 'd019b038bc3c4b02b962e1756f49e179',
        "app_sec": key[0]["app_sec"],
        # "server_url": 'http://192.168.1.13/K3Cloud',
        "server_url": key[0]["server_url"],
    }

    data = []

    if category == '客户':

        # 客户的名称
        data=customer.FCREATEDATE_get_ECS(app2=app2,app3=app3,option1=option,starttime=FDate, endtime=FDate)

        pass
    if category == '物料':
        # 列表格式
        data=material.performFNumber_bydate(app2=app2,app3=app3,option1=option,FDate=FDate)
        pass
    if category == '供应商':
        # 列表格式

        data=supplier.FNAME_get_supplier_bydate(app2=app2,app3=app3,option1=option,Fdate=FDate)
        pass

    return data


def getDataSource_byOrder(app3, tablename, field, FNumber):
    '''
    通过单据编号获得数据
    :param FNumber:
    :return:
    '''
    sql = f"""select * from {tablename} where {field}='{FNumber}'"""

    res = app3.select(sql)

    return res


def SRCTable_query(token, category, FNumber):
    '''
    按照单据编号查询
    :param app3:
    :param category:
    :param FNumber:
    :return:
    '''

    app3 = RdClient(token=token)

    data = []

    if category == '客户':
        data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_BD_CUSTOMER", field="FNumber",
                                     FNumber=FNumber)

        pass
    if category == '物料':
        data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_bd_MaterialDetail", field="FNumber",
                                     FNumber=FNumber)

        pass
    if category == '供应商':
        data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_bd_SupplierDetail", field="FNumber",
                                     FNumber=FNumber)

        pass

    res = pd.DataFrame(data)

    return res



def getDataSource_byDate(app3, tablename, field, FStartDate):
    '''
    通过日期获得数据
    :param FNumber:
    :return:
    '''
    # sql = f"""select * from {tablename} where {field}>='{FStartDate}' and {field}<='{FEndDate}'"""

    sql = f"""select * from {tablename} where {field} like '{FStartDate}'"""

    res = app3.select(sql)

    df=pd.DataFrame(res)

    return df

def SRCTable_queryByDate(token, category, FStartDate):
    '''
    按照日期查询
    :param app3:
    :param category:
    :param FNumber:
    :return:
    '''

    app3 = RdClient(token=token)

    data = []

    if category == '客户':
        data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_BD_CUSTOMER", field="FDate",
                                    FStartDate=FStartDate)

        pass
    if category == '物料':
        data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_bd_MaterialDetail", field="FVarDateTime",
                                    FStartDate=FStartDate)

        pass
    if category == '供应商':
        data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_bd_SupplierDetail", field="FDate",
                                    FStartDate=FStartDate)

        pass

    res = pd.DataFrame(data)

    return res


def ERPData_query(api_sdk, option, FNumber, Formid):
    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])
    model = {
        "CreateOrgId": 0,
        "Number": FNumber,
        "Id": "",
        "IsSortBySeq": "false"
    }

    res = api_sdk.View(Formid, model)

    result = json.loads(res)

    if result["Result"]["ResponseStatus"]["IsSuccess"]:

        return "单据已存在ERP系统"

    else:
        return "单据未存在金蝶系统"


def ERPDATA_queryByOrder(token,category, FNumber,FName="赛普集团新账套"):
    '''
    按照单据编号查询
    :param app3:
    :param category:
    :param FNumber:
    :return:
    '''
    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql=f"select * from rds_key_values where FName='{FName}'"

    key=app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        # "app_sec": 'd019b038bc3c4b02b962e1756f49e179',
        "app_sec": key[0]["app_sec"],
        # "server_url": 'http://192.168.1.13/K3Cloud',
        "server_url": key[0]["server_url"],
    }

    if category == '客户':
        res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="BD_Customer")

        return res
    if category == '物料':
        res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="BD_MATERIAL")

        return res
    if category == '供应商':
        res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="BD_Supplier")

        return res


def Status_upload(app3, tablename, field, FNumber):
    '''
    修改单据状态
    :param app3:
    :param tablename:
    :param field:
    :param FNumber:
    :return:
    '''

    sql = f"update a set a.{field}=0 from {tablename} a where a.{field}='{FNumber}'"

    app3.update(sql)

    return "单据状态修改已完成"


def deleteData(app3,FTableName,field,FNumber):

    sql=f"""
    delete from {FTableName} where {field}='{FNumber}'
    """

    app3.update(sql)



def FBillStatus_upload(token,category, FNumber,FName="赛普集团新账套"):
    '''
    单据状态修改
    :param app3:
    :param category:
    :param FNumber:
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    # app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        # "app_sec": 'd019b038bc3c4b02b962e1756f49e179',
        "app_sec": key[0]["app_sec"],
        # "server_url": 'http://192.168.1.13/K3Cloud',
        "server_url": key[0]["server_url"],
    }

    data = ""

    if category == '客户':

        # 通过客户名称

        deleteData(app3=app3, FTableName="RDS_ECS_src_BD_CUSTOMER", field="FName", FNumber=FNumber)

        deleteData(app3=app3, FTableName="RDS_ECS_ods_BD_CUSTOMER", field="FName", FNumber=FNumber)
        l=[]
        l.append(FNumber)

        customer.CUSTOMERNAME_get_ECS(app2=app2,app3=app3,option1=option,CUSTOMERNAMES=l)

    if category == '物料':

        deleteData(app3=app3, FTableName="RDS_ECS_src_bd_MaterialDetail", field="FNumber", FNumber=FNumber)

        deleteData(app3=app3, FTableName="RDS_ECS_ods_bd_MaterialDetail", field="FNumber", FNumber=FNumber)
        l = []

        l.append(FNumber)

        material.performFNumber(app2=app2,app3=app3,option1=option,codeList=l)


    if category == '供应商':

        deleteData(app3=app3, FTableName="RDS_ECS_src_bd_SupplierDetail", field="FName", FNumber=FNumber)

        deleteData(app3=app3, FTableName="RDS_ECS_ods_bd_SupplierDetail", field="FName", FNumber=FNumber)
        l = []

        l.append(FNumber)

        supplier.FNAME_get_supplier(app2=app2,app3=app3,option1=option,fname_list=l)


    return data


def log_query(token,FNumber):
    '''
    日志查询
    :param app3:
    :param FNumber:
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df
