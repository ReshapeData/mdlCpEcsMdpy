#!/usr/bin/env python
# -*- coding: utf-8 -*-
from . import material
from . import supplier
from . import customer
import pandas as pd
from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk
import json


def materialByFNumber_sync(token,FNumber,FName="赛普集团新账套"):
    '''
    物料按货号同步
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    l = []
    l.append(FNumber)

    data = material.performFNumber(app2=app2, app3=app3, option1=option, codeList=l)

    return data


def customerByFName_sync(token,FCustomerName,FName="赛普集团新账套"):

    '''
    客户按名称同步
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    # 客户的名称
    l = []

    l.append(FCustomerName)

    data = customer.CUSTOMERNAME_get_ECS(app2=app2, app3=app3, option1=option, CUSTOMERNAMES=l)

    return data



def supplierByFName_sync(token,FSupplierName,FName="赛普集团新账套"):

    '''
    供应商按名称同步
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    # 列表格式
    l = []
    l.append(FSupplierName)

    data = supplier.FNAME_get_supplier(app2=app2, app3=app3, option1=option, fname_list=l)

    return data



def materialByDate_sync(token,FDate,FName="赛普集团新账套"):
    '''
    供应商按名称日期
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    data=material.performFNumber_bydate(app2=app2,app3=app3,option1=option,FDate=FDate)
    
    return data


def customerByDate_sync(token,FDate,FName="赛普集团新账套"):

    '''
    供应商按名称日期
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    data = customer.FCREATEDATE_get_ECS(app2=app2, app3=app3, option1=option, starttime=FDate, endtime=FDate)

    return data



def supplierByDate_sync(token,FDate,FName="赛普集团新账套"):

    '''
    供应商按名称日期
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    data=supplier.FNAME_get_supplier_bydate(app2=app2,app3=app3,option1=option,Fdate=FDate)

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


def materialByFNumber_query(token,FNumber):
    '''
    物料按编号查询
    :return:
    '''

    app3 = RdClient(token=token)

    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_bd_MaterialDetail", field="FNumber",
                                 FNumber=FNumber)

    df=pd.DataFrame(data)

    return df


def customerByFNumber_query(token,FNumber):
    '''
    客户编号查询
    :return:
    '''

    app3 = RdClient(token=token)

    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_BD_CUSTOMER", field="FNumber",
                                 FNumber=FNumber)

    df = pd.DataFrame(data)

    return df


def supplierByFNumber_query(token,FNumber):
    '''
    供应商编号查询
    :return:
    '''

    app3 = RdClient(token=token)

    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_bd_SupplierDetail", field="FNumber",
                                 FNumber=FNumber)

    df = pd.DataFrame(data)

    return df




def getDataSource_byDate(app3, tablename, field, FStartDate):
    '''
    通过日期获得数据
    :param FNumber:
    :return:
    '''
    sql = f"""select * from {tablename} where {field} like '{FStartDate}'"""

    res = app3.select(sql)

    df=pd.DataFrame(res)

    return df



def materialByDate_query(token,FDate):
    '''
    物料按日期查询
    :return:
    '''

    app3 = RdClient(token=token)

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_bd_MaterialDetail", field="FVarDateTime",
                                FStartDate=FDate)

    df=pd.DataFrame(data)

    return df


def customerByDate_query(token,FDate):
    '''
    客户按日期查询
    :return:
    '''

    app3 = RdClient(token=token)

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_BD_CUSTOMER", field="FDate",
                                FStartDate=FDate)

    df = pd.DataFrame(data)

    return df


def supplierByDate_query(token,FDate):
    '''
    供应商按日期查询
    :return:
    '''

    app3 = RdClient(token=token)

    data = getDataSource_byDate(app3=app3, tablename="RDS_ECS_src_bd_SupplierDetail", field="FDate",
                                FStartDate=FDate)

    df = pd.DataFrame(data)

    return df


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


def materialErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    物料按编号查询
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="BD_MATERIAL")

    return res


def customerErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    客户按编号查询
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="BD_Customer")

    return res


def supplierErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    供应商按编号查询
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

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



def materialStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '''
    物料按货号修改
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)


    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    deleteData(app3=app3, FTableName="RDS_ECS_src_bd_MaterialDetail", field="FNumber", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_bd_MaterialDetail", field="FNumber", FNumber=FNumber)
    l = []

    l.append(FNumber)

    material.performFNumber(app2=app2, app3=app3, option1=option, codeList=l)

    return "修改成功"


def customerStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '''
    客户按名称修改
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    deleteData(app3=app3, FTableName="RDS_ECS_src_BD_CUSTOMER", field="FName", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_BD_CUSTOMER", field="FName", FNumber=FNumber)
    l = []
    l.append(FNumber)

    customer.CUSTOMERNAME_get_ECS(app2=app2, app3=app3, option1=option, CUSTOMERNAMES=l)

    return "修改成功"


def supplierStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '''
    供应商按名称修改
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    app2 = RdClient(token=key[0]["FApp2"])

    option = {
        "acct_id": key[0]["acct_id"],
        "user_name": key[0]["user_name"],
        "app_id": key[0]["app_id"],
        "app_sec": key[0]["app_sec"],
        "server_url": key[0]["server_url"],
    }

    deleteData(app3=app3, FTableName="RDS_ECS_src_bd_SupplierDetail", field="FName", FNumber=FNumber)

    deleteData(app3=app3, FTableName="RDS_ECS_ods_bd_SupplierDetail", field="FName", FNumber=FNumber)
    l = []

    l.append(FNumber)

    supplier.FNAME_get_supplier(app2=app2, app3=app3, option1=option, fname_list=l)

    return "修改成功"


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
