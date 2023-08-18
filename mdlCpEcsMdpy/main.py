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
    物料按编号同步
    :param token: 中台token
    :param FNumber: 编号
    :param FName: 账套名
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

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

        data =material.byFNumber_sync(app2=app2, app3=app3, option=option, codeList=l)

        # data = material.performFNumber(app2=app2, app3=app3, option1=option, codeList=l)

        return data

    else:

        return False


def customerByFNumber_sync(token,FNumber,FName="赛普集团新账套"):
    '''
    客户按编号同步
    :param token: 中台token
    :param FNumber: 编号
    :param FName: 账套
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }


        data = customer.CUSTOMERNumber_get_ECS(app2=app2, app3=app3, option1=option, FNumber=FNumber)

        return data

    else:

        return False



def supplierByFNumber_sync(token,FNumber,FName="赛普集团新账套"):
    '''
    供应商按编号同步
    :param token: 中台token
    :param FNumber: 编号
    :param FName: 账套名
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        # 列表格式

        data = supplier.FNumber_get_supplier(app2=app2, app3=app3, option1=option, FNumber=FNumber)

        return data

    else:

        return False




def materialByDate_sync(token,FDate,FName="赛普集团新账套"):
    '''
    物料按日期同步
    :param token: 中台数据库token
    :param FDate: 日期
    :param FName: ERP账套名
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

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

    else:

        return False


def customerByDate_sync(token,FDate,FName="赛普集团新账套"):
    '''
    客户按日期同步
    :param token: 中台token
    :param FDate: 日期
    :param FName: 账套名
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }


        data = customer.FCREATEDATE_get_ECS(app2=app2, app3=app3, option1=option, starttime=FDate)

        return data

    else:

        return False



def supplierByDate_sync(token,FDate,FName="赛普集团新账套"):
    '''
    供应商按日期同步
    :param token: 中台token
    :param FDate: 日期
    :param FName: 账套名
    :return:
    '''

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        app2 = RdClient(token=key[0]["FApp2"])

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        data=supplier.FDate_get_supplier_bydate(app2=app2,app3=app3,option1=option,Fdate=FDate)

        return data

    else:

        return False



def getDataSource_byOrder(app3, tablename, field, FNumber):
    '''
    通过编号获得数据
    :param FNumber:
    :return:
    '''
    sql = f"""select * from {tablename} where {field}='{FNumber}'"""

    res = app3.select(sql)

    return res


def materialByFNumber_query(token,FNumber):
    '''
    通过物料按编码查询
    :param token: 中台token
    :param FNumber: 编码
    :return:
    '''

    app3 = RdClient(token=token)

    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_bd_MaterialDetail", field="FNumber",
                                 FNumber=FNumber)

    df=pd.DataFrame(data)

    return df


def customerByFNumber_query(token,FNumber):
    '''
    通过客户编号查询
    :param token: 中台token
    :param FNumber: 编号
    :return:
    '''

    app3 = RdClient(token=token)

    data = getDataSource_byOrder(app3=app3, tablename="RDS_ECS_src_BD_CUSTOMER", field="FNumber",
                                 FNumber=FNumber)

    df = pd.DataFrame(data)

    return df


def supplierByFNumber_query(token,FNumber):
    '''
    通过供应商编号查询
    :param token: 中台token
    :param FNumber: 编号
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
    sql = f"""select * from {tablename} where CONVERT(date,{field},20) = '{FStartDate}'"""

    res = app3.select(sql)

    return res



def materialByDate_query(token,FDate):
    '''
    物料按日期查询
    :param token: 中台token
    :param FDate: 日期
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
    :param token: 中台token
    :param FDate: 日期
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
    :param token: 中台token
    :param FDate: 日期
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
    ERP系统物料按编号查询
    :param token: 中台token
    :param FNumber: 编号
    :param FName: 账套名
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="BD_MATERIAL")

        return res

    else:
        return ""


def customerErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    客户按编号查询
    :param token:中台token
    :param FNumber: 编号
    :param FName: 账套名
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="BD_Customer")

        return res

    else:

        return ""


def supplierErpDataByFNumber_query(token,FNumber,FName="赛普集团新账套"):
    '''
    ERP系统供应商按编号查询
    :param token: 中台token
    :param FNumber: 编号
    :param FName: 账套名
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app3 = RdClient(token=token)

    sql = f"select * from rds_key_values where FName='{FName}'"

    key = app3.select(sql)

    if key:

        option = {
            "acct_id": key[0]["acct_id"],
            "user_name": key[0]["user_name"],
            "app_id": key[0]["app_id"],
            "app_sec": key[0]["app_sec"],
            "server_url": key[0]["server_url"],
        }

        res = ERPData_query(api_sdk=api_sdk, option=option, FNumber=FNumber, Formid="BD_Supplier")

        return res
    else:
        return ""



def Status_upload(app3, tablename, field, FNumber):
    '''
    修改单据状态
    :param app3:数据库执行对象
    :param tablename: 表名
    :param field: 字段名
    :param FNumber:编号
    :return:
    '''

    sql = f"update a set a.{field}=0 from {tablename} a where a.{field}='{FNumber}'"

    app3.update(sql)

    return "单据状态修改已完成"


def dataStatus_update(app3,FTableName,field,FNumber):
    '''
    数据状态修改
    :param app3: 数据库执行对象
    :param FTableName: 表名
    :param field: 字段
    :param FNumber: 编号
    :return:
    '''

    sql=f"""
    update a set a.FIsdo=0 from {FTableName} a where a.{field}='{FNumber}'
    """

    app3.update(sql)



def materialStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '''
    物料按货号修改状态
    :param token: 中台token
    :param FNumber: 编号
    :param FName: 账套名
    :return:
    '''

    app3 = RdClient(token=token)

    dataStatus_update(app3=app3, FTableName="RDS_ECS_ods_bd_MaterialDetail", field="FNumber", FNumber=FNumber)

    return True


def customerStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '''
    客户按编号修改状态
    :param token: 中台token
    :param FNumber: 编号
    :param FName: 账套名
    :return:
    '''

    app3 = RdClient(token=token)

    dataStatus_update(app3=app3, FTableName="RDS_ECS_ods_BD_CUSTOMER", field="FNumber", FNumber=FNumber)

    return True


def supplierStatus_upload(token,FNumber,FName="赛普集团新账套"):
    '''
    供应商按编号修改状态
    :param token: 中台token
    :param FNumber: 编号
    :param FName: 账套名
    :return:
    '''

    app3 = RdClient(token=token)

    dataStatus_update(app3=app3, FTableName="RDS_ECS_ods_bd_SupplierDetail", field="FNumber", FNumber=FNumber)

    return True


def materialLog_query(token,FNumber):
    '''
    物料日志查询
    :param token:中台token
    :param FNumber:编号
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='ECS物料'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df


def customerLog_query(token,FNumber):
    '''
    客户日志查询
    :param token:中台token
    :param FNumber: 编号
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='ECS客户'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df

def supplierLog_query(token,FNumber):
    '''
    供应商日志查询
    :param token:中台token
    :param FNumber: 编号
    :return:
    '''
    app3 = RdClient(token=token)

    sql=f"""select * from RDS_ECS_Log where FNUMBER='{FNumber}' and FProgramName='ECS供应商'"""

    res=app3.select(sql)

    df=pd.DataFrame(res)

    return df
