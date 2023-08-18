from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk
import pandas as pd
import re
import requests
import hashlib
import time
import json

# sql插入语句
from .threebasicdata import *
# from pyecsbill.threebasicdata import NOAccount


def insertData(app2, sql):
    '''
    数据插入到对应的数据表中
    :param app2: 执行sql语句对象
    :param sql:  sql语句
    :return:
    '''
    app2.insert(sql)


# sql查询语句
def getData(app2, sql):
    '''
    从数据表中获得数据
    :param app2: 执行sql语句对象
    :param sql: sql语句
    :return: 返回查询到的数据
    '''
    result = app2.select(sql)

    if result:

        return result

    else:
        return []


# 获取最大FInterId
def getFinterId(app2, tableName):
    '''
    在两张表中找到最后一列数据的索引值
    :param app2: sql语句执行对象
    :param tableName: 要查询数据对应的表名表名
    :return:
    '''
    sql = f"select isnull(max(FInterId),0) as FMaxId from {tableName}"
    res = app2.select(sql)
    return res[0]['FMaxId']


# 插入错误日志
def insert_log(app2, res, FNumber, cp='赛普'):
    FMessages = []
    for errors in res['Result']['ResponseStatus']['Errors']:
        FMessages.append(errors['Message'])
    FMessages = ','.join(FMessages)
    FOccurrenceTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    sql = f"""insert into RDS_ECS_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName) values('ECS客户','{FNumber}','{FMessages}','{FOccurrenceTime}','{cp}')"""
    app2.insert(sql)


def insert_log2(app2, res, FNumber, cp='赛普'):

    sql = f"""insert into RDS_ECS_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName) values('ECS客户','{FNumber}','{res}',getdate(),'{cp}')"""
    app2.insert(sql)


# 修改Fisdo状态
def changeStatus(app2, status, tableName, param, param2):
    '''
    改变数据状态
    :param app2: sql语句执行对象
    :param status: 状态
    :param tableName: 表名
    :param param: 条件名
    :param param2: 条件
    :return:
    '''
    sql = f"update a set a.Fisdo={status} from {tableName} a where {param}='{param2}'"

    app2.update(sql)


# 通过传入的参数获得相应的编码
def getcode(app2, param, tableName, param1, param2):
    '''
    通过传入的参数获得相应的编码
    :param app2 执行sql语句对象:
    :param param 需要查询的字段:
    :param tableName 表名:
    :param param1 查询条件字段:
    :param param2 条件:
    :return 将查询的到的结果返回:
    '''

    if param2 == "销售四部":
        param2 = "国际销售部"

    if param2 == '0.13':
        param2 = '13%增值税'

    sql = f"select {param} from {tableName} where {param1}='{param2}'"
    res = app2.select(sql)
    return res[0][f'{param}'] if res != [] else ''


# 查看数据是否已存在ERP
def checkExist(app2, FName, FORGNUMBER):
    '''
    查看数据是否已存在
    :param app2:
    :param Fnumber:
    :return:
    '''

    sql = f"select distinct FNUMBER from rds_vw_customer where FNAME='{FName}' and FORGNUMBER='{FORGNUMBER}'"
    # sql = f"select distinct FNUMBER from  rds_vw_customer where FNAME='{FName}'"

    res = app2.select(sql)

    return res


# 查询结算方式编码
def get_settlement(app2, fname):
    sql = f"""select FNUMBER from rds_vw_settlement where FNAME='{fname}'"""
    res = app2.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        # return 'JSFS04_SYS'
        return ""


# 查看收款条件
def get_collection(app2, fname):
    sql = f"""select FNUMBER from rds_vw_collection where FNAME='{fname}'"""
    res = app2.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        return ""


# 查看结算币别
def get_FCurrencyNo(app2, fname):
    sql = f"""select fnumber from rds_vw_currency where fname = '{fname}'"""
    res = app2.select(sql)
    if res:
        return res[0]['fnumber']
    else:
        # return 'PRE001'
        return ''


# 查询国家编码
def get_FCOUNTRY(app2, FCOUNTRY):
    sql = f"""select FNUMBER from rds_vw_auxiliary where FNAME = '国家' and FDATAVALUE = '{FCOUNTRY}'"""
    res = app2.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        # return 'PRE001'
        return ''


# 通过编码找到对应的内码
def getCodeByView(forbid, number, api_sdk):
    '''
    通过编码找到对应的内码
    :param forbid: 表单ID
    :param number: 编码
    :param api_sdk: 接口执行对象
    :return:
    '''

    data = {
        "CreateOrgId": 0,
        "Number": number,
        "Id": "",
        "IsSortBySeq": "false"
    }
    # 将结果转换成json类型
    rs = json.loads(api_sdk.View(forbid, data))
    res = rs['Result']['Result']['Id']

    return res


# 获取客户编码
def queryDocuments(app2, number, name):
    sql = f"""
        select a.FNUMBER,a.FCUSTID,a.FMASTERID,a.FUSEORGID,a.FCREATEORGID,b.FNAME from T_BD_Customer
        a inner join takewiki_t_organization b
        on a.FUSEORGID = b.FORGID
        where a.FNUMBER = '{number}' and a.FUSEORGID = '{name}'
        """
    res = app2.select(sql)

    if res != []:

        return res[0]['FCUSTID']

    else:

        return "0"


# 获取分配组织id
def getOrganizationCode(app2, FUseOrg):
    '''
    获取分配组织id
    :param FUseOrg:
    :return:
    '''
    if FUseOrg == "赛普总部":
        FUseOrg = "苏州赛普生物科技股份有限公司"
    elif FUseOrg == "南通分厂":
        FUseOrg = "赛普生物科技（南通）有限公司"

    sql = f"select FORGID from rds_vw_organizations where FNAME like '%{FUseOrg}%'"

    oResult = app2.select(sql)

    return oResult[0]['FORGID']


# 获取分配组织id
def getOrganizationFNumber(app2, FUseOrg):
    '''
    获取分配组织id
    :param FUseOrg:
    :return:
    '''
    if FUseOrg == "赛普总部":
        FUseOrg = "苏州赛普生物科技有限公司"
    elif FUseOrg == "南通分厂":
        FUseOrg = "赛普生物科技（南通）有限公司"

    sql = f"select FORGID,FNUMBER  from rds_vw_organizations where FNAME like '%{FUseOrg}%'"

    res = app2.select(sql)

    if res == []:
        return ""
    else:
        return res[0]


# ECS的token加密
def encryption(pageNum, pageSize, queryList, tableName):
    '''
    ECS的token加密
    :param pageNum:
    :param pageSize:
    :param queryList:
    :param tableName:
    :return:
    '''
    m = hashlib.md5()
    token = f'accessId=skyx@prod&accessKey=skyx@0512@1024@prod&pageNum={pageNum}&pageSize={pageSize}&queryList={queryList}&tableName={tableName}'
    # token = f'accessId=skyx&accessKey=skyx@0512@1024&pageNum={pageNum}&pageSize={pageSize}&queryList={queryList}&tableName={tableName}'
    m.update(token.encode())
    md5 = m.hexdigest()
    return md5


# 通过名称请求ECS数据
def ECS_post_info(url, pageNum, pageSize, qw, tableName, updateTime, key):

    '''
    生科云选API接口
    :param url: 地址
    :param pageNum: 页码
    :param pageSize: 页面大小
    :param qw: 查询条件
    :param tableName: 表名
    :param updateTime: 时间戳
    :return: dataframe
    '''
    try:

        queryList = '[{"qw":' + f'"{qw}"' + ',"value":' + f'"{updateTime}"' + ',"key":' + f'"{key}"' + '}]'
        # 查询条件
        queryList1 = [{"qw": qw, "value": updateTime, "key": key}]
        # 查询的表名
        tableName = tableName
        data = {
            "tableName": tableName,
            "pageNum": pageNum,
            "pageSize": pageSize,
            "token": encryption(pageNum, pageSize, queryList, tableName),
            "queryList": queryList1
        }
        data = json.dumps(data)
        headers = {
            'Content-Type': 'application/json',
        }
        response = requests.post(url, headers=headers, data=data)
        info = response.json()
        df = pd.DataFrame(info['data']['list'])

        return df

    except Exception as e:

        return pd.DataFrame()



# 通过开始时间，结束时间请求数据
def ECS_post_info2(url, pageNum, pageSize, qw, qw2, tableName, updateTime, updateTime2, key):
    '''
    生科云选API接口
    :param url: 地址
    :param pageNum: 页码
    :param pageSize: 页面大小
    :param qw: 查询条件
    :param tableName: 表名
    :param updateTime: 时间戳
    :return: dataframe
    '''

    try:

        queryList = '[{"qw":' + f'"{qw}"' + ',"value":' + f'"{updateTime}"' + ',"key":' + f'"{key}"' + '},{"qw":' + f'"{qw2}"' + ',"value":' + f'"{updateTime2}"' + ',"key":' + f'"{key}"' + '}]'

        # 查询条件
        queryList1 = [{"qw": qw, "value": updateTime, "key": key}, {"qw": qw2, "value": updateTime2, "key": key}]

        # 查询的表名
        tableName = tableName

        data = {
            "tableName": tableName,
            "pageNum": pageNum,
            "pageSize": pageSize,
            "token": encryption(pageNum, pageSize, queryList, tableName),
            "queryList": queryList1
        }
        data = json.dumps(data)

        # url = f"http://10.3.1.99:8107/customer/getCustomerList?startDate={startDate}&endDate={endDate}&token={md5}"

        # url = "https://test-kingdee-api.bioyx.cn/dynamic/query"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(url, headers=headers, data=data)

        info = response.json()
        #
        # print(info)
        df = pd.DataFrame(info['data']['list'])
        # df = pd.DataFrame(info['data']['list'])
        return df

    except Exception as e:

        return pd.DataFrame()


def ECS_post_info3(url, pageNum, pageSize, qw, tableName, updateTime, key):
    '''
    生科云选API接口
    :param url: 地址
    :param pageNum: 页码
    :param pageSize: 页面大小
    :param qw: 查询条件
    :param tableName: 表名
    :param updateTime: 时间戳
    :return: dataframe
    '''
    queryList = '[{"qw":' + f'"{qw}"' + ',"value":' + f'"{updateTime}"' + ',"key":' + f'"{key}"' + '}]'
    # 查询条件
    queryList1 = [{"qw": qw, "value": updateTime, "key": key}]
    # 查询的表名
    tableName = tableName
    data = {
        "tableName": tableName,
        "pageNum": pageNum,
        "pageSize": pageSize,
        "token": encryption(pageNum, pageSize, queryList, tableName),
        "queryList": queryList1
    }
    data = json.dumps(data)
    headers = {
        'Content-Type': 'application/json',
    }
    response = requests.post(url, headers=headers, data=data)
    info = response.json()

    info_data = info.get('data')
    if info_data:
        data_info_list = info_data.get('list')
        if info_data.get('list') == None:
            data_info_list = []
    else:
        data_info_list = []
    # print(data_info_list)
    return data_info_list

    # df = pd.DataFrame(info['data']['list'])
    # return df


# ECS分表数据整合成SRC.model
def combination(data_info, data_address, data_contact):
    '''
    组装数据
    :return:
    '''

    model = {
        # "FInterId":'',
        "FApplyOrgName": '苏州生科云选生物科技有限公司',  # 申请组织
        "FApplyDeptName": '',  # 申请部门
        "FApplierName": '',  # 申请人
        "FDate": data_info['FCREATEDATE'],  # 申请日期
        "FNumber": data_info['CUSTOMER_SEQ'],  # 客户编码
        "FName": data_info['CUSTOMERNAME'],  # 客户名称(中文简体)
        "FShortName": '',  # 简称(中文简体)
        "FCOUNTRY": data_info['FRegion'],  # 国家名称
        "FPROVINCIAL": data_info['FPROVINCIAL'],  # 地区
        "FTEL": data_address.get('FTEL'),  # 联系电话
        "FINVOICETITLE": '',  # 发票抬头
        "FTAXREGISTERCODE": data_info['FTAXREGISTERCODE'],  # 纳税登记号
        "FBankName": data_info['BANK_NAME'],  # 开户银行
        "FINVOICETEL": data_info['INVOICE_TEL'],  # 开票联系电话
        "FAccountNumber": data_info['BANK_ACCOUNT'],  # 银行账号
        "FINVOICEADDRESS": data_info['INVOICE_ADDRESS'],  # 开票通讯地址
        "FINVOICETYPE": data_info['FINVOICETYPE'],  # 发票类型
        "FTaxRate": data_address.get('FTAXRATE'),  # 默认税率
        "FCONTACT": data_contact.get('FCONTACT'),  # 联系人名称
        "FBizAddress": data_contact.get('FBIZADDRESS'),  # 详细地址
        "FMOBILE": data_contact.get('FMOBILEPHONE'),  # 移动电话
        "FSalesman": data_address.get('FSELLER'),  # 销售人员名称
        "FAalesDeptName": data_info['FSALDEPTID'],  # 销售人员部门
        "FCustTypeNo": data_info['FCUSTTYPEID'],  # 客户类别编码
        "FGroupNo": data_info['FPRIMARYGROUP'],  # 客户分组编码
        "F_SZSP_KHFLNo": data_info['F_SZSP_KHFL'],  # 客户分类编码
        "FSalesGroupNo": data_address.get('FSALGROUPID'),  # 销售组编码
        "FTRADINGCURRNO": data_info.get('FRECEIVECURRID'),  # 结算币别编码
        "FSETTLETYPENO": data_info['FSETTLETYPEID'],  # 结算方式编码
        "FRECCONDITIONNO": data_info['FRECCONDITIONID'],  # 收款条件编码
        "FPRICELISTNO": data_info['FPRICELISTID'],  # 价目表编码
        "FUploadDate": '',  # 上传时间
        # "FUploadDate": data_info['FAPPROVEDATE'],
        # "FIsdo":'',
        "F_SZSP_BLOCNAME": data_info['F_SZSP_BLOCNAME'],  # 客户集团名称
        "F_SZSP_KHZYJBNo": data_info['F_SZSP_KHZYJB'],  # 客户重要级别属性
        "F_SZSP_KHGHSXNo": data_info['F_SZSP_KHGHSX'],  # 客户公海属性
        "F_SZSP_XSMSNo": data_info['F_SZSP_XSMS'],  # 销售模式
        "F_SZSP_XSMSSXNo": data_info['F_SZSP_XSMSSX'],  # 销售模式属性
        "F_SZSP_Text": ''  # ECS客户编码
    }

    for key in model:
        if model.get(key) == None:
            model[key] = ''
    model['FName'] = re.sub("'", '', model['FName'])
    return model


# ECS数据插入SRC
def insert_data(app2, data):
    '''
    数据库写入语句
    :param app2:
    :param data:
    :return:
    '''

    sql = f"""insert into RDS_ECS_SRC_BD_CUSTOMER(FInterId,FApplyOrgName,FApplyDeptName,FApplierName,FDate,FNumber,FName,
    FShortName,FCOUNTRY,FPROVINCIAL,FTEL,FINVOICETITLE,FTAXREGISTERCODE,FBankName,FINVOICETEL,FAccountNumber,
    FINVOICEADDRESS,FINVOICETYPE,FTaxRate,FCONTACT,FBizAddress,FMOBILE,FSalesman,FAalesDeptName,FCustTypeNo,FGroupNo,
    F_SZSP_KHFLNo,FSalesGroupNo,FTRADINGCURRNO,FSETTLETYPENO,FRECCONDITIONNO,FPRICELISTNO,FUploadDate,FIsdo,
    F_SZSP_BLOCNAME,F_SZSP_KHZYJBNo,F_SZSP_KHGHSXNo,F_SZSP_XSMSNo,F_SZSP_XSMSSXNo,F_SZSP_Text
    ) values({getFinterId(app2, 'RDS_ECS_SRC_BD_CUSTOMER') + 1},'苏州生科云选生物科技有限公司','{data.get('FApplyDeptName', '')}', 
    '{data.get('FApplierName', '')}', '{data.get('FDate', '')}','{data.get('FNumber', '')}', '{str(data.get('FName', '')).replace("'","''").strip()}',
    '{data.get('FShortName', '')}', '{data.get('FCOUNTRY', '')}', '{data.get('FPROVINCIAL', '')}', '{data.get('FTEL', '')}',
    '{data.get('FINVOICETITLE', '')}', '{data.get('FTAXREGISTERCODE', '')}', '{data.get('FBankName', '')}',
    '{data.get('FINVOICETEL', '')}', '{data.get('FAccountNumber', '')}', '{data.get('FINVOICEADDRESS', '')}',
    '{data.get('FINVOICETYPE', '')}', '{data.get('FTaxRate', '')}', '{data.get('FCONTACT', '')}','{data.get('FBizAddress', '')}',
    '{data.get('FMOBILE', '')}', '{data.get('FSalesman', '')}', '{data.get('FAalesDeptName', '')}', '{data.get('FCustTypeNo', '')}',
    '{data.get('FGroupNo', '')}', '{data.get('F_SZSP_KHFLNo', '')}', '{data.get('FSalesGroupNo', '')}', 
    '{data.get('FTRADINGCURRNO', '')}', '{data.get('FSETTLETYPENO', '')}', '{data.get('FRECCONDITIONNO', '')}', 
    '{data.get('FPRICELISTNO', '')}', getdate(), 0,'{data.get('F_SZSP_BLOCNAME', '')}',
    '{data.get('F_SZSP_KHZYJBNo', '')}','{data.get('F_SZSP_KHGHSXNo', '')}','{data.get('F_SZSP_XSMSNo', '')}',
    '{data.get('F_SZSP_XSMSSXNo', '')}','{data.get('F_SZSP_Text', '')}'
    )"""
    insertData(app2, sql)


# ODS数据插入ERP
def ecs_ods_erp(app2, app3, option1):
    '''
    :param app2: token_china
    :param app3: token_erp
    :param option1: 金蝶用户信息
    :return: 写入金蝶
    '''
    sql4 = "select FInterId, FApplyOrgName, FApplyDeptName, FApplierName, FDate, FNumber, FName,FShortName, FCOUNTRY, FPROVINCIAL, FTEL, FINVOICETITLE, FTAXREGISTERCODE, FBankName, FINVOICETEL, FAccountNumber,FINVOICEADDRESS, FINVOICETYPE, FTaxRate, FCONTACT, FBizAddress, FMOBILE, FSalesman, FAalesDeptName, FCustTypeNo, FGroupNo,F_SZSP_KHFLNo, FSalesGroupNo, FTRADINGCURRNO, FSETTLETYPENO, FRECCONDITIONNO, FPRICELISTNO, FUploadDate, FIsdo,F_SZSP_BLOCNAME, F_SZSP_KHZYJBNo, F_SZSP_KHGHSXNo, F_SZSP_XSMSNo, F_SZSP_XSMSSXNo, F_SZSP_Text from RDS_ECS_ODS_BD_CUSTOMER where FIsdo = 0"
    result = getData(app3, sql4)
    # print(result)

    api_sdk = K3CloudApiSdk()

    # print("开始保存数据")
    res=ERP_customersave(api_sdk, option1, result, app2, app3, 'RDS_ECS_SRC_BD_CUSTOMER', 'RDS_ECS_ODS_BD_CUSTOMER')

    return res


#  提交
def ERP_customersubmit(fNumber, api_sdk):
    '''
    提交
    :param fNumber:
    :param api_sdk:
    :return:
    '''
    model = {
        "CreateOrgId": 0,
        "Numbers": [fNumber],
        "Ids": "",
        "SelectedPostId": 0,
        "NetworkCtrl": "",
        "IgnoreInterationFlag": ""
    }
    res = api_sdk.Submit("BD_Customer", model)

    return res


# 审核
def ERP_audit(forbid, number, api_sdk):
    '''
    将状态为审核中的数据审核
    :param forbid: 表单ID
    :param number: 编码
    :param api_sdk: 接口执行对象
    :return:
    '''

    data = {
        "CreateOrgId": 0,
        "Numbers": [number],
        "Ids": "",
        "InterationFlags": "",
        "NetworkCtrl": "",
        "IsVerifyProcInst": "",
        "IgnoreInterationFlag": ""
    }

    res = api_sdk.Audit(forbid, data)

    return res


# 分配
def ERP_allocate(forbid, PkIds, TOrgIds, api_sdk):
    '''
    分配
    :param forbid: 表单
    :param PkIds: 被分配的基础资料内码集合
    :param TOrgIds: 目标组织内码集合
    :param api_sdk: 接口执行对象
    :return:
    '''

    data = {
        "PkIds": int(PkIds),
        "TOrgIds": TOrgIds
    }

    res = api_sdk.Allocate(forbid, data)

    return res


# ERP的保存，提交，审核，分配，保存，提交，审核流程
def ERP_customersave(api_sdk, option, dData, app2, app3, src_table_name, ods_table_name):
    '''
    将数据进行保存
    :param option:
    :param dData:
    :return:
    '''

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    FINVOICETYPE_dict = {"增值税专用发票": "1", "普通发票": "2"}

    for i in dData:

        i['FTAXREGISTERCODE'] = re.sub('[" "\?]', '', i['FTAXREGISTERCODE'])
        i['FAccountNumber'] = re.sub('[" "\?]', '', i['FAccountNumber'])
        i['FName'] = i['FName'].strip()

        res_100 = checkExist(app2, i['FName'], 100)
        if res_100 == []:
            model = {
                "Model": {
                    "FCUSTID": 0,
                    "FCreateOrgId": {
                        "FNumber": "100"
                    },
                    "FUseOrgId": {
                        "FNumber": "100"
                    },
                    "FName": i['FName'],
                    # "FNumber": i['FNumber'],
                    "FShortName": i['FShortName'],
                    "FCOUNTRY": {
                        "FNumber": get_FCOUNTRY(app2, i['FCOUNTRY'])
                    },
                    "FTEL": i['FTEL'],
                    "FINVOICETITLE": i['FName'],
                    "FTAXREGISTERCODE": i['FTAXREGISTERCODE'],
                    "FINVOICEBANKNAME": i['FBankName'],
                    "FINVOICETEL": i['FINVOICETEL'],
                    "FINVOICEBANKACCOUNT": i['FAccountNumber'],
                    "FINVOICEADDRESS": i['FINVOICEADDRESS'],
                    "FSOCIALCRECODE": i['FTAXREGISTERCODE'],
                    "FIsGroup": False,
                    "FIsDefPayer": False,
                    "FCustTypeId": {
                        "FNumber": i['FCustTypeNo']
                    },
                    "FGroup": {
                        "FNumber": i['FGroupNo']
                    },
                    "FTRADINGCURRID": {
                        "FNumber":
                        # "PRE001" if i['FTRADINGCURRNO'] == '' else
                            get_FCurrencyNo(app2, i['FTRADINGCURRNO'])
                    },
                    "FInvoiceType": FINVOICETYPE_dict.get(i['FINVOICETYPE'], ""),
                    # "1" if i['FINVOICETYPE'] == "" or i['FINVOICETYPE'] == "增值税专用发票" else "2",
                    "FTaxType": {
                        "FNumber": "SFL02_SYS"
                    },
                    "FTaxRate": {
                        "FNumber":
                        # "SL02_SYS" if i['FTaxRate'] == "" else
                            getcode(app2, "FNUMBER", "rds_vw_taxRate", "FNAME", i['FTaxRate'])
                    },
                    "FISCREDITCHECK": True,
                    "FIsTrade": True,
                    "FUncheckExpectQty": False,
                    "F_SZSP_KHFL": {
                        "FNumber": i['F_SZSP_KHFLNo']
                    },
                    "F_SZSP_Text": i['FNumber'],
                    "FT_BD_CUSTOMEREXT": {
                        "FEnableSL": False,
                        "FALLOWJOINZHJ": False
                    },

                    "FT_BD_CUSTBANK": [
                        {
                            "FENTRYID": 0,
                            "FCOUNTRY1": {
                                "FNumber": get_FCOUNTRY(app2, i['FCOUNTRY'])},
                            "FBANKCODE": i['FAccountNumber'],
                            "FACCOUNTNAME": i['FName'],
                            "FBankTypeRec": {
                                "FNUMBER": ""
                            },
                            "FTextBankDetail": "",
                            "FBankDetail": {
                                "FNUMBER": ""
                            },
                            "FOpenAddressRec": "",
                            "FOPENBANKNAME": i['FBankName'],
                            "FCNAPS": "",
                            "FCURRENCYID": {
                                "FNumber": ""
                            },
                            "FISDEFAULT1": "false"
                        }
                    ],
                    # "FT_BD_CUSTLOCATION": [
                    #     {
                    #         "FContactId": {
                    #             "FNUMBER": ""
                    #         },
                    #         # "FCONTACT": {
                    #         #     "FNAME": i["FCONTACT"]
                    #         # },
                    #         "FIsDefaultConsigneeCT": "false",
                    #         "FIsCopy": "false"
                    #     }
                    # ]
                }
            }

            savedResultInformation = api_sdk.Save("BD_Customer", model)
            print(f"编码为{i['FNumber']}：{savedResultInformation}保存成功")
            sri = json.loads(savedResultInformation)

            if sri['Result']['ResponseStatus']['IsSuccess']:

                submittedResultInformation = ERP_customersubmit(
                    sri['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'], api_sdk)
                print(f"编码为{i['FNumber']}：{submittedResultInformation}数据提交成功")

                subri = json.loads(submittedResultInformation)

                if subri['Result']['ResponseStatus']['IsSuccess']:

                    auditResultInformation = ERP_audit('BD_Customer',
                                                       subri['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'],
                                                       api_sdk)

                    auditres = json.loads(auditResultInformation)

                    if auditres['Result']['ResponseStatus']['IsSuccess']:

                        result = ERP_allocate('BD_Customer', getCodeByView('BD_Customer',
                                                                           auditres['Result']['ResponseStatus'][
                                                                               'SuccessEntitys'][0]['Number'], api_sdk),
                                              getOrganizationCode(app2, i['FApplyOrgName']), api_sdk)

                        AlloctOperation(
                            auditres['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'], api_sdk, i,
                            app2, app3)

                        changeStatus(app3, "1", src_table_name, "FNumber", i['FNumber'])
                        changeStatus(app3, "1", ods_table_name, "FNumber", i['FNumber'])

                        insert_log2(app3, "数据同步成功", i['FNumber'])

                    else:
                        insert_log(app3, auditres, i['FNumber'])
                        changeStatus(app3, "2", src_table_name, "FNumber", i['FNumber'])
                        changeStatus(app3, "2", ods_table_name, "FNumber", i['FNumber'])
                        print(auditres)
                else:
                    insert_log(app3, subri, i['FNumber'])
                    changeStatus(app3, "2", src_table_name, "FNumber", i['FNumber'])
                    changeStatus(app3, "2", ods_table_name, "FNumber", i['FNumber'])
                    print(subri)
            else:
                insert_log(app3, sri, i['FNumber'])
                changeStatus(app3, "2", src_table_name, "FNumber", i['FNumber'])
                changeStatus(app3, "2", ods_table_name, "FNumber", i['FNumber'])
                print(sri)
        else:
            res_104 = checkExist(app2, i['FName'], 104)
            if res_104 == []:
                fnumber_100 = res_100[0]['FNUMBER']
                result = ERP_allocate('BD_Customer', getCodeByView('BD_Customer',
                                                                   fnumber_100, api_sdk),
                                      getOrganizationCode(app2, i['FApplyOrgName']), api_sdk)

                AlloctOperation(fnumber_100, api_sdk, i, app2, app3)

                changeStatus(app3, "1", src_table_name, "FNumber", i['FNumber'])
                changeStatus(app3, "1", ods_table_name, "FNumber", i['FNumber'])

                print(result)
            else:
                print("该编码{}数据已存在于金蝶".format(i['FNumber']))
                changeStatus(app3, "1", src_table_name, "FNumber", i['FNumber'])
                changeStatus(app3, "1", ods_table_name, "FNumber", i['FNumber'])
        # break

    return "程序运行完成"


# ERP分配后的，保存，提交，审核流程
def AlloctOperation(number, api_sdk, i, app2, app3):
    '''
    数据分配后进行提交审核
    :param forbid:
    :param number:
    :param api_sdk:
    :return:
    '''

    SaveAfterAllocation(api_sdk, i, app2, number, app3)


# ERP分配后的，保存，提交，审核流程
def SaveAfterAllocation(api_sdk, i, app2, FNumber104, app3):
    FOrgNumber = getOrganizationFNumber(app2, i['FApplyOrgName'])
    if FOrgNumber:
        model = {
            "Model": {
                "FCUSTID": queryDocuments(app2, FNumber104, FOrgNumber['FORGID']),
                "FCreateOrgId": {
                    "FNumber": "100"
                },
                "FUseOrgId": {
                    "FNumber": str(FOrgNumber['FNUMBER'])
                },
                "FName": str(i['FName']),
                "FNUMBER": FNumber104,
                "FCOUNTRY": {
                    "FNumber": get_FCOUNTRY(app2, i['FCOUNTRY'])
                },
                "FTRADINGCURRID": {
                    "FNumber":
                    # "PRE001" if i['FTRADINGCURRNO'] == '' else
                        get_FCurrencyNo(app2, i['FTRADINGCURRNO'])
                },
                "FSALDEPTID": {
                    "FNumber": getcode(app2, "FNUMBER", "rds_vw_department", "FNAME", i['FAalesDeptName'])
                },
                "FSALGROUPID": {
                    "FNumber": "SKYX01"
                },
                "FSELLER": {
                    "FNumber": getcode(app2, "FNUMBER", "rds_vw_salesman", "FNAME", i['FSalesman'])
                },
                "FSETTLETYPEID": {
                    "FNumber": get_settlement(app2, i['FSETTLETYPENO'])
                },
                "FRECCONDITIONID": {
                    "FNumber": get_collection(app2, i['FRECCONDITIONNO'])
                },
            }
        }
        res = api_sdk.Save("BD_Customer", model)
        save_res = json.loads(res)
        if save_res['Result']['ResponseStatus']['IsSuccess']:
            submit_res = ERP_customersubmit(FNumber104, api_sdk)
            audit_res = ERP_audit("BD_Customer", FNumber104, api_sdk)
        else:
            insert_log(app3, save_res, FNumber104)

        print(f"修改编码为{FNumber104}的信息:" + res)


# 时间入口
def FCREATEDATE_get_ECS(app2,app3,option1,starttime):

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    res = ECS_post_info(url, 1, 1000, "like", "v_customer_info", starttime, "UPDATE_TIME")

    if not res.empty:
        for i in res.index:

            data_address = ECS_post_info(url, 1, 1000, "eq", "v_customer_address", res.loc[i]['CUSTOMER_SEQ'],
                                         "CUSTOMER_SEQ")
            data_contact = ECS_post_info(url, 1, 1000, "eq", "v_customer_contact", res.loc[i]['CUSTOMER_SEQ'],
                                         "CUSTOMER_SEQ")

            if data_address.empty:
                data_address = {}
            else:
                data_address = data_address.loc[0]

            if data_contact.empty:
                data_contact = {}
            else:
                data_contact = data_contact.loc[0]

            data_info = res.loc[i]

            data = combination(data_info, data_address, data_contact)

            if data['FCOUNTRY'] == '中国' and data['FTAXREGISTERCODE'] == '':
                ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '纳税登记号为空'}]}}}
                insert_log(app3, ero, data['FNumber'])
                print(f"{data['FName']}纳税登记号为空")
                continue
            if data['FTRADINGCURRNO'] == '':
                ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '结算币别为空'}]}}}
                insert_log(app3, ero, data['FNumber'])
                print(f"{data['FName']}结算币别为空")
                continue

            sql = f"""select FName from RDS_ECS_SRC_BD_CUSTOMER"""
            fdata = app3.select(sql)
            fnames = []
            for name_date in fdata:
                fnames.append(name_date['FName'])

            if data['FName'] not in fnames:

                insert_data(app3, data)
                NOAccount(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3")

                print(f"{data['FNumber']}插入成功")
            else:
                print(f"{data['FNumber']}已存在数据库")
    else:
        print(f"请求数据为空")

    # 写入金蝶
    ecs_ods_erp(app2, app3, option1)

    return True


# 名称入口
def CUSTOMERNumber_get_ECS(app2,app3,option1,FNumber):
    # 新账套

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    data_info = ECS_post_info(url, 1, 1000, "eq", "v_customer_info", FNumber, "CUSTOMER_SEQ")

    if not data_info.empty:
        data_info = data_info.loc[0]

        data_address = ECS_post_info(url, 1, 1000, "eq", "v_customer_address", data_info['CUSTOMER_SEQ'],
                                     "CUSTOMER_SEQ")

        data_contact = ECS_post_info(url, 1, 1000, "eq", "v_customer_contact", data_info['CUSTOMER_SEQ'],
                                     "CUSTOMER_SEQ")

        if data_address.empty:
            data_address = {}
        else:
            data_address = data_address.loc[0]

        if data_contact.empty:
            data_contact = {}
        else:
            data_contact = data_contact.loc[0]

        data = combination(data_info, data_address, data_contact)

        # 判断纳税登记号是否为空
        if data['FCOUNTRY'] == '中国' and data['FTAXREGISTERCODE'] == '':
            ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '纳税登记号为空'}]}}}
            insert_log(app3, ero, data['FNumber'])
            print(f"{data['FName']}纳税登记号为空")

        if data['FTRADINGCURRNO'] == '':
            ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '结算币别为空'}]}}}
            insert_log(app3, ero, data['FNumber'])
            print(f"{data['FName']}结算币别为空")


        # 查重
        sql = f"""select FName from RDS_ECS_SRC_BD_CUSTOMER"""
        fdata = app3.select(sql)
        fnames = []
        for name_date in fdata:
            fnames.append(name_date['FName'])

        # 插入
        if data['FName'] not in fnames:
            insert_data(app3, data)
            NOAccount(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3")
            # acc = NOAccount(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3")
            # acc.update_RDS_ECS_ODS_BD_CUSTOMER()
            print(f"{data['FNumber']}插入成功")
        else:

            print(f"{data['FNumber']}已存在数据库")
            insert_log2(app3, f"{data['FNumber']}已存在数据库", data['FNumber'])
    else:
        print(f"{FNumber}未请求到数据")

        insert_log2(app3, f"{FNumber}未请求到数据", FNumber)
    # 写入金蝶
    ecs_ods_erp(app2, app3, option1)

    return True


