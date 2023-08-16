import re
from k3cloud_webapi_sdk.main import K3CloudApiSdk
from pyrda.dbms.rds import RdClient
import requests
import hashlib
import time
import json
from .threebasicdata import *
# from mdlCpEcsMdpy.threebasicdata import NOAccount


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


# 名称请求ECS
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


# 开始时间，结束时间请求数据
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
    # df = pd.DataFrame(info['data']['list'])
    df = info['data']['list']
    return df


# ECS数据整合成SRC.model
def combination(data_info, data_bank, data_base, data_contact, data_business):
    '''
    组装数据
    :return:
    '''

    model = {
        # FInterId
        "FApplyOrgName": '苏州生科云选生物科技有限公司',  # 申请组织
        "FApplyDeptName": '',  # 申请部门
        "FApplierName": '',  # 申请人
        "FDate": data_info['FCREATEDATE'],  # 申请日期
        "FNumber": data_info['FNUMBER'],  # 供应商编码
        "FName": data_info['FNAME'],  # 供应商名称
        "FShortName": data_info['FSHORTNAME'],  # 简称
        "FCountry": data_bank['FCOUNTRY'],  # 国家
        "FProvince": data_base['FPROVINCIAL'],  # 地区
        "FZipCode": data_base['FZIP'],  # 邮政
        "FUniversalCode": data_base['FREGISTERCODE'],  # 统一社会信用代码
        "FRegisterAddress": data_base['FREGISTERADDRESS'],  # 注册地址
        "FMngrDeptName": data_base['FDEPTID'],  # 负责部门
        "FMngrMan": data_base['FSTAFFID'],  # 负责人
        "FSullierType": data_base['FSUPPLIERCLASSIFY'],  # 供应类别
        "FInvoiceType": data_business['FINVOICETYPE'],  # 发票类型
        "FTaxRate": data_business['FTAXRATEID'],  # 税率
        "FAccountNumber": data_bank['FBANKCODE'],  # 银行账号
        "FAccountName": data_bank['FOPENBANKNAME'],  # 账户名称
        "FBankTransferCode": '',  # 联行号
        "FBankName": data_bank['FNAME'],  # 开户银行
        "FBankAddr": '',  # 开户行地址
        "FContact": data_contact['FCONTACT'],  # 联系人
        "FMobile": data_contact['FMOBILE'],  # 手机
        "FEMail": data_contact['FEMAIL'],  # 电子邮箱
        "FSupplierCategoryNo": data_base['FSUPPLIERGROUPING'],  # 供应商分类编码（分组）
        "FSupplierGradeNo": data_base['FSUPPLIERCLASSIFICATION'],  # 供应商等级编码
        "FPriceListNo": '',  # 默认价目表编码
        "FSettleCurrencyNo": data_business['FSETTLEMENTCURRENCY'],  # 结算币别编码
        "FSettlementMethodNo": data_business["FSETTLETYPEID"],  # 结算方式编码
        "FPaymentConditionNo": data_business["FRECCONDITIONID"],  # 付款条件
        "FCurrencyNo": data_bank['FCURRENCYID'],  # 币别编码
        # "FUploadDate": data_info['FMODIFYDATE'],# 修改时间
        "FUploadDate": "",  # 上传时间
        # "FUploadDate": data_info['FAUDITDATE'],
        # Fisdo
        "FPurchaserGroupId": ''  # 采购组

    }
    for key in model:
        if model.get(key) == None:
            model[key] = ''
    return model


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


# sql插入语句
def insertData(app2, sql):
    '''
    将从OA接口里面获得的数据插入到对应的数据表中
    :param app2: 执行sql语句对象
    :param sql:  sql语句
    :return:
    '''
    app2.insert(sql)


# ECS插入SRC
def insert_data(app2, data):
    '''
    数据库写入语句
    :param app2:
    :param data:
    :return:
    '''

    # table_name = 'RDS_ECS_SRC_bd_MaterialDetail'
    # print(data)

    sql = f"""insert into RDS_ECS_SRC_bd_SupplierDetail(FInterId,FApplyOrgName,FApplyDeptName,FApplierName,FDate,FNumber,
    FName,FShortName,FCountry,FProvince,FZipCode,FUniversalCode,FRegisterAddress,FMngrDeptName,FMngrMan,FSullierType,
    FInvoiceType,FTaxRate,FAccountNumber,FAccountName,FBankTransferCode,FBankName,FBankAddr,FContact,FMobile,FEMail,
    FSupplierCategoryNo,FSupplierGradeNo,FPriceListNo,FSettleCurrencyNo,FSettlementMethodNo,FPaymentConditionNo,
    FCurrencyNo,FUploadDate,Fisdo,FPurchaserGroupId) values({getFinterId(app2, 'RDS_ECS_SRC_bd_SupplierDetail') + 1},
    '{data.get('FApplyOrgName', '')}','{data.get('FApplyDeptName', '')}','{data.get('FApplierName', '')}', 
    '{data.get('FDate', '')}','{data.get('FNumber', '')}', '{data.get('FName', '').replace("'", "''").strip()}','{data.get('FShortName', '')}', 
    '{data.get('FCountry', '')}', '{data.get('FProvince', '')}', '{data.get('FZipCode', '')}','{data.get('FUniversalCode', '')}', 
    '{data.get('FRegisterAddress', '')}', '{data.get('FMngrDeptName', '')}','{data.get('FMngrMan', '')}', 
    '{data.get('FSullierType', '')}', '{data.get('FInvoiceType', '')}','{data.get('FTaxRate', '')}', 
    '{data.get('FAccountNumber', '')}', '{data.get('FAccountName', '')}','{data.get('FBankTransferCode', '')}',
    '{data.get('FBankName', '')}', '{data.get('FBankAddr', '')}', '{data.get('FContact', '')}', '{data.get('FMobile', '')}',
    '{data.get('FEMail', '')}', '{data.get('FSupplierCategoryNo', '')}', '{data.get('FSupplierGradeNo', '')}', 
    '{data.get('FPriceListNo', '')}', '{data.get('FSettleCurrencyNo', '')}', '{data.get('FSettlementMethodNo', '')}', 
    '{data.get('FPaymentConditionNo', '')}','{data.get('FCurrencyNo', '')}',getdate(),0,
    '{data.get('FPurchaserGroupId', '')}')"""

    insertData(app2, sql)


# 查询ODS待处理数据
def judgeDetailData(option, app2, app3):
    '''
    判断RDS_ECS_ODS_bd_SupplierDetail表中是否有数据
    :param app3:
    :return:
    '''

    sql = "select FInterId ,FApplyOrgName,FApplyDeptName,FApplierName,FDate,FNumber,FName,FShortName,FCountry,FZipCode,FUniversalCode,FRegisterAddress,FMngrDeptName,FMngrMan,FSullierType,FInvoiceType,FTaxRate,FAccountNumber,FAccountName ,FBankTransferCode,FBankName,FBankAddr,FContact,FMobile,FEMail, FSupplierCategoryNo,FSupplierGradeNo ,FPriceListNo,FSettleCurrencyNo,FSettlementMethodNo,FPaymentConditionNo,FCurrencyNo,FUploadDate,Fisdo from RDS_ECS_ODS_bd_SupplierDetail where Fisdo=0"

    res = app3.select(sql)
    print(res)
    if res != []:

        insert_into_ERP(option=option, data=res, app2=app2, app3=app3)
    else:
        pass


# ODS插入ERP
def insert_into_ERP(option, data, app2, app3):
    '''
    将数据插入到ERP系统
    :param erp_token:
    :param data:
    :param app2:
    :param app3:
    :return:
    '''

    api_sdk = K3CloudApiSdk()
    print("开始保存数据")
    ERP_suppliersave(api_sdk, option, data, app2, app3)


# 插入错误日志
def insert_log(app2, res, FNumber, cp='赛普'):
    FMessages = []
    for errors in res['Result']['ResponseStatus']['Errors']:
        FMessages.append(errors['Message'])
    FMessages = ','.join(FMessages)
    FOccurrenceTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    sql = f"""insert into RDS_ECS_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName) 
    values('ECS供应商','{FNumber}','{FMessages}','{FOccurrenceTime}','{cp}')"""
    app2.insert(sql)


def insert_log2(app2, res, FNumber, cp='赛普'):
    sql = f"""insert into RDS_ECS_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName) 
    values('ECS供应商','{FNumber}','{res}',getdate(),'{cp}')"""
    app2.insert(sql)


# 转换税率编码
def getTaxRateCode(app2, param):
    '''
    转换税率编码
    :param app2: sql语句执行对象
    :param param: 条件
    :return:
    '''

    if param == "1":
        param = 13
    elif param == "0":
        param = "零"

    sql = f"select FNUMBER from rds_vw_taxrate where  FNAME like '{param}%'"
    res = app2.select(sql)

    if res == []:
        return ""
    else:
        return res[0]['FNUMBER']


# 查询币别编码
def get_FCurrencyNo(app2, fname):
    sql = f"""select fnumber from rds_vw_currency where fname = '{fname}'"""
    res = app2.select(sql)
    if res:
        return res[0]['fnumber']
    else:
        # return 'PRE001'
        return ''


# 查询供应商等级编码
def get_auxiliary(app2, fname):
    sql = f"""select FNUMBER from rds_vw_auxiliary where FNAME='供应商等级' and FDATAVALUE='{fname}'"""
    res = app2.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        return ''


# 查询结算方式编码
def get_settlement(app2, fname):
    sql = f"""select FNUMBER from rds_vw_settlement where FNAME='{fname}'"""
    res = app2.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        # return 'JSFS04_SYS'
        return ''


# 查看付款条件
def get_payment(app2, fname):
    sql = f"""select FNUMBER from rds_vw_payment where FNAME='{fname}'"""
    res = app2.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        # return 'JSFS04_SYS'
        return ''


# 查询供应商分组编码
def get_supplierGrouping(app2, fname):
    sql = f"""select FNUMBER from rds_vw_supplierGrouping where FNAME='{fname}'"""
    res = app2.select(sql)
    if res:
        return res[0]['FNUMBER']
    else:
        return ''


# 查询国家编码
def getCountryCode(app2, param):
    # sql = f"select FNUMBER from rds_vw_country where FNAME='{param}'"
    sql = f"select FNUMBER from rds_vw_auxiliary where FNAME='国家' and FDATAVALUE='{param}'"

    res = app2.select(sql)

    if res == []:
        return ""
    else:
        return res[0]['FNUMBER']


# 供应类别编码
def getSullierTypeCode(param):
    '''
    转换code码
    :param param: 条件
    :return:
    '''
    d = {"采购": "CG", "委外": "WW", "服务": "FW", "综合": "ZH"}

    res = d.get(param, '')

    return res


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


# 查询供应商编码
def queryDocuments(app2, api_sdk, number, forgid):
    sql = f"""
        select a.FNUMBER,a.FSUPPLIERID,a.FMASTERID,a.FUSEORGID,a.FCREATEORGID,b.FNAME from T_BD_SUPPLIER  
        a inner join takewiki_t_organization b
        on a.FUSEORGID = b.FORGID
        where a.FNUMBER = '{number}' and b.FORGID = '{forgid}'
        """
    res = app2.select(sql)

    if res != []:

        return res[0]['FSUPPLIERID']

    else:

        return "0"


# 编码值转换
def codeConversion(app2, FTableName, FName):
    '''
    编码值转换
    :param app2:
    :param FName:
    :return:
    '''

    sql = f"select FNUMBER from {FTableName} where FNAME='{FName}'"

    res = app2.select(sql)

    if res == []:

        return ""

    else:

        return res[0]['FNUMBER']


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

    res = app2.select(sql)

    if res == []:
        return ""
    else:
        return res[0]['FORGID']


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

    sql = f"select FNumber,FORGID from rds_vw_organizations where FNAME like '%{FUseOrg}%'"

    res = app2.select(sql)

    if res == []:
        return ""
    else:
        return res[0]


# 提交
def ERP_suppliersubmit(number, api_sdk):
    '''
    对创建的数据进行提交
    :param number 单据编号:
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

    res = api_sdk.Submit("BD_Supplier", data)

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


# 查看是否存在ERP
def checkExist(app2, FName, FORGNUMBER):
    '''
    查看数据是否已存在
    :param app2:
    :param Fnumber:
    :return:
    '''

    sql = f"select FNUMBER from rds_vw_supplier where FNAME='{FName}' and FORGNUMBER='{FORGNUMBER}'"
    res = app2.select(sql)

    return res


# 查看是否存在ERP
def supplierISExist(app2, FName, FOrgNumber):
    '''
    通过供应商的名字到系统查看供应商是否存在
    :param app2:
    :param FNumber:
    :param FOrgNumber:
    :return:
    '''

    sql = f"select * from rds_vw_supplier where FNAME='{FName}' and FORGNUMBER='{FOrgNumber}'"
    res = app2.select(sql)

    return res


# ERP保存，提交，审核，分配，保存，提交，审批流程
def ERP_suppliersave(api_sdk, option, dData, app2, app3):
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

        i['FUniversalCode'] = re.sub('[" "\?]', '', i['FUniversalCode'])

        Exist_100 = checkExist(app2, i['FName'], 100)

        if Exist_100 == []:
            model = {
                "Model": {
                    "FSupplierId": 0,
                    "FCreateOrgId": {
                        "FNumber": "100"
                    },
                    "FUseOrgId": {
                        "FNumber": "100"
                    },
                    "FGroup": {
                        "FNumber": get_supplierGrouping(app2, i['FSupplierCategoryNo'])
                    },
                    "FName": i['FName'],
                    # "FNumber": '',
                    "FShortName": i['FShortName'],
                    "F_SZSP_Text": i['FNumber'],
                    "FBaseInfo": {
                        "FCountry": {
                            "FNumber":
                            # "China" if i['FCountry'] == "" or i['FCountry'] == "中国" else
                                getCountryCode(app2, i['FCountry'])
                        },
                        "FSOCIALCRECODE": i['FUniversalCode'],
                        "FRegisterAddress": i['FRegisterAddress'],
                        "FZip": i['FZipCode'],
                        "FFoundDate": "",
                        "FRegisterCode": str(i['FUniversalCode']),
                        "FSupplyClassify":
                        # "CG" if i['FSullierType'] == "" else
                            getSullierTypeCode(i['FSullierType']),
                        "FSupplierGrade": {
                            "FNumber": get_auxiliary(app2, i['FSupplierGradeNo'])
                        }
                    },
                    "FBusinessInfo": {
                        "FSettleTypeId": {
                            "FNumber": get_settlement(app2, i['FSettlementMethodNo'])
                        },
                        "FPRICELISTID": {
                            "FNumber": i['FPriceListNo']
                        },
                        "FVmiBusiness": False,
                        "FEnableSL": False
                    },
                    "FFinanceInfo": {
                        "FPayCurrencyId": {
                            "FNumber":
                            # "PRE001" if i['FCurrencyNo'] == '' else
                                get_FCurrencyNo(app2, i['FSettleCurrencyNo'])
                        },
                        "FPayCondition": {
                            "FNumber": get_payment(app2, i['FPaymentConditionNo'])
                        },
                        "FTaxType": {
                            "FNumber": "SFL02_SYS"
                        },
                        "FTaxRegisterCode": str(i['FUniversalCode']),
                        "FInvoiceType": FINVOICETYPE_dict.get(i['FInvoiceType'], ""),
                        # "1" if (i['FInvoiceType'] == "" or i['FInvoiceType'] == "增值税专用发票") else "2",
                        "FTaxRateId": {
                            "FNUMBER":
                            # "SL02_SYS" if i['FTaxRate'] == "" else
                                getTaxRateCode(app2, i['FTaxRate'])
                        }
                    },
                    "FBankInfo": [
                        {
                            "FBankCountry": {
                                "FNumber":
                                # "China" if i['FCountry'] == "" or i['FCountry'] == "中国" else
                                    getCountryCode(app2, i['FCountry'])
                            },
                            "FBankCode": i['FAccountNumber'],
                            "FBankHolder": i['FAccountName'],
                            "FOpenBankName": i['FBankName'],
                            "FCNAPS": i['FBankTransferCode'],
                            "FOpenAddressRec": i['FBankAddr'],
                            "FBankCurrencyId": {
                                "FNumber":
                                # "PRE001" if i['FCurrencyNo'] == '' else
                                    get_FCurrencyNo(app2, i['FSettleCurrencyNo'])
                            },
                            "FBankIsDefault": False
                        }
                    ],
                    "FSupplierContact": [
                        {
                            "FContactId": 0,
                            "FContact ": i['FContact'],
                            "FMobile": i['FMobile'],
                            "FEMail": i['FEMail']
                        }
                    ]
                }
            }
            res = api_sdk.Save("BD_Supplier", model)
            # print(f"{i['FNumber']}保存数据结果为:" + res)
            rj = json.loads(res)

            # print(res)
            #       rj是保存后的结果

            if rj['Result']['ResponseStatus']['IsSuccess']:

                returnResult = ERP_suppliersubmit(rj['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'],
                                                  api_sdk)
                #           rs是提交后的结果
                rs = json.loads(returnResult)

                if rs['Result']['ResponseStatus']['IsSuccess']:
                    resAudit = ERP_audit('BD_Supplier',
                                         rs['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'],
                                         api_sdk)
                    ra = json.loads(resAudit)
                    # ra是审核后的结果信息
                    if ra['Result']['ResponseStatus']['IsSuccess']:
                        if not supplierISExist(app2, i['FName'], "104"):
                            r = ERP_allocate('BD_Supplier', getCodeByView('BD_Supplier', rs['Result']['ResponseStatus'][
                                'SuccessEntitys'][0]['Number'], api_sdk),
                                             getOrganizationCode(app2, i['FApplyOrgName']), api_sdk)
                        FNumber104 = rs['Result']['ResponseStatus']['SuccessEntitys'][0]['Number']
                        AlloctOperation(api_sdk, i, app2, FNumber104, app3)

                        changeStatus(app3, "1", "RDS_ECS_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                        changeStatus(app3, "1", "RDS_ECS_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])
                        insert_log2(app3, "数据同步成功", i['FNumber'])
                    else:
                        insert_log(app3, ra, i['FNumber'])
                        changeStatus(app3, "2", "RDS_ECS_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                        changeStatus(app3, "2", "RDS_ECS_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])
                        print(ra)
                else:
                    insert_log(app3, rs, i['FNumber'])
                    changeStatus(app3, "2", "RDS_ECS_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                    changeStatus(app3, "2", "RDS_ECS_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])
                    print(rs)
            else:
                insert_log(app3, rj, i['FNumber'])
                changeStatus(app3, "2", "RDS_ECS_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                changeStatus(app3, "2", "RDS_ECS_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])
                print(rj)
        else:
            Exist_104 = checkExist(app2, i['FName'], 104)
            if Exist_104 == []:
                FNumber_104 = Exist_100[0]['FNUMBER']
                ERP_allocate('BD_Supplier', getCodeByView('BD_Supplier', FNumber_104, api_sdk),
                             getOrganizationCode(app2, i['FApplyOrgName']), api_sdk)

                AlloctOperation(api_sdk, i, app2, FNumber_104, app3)

                changeStatus(app3, "1", "RDS_ECS_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                changeStatus(app3, "1", "RDS_ECS_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])

            else:
                print("该编码{}已存在于金蝶".format(i['FNumber']))
                changeStatus(app3, "1", "RDS_ECS_ODS_bd_SupplierDetail", "FNumber", i['FNumber'])
                changeStatus(app3, "1", "RDS_ECS_SRC_bd_SupplierDetail", "FNumber", i['FNumber'])


# 数据分配后进行保存提交审核
def AlloctOperation(api_sdk, i, app2, FNumber104, app3):
    '''
    数据分配后进行提交审核
    :param forbid:
    :param number:
    :param api_sdk:
    :return:
    '''

    SaveAfterAllocation(api_sdk, i, app2, FNumber104, app3)


# 分配后保存，提交，审核
def SaveAfterAllocation(api_sdk, i, app2, FNumber104, app3):
    FOrgNumber = getOrganizationFNumber(app2, i['FApplyOrgName'])

    FINVOICETYPE_dict = {"增值税专用发票": "1", "普通发票": "2"}
    if FOrgNumber:
        model = {
            "Model": {
                "FSupplierId": queryDocuments(app2, api_sdk, FNumber104, FOrgNumber['FORGID']),
                "FCreateOrgId": {
                    "FNumber": "100"
                },
                "FUseOrgId": {
                    "FNumber": str(FOrgNumber['FNumber'])
                },
                "FGroup": {
                    "FNumber": get_supplierGrouping(app2, i['FSupplierCategoryNo'])
                },
                "FName": str(i['FName']),
                "FNumber": str(FNumber104)
                ,
                "FShortName": i['FShortName'],
                "FBaseInfo": {
                    "FCountry": {
                        "FNumber":
                        # "China" if i['FCountry'] == "" or i['FCountry'] == "中国" else
                            getCountryCode(app2, i['FCountry'])
                    },
                    "FSOCIALCRECODE": i['FUniversalCode'],
                    "FRegisterAddress": i['FRegisterAddress'],
                    # "FDeptId": {
                    #     "FNumber": rc.codeConversionOrg(app2, "rds_vw_department", i['FMngrDeptName'],
                    #                                     str(FOrgNumber['FNumber']))
                    # },
                    # "FStaffId": {
                    #     "FNumber": rc.codeConversion(app2, "rds_vw_employees", i['FMngrMan'])
                    # },
                    "FDeptId": {
                        "FNumber": 'BM000040'
                    },
                    "FStaffId": {
                        "FNumber": 'BSP00019'
                    },
                    "FZip": i['FZipCode'],
                    "FFoundDate": '',
                    "FRegisterCode": str(i['FUniversalCode']),
                    "FSupplyClassify":
                    # "CG" if i['FSullierType'] == "" else
                        getSullierTypeCode(i['FSullierType']),
                    "FSupplierGrade": {
                        "FNumber": get_auxiliary(app2, i['FSupplierGradeNo'])
                    }
                },
                "FBusinessInfo": {
                    "FPurchaserGroupId": {
                        "FNumber": "SKYX02"
                    },
                    "FSettleTypeId": {
                        "FNumber": get_settlement(app2, i['FSettlementMethodNo'])
                    },
                    "FPRICELISTID": {
                        "FNumber": str(i['FPriceListNo'])
                    },
                    "FProviderId": {
                        "FNumber": str(FNumber104)
                    },
                    "FVmiBusiness": False,
                    "FEnableSL": False
                },
                "FFinanceInfo": {
                    "FPayCurrencyId": {
                        "FNumber":
                        # "PRE001" if i['FCurrencyNo'] == '' else
                            get_FCurrencyNo(app2, i['FSettleCurrencyNo'])
                    },
                    "FPayCondition": {
                        "FNumber": get_payment(app2, i['FPaymentConditionNo'])
                    },
                    "FSettleId": {
                        "FNumber": str(FNumber104)
                    },
                    "FTaxType": {
                        "FNumber": "SFL02_SYS"
                    },
                    "FTaxRegisterCode": str(i['FUniversalCode']),
                    "FChargeId": {
                        "FNumber": str(FNumber104)
                    },
                    "FInvoiceType":
                        FINVOICETYPE_dict.get(i['FInvoiceType'], ""),
                    # "1" if (i['FInvoiceType'] == "" or i['FInvoiceType'] == "增值税专用发票") else "2",
                    "FTaxRateId": {
                        "FNUMBER":
                        # "SL02_SYS" if i['FTaxRate'] == "" else
                            getTaxRateCode(app2, i['FTaxRate'])
                    }
                },
                "FBankInfo": [
                    {
                        "FBankCountry": {
                            "FNumber":
                            # "China" if i['FCountry'] == "" or i['FCountry'] == "中国" else
                                getCountryCode(app2, i['FCountry'])
                        },
                        "FBankCode": i['FAccountNumber'],
                        "FBankHolder": i['FAccountName'],
                        "FOpenBankName": i['FBankName'],
                        "FCNAPS": i['FBankTransferCode'],
                        "FOpenAddressRec": i['FBankAddr'],
                        "FBankCurrencyId": {
                            "FNumber":
                            # "PRE001" if i['FCurrencyNo'] == '' else
                                get_FCurrencyNo(app2, i['FSettleCurrencyNo'])
                        },
                        "FBankIsDefault": False
                    }
                ],
                "FSupplierContact": [
                    {
                        "FContactId": 0,
                        "FContact ": i['FContact'],
                        "FMobile": i['FMobile'],
                        "FEMail": i['FEMail']
                    }
                ]
            }
        }
        res = json.loads(api_sdk.Save("BD_Supplier", model))

        print(f"{FNumber104}修改数据结果为:" + str(res))

        if res['Result']['ResponseStatus']['IsSuccess']:
            submit_res = json.loads(ERP_suppliersubmit(FNumber104, api_sdk))
            audit_res = json.loads(ERP_audit("BD_Supplier", FNumber104, api_sdk))
        else:
            insert_log(app3, res, FNumber104)



def FNumber_get_supplier(app2, app3, option1, FNumber):
    # 新账套

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    data_info_list = ECS_post_info(url, 1, 1000, "eq", "v_supplier", FNumber, "FNUMBER")

    if data_info_list:
        for i in range(len(data_info_list)):

            data_bank = ECS_post_info(url, 1, 1000, "eq", "v_supplier_bank_property", data_info_list[i]['FNUMBER'],
                                      "FNUMBER")

            data_base = ECS_post_info(url, 1, 1000, "eq", "v_supplier_base_property", data_info_list[i]['FNUMBER'],
                                      "FNUMBER")

            data_contact = ECS_post_info(url, 1, 1000, "eq", "v_supplier_contact", data_info_list[i]['FNUMBER'],
                                         "FNUMBER")

            data_business = ECS_post_info(url, 1, 1000, "eq", "v_supplier_business_property",
                                          data_info_list[i]['FNUMBER'],
                                          "FNUMBER")
            if len(data_bank) == 0:
                data_bank = [{}]
            if len(data_base) == 0:
                data_base = [{}]
            if len(data_contact) == 0:
                data_contact = [{}]
            if len(data_business) == 0:
                data_business = [{}]

            data = combination(data_info_list[i], data_bank[0], data_base[0], data_contact[0], data_business[0])

            if data['FUniversalCode'] == '':
                ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '纳税登记号为空'}]}}}
                insert_log(app2, ero, data['FNumber'])
                print(f"{data['FName']}纳税登记号为空")


            sql = f"""select FNumber from RDS_ECS_SRC_bd_SupplierDetail"""
            res = app3.select(sql)
            FNumbers = []
            for name_date in res:
                FNumbers.append(name_date['FNumber'])

            if data['FNumber'] not in FNumbers:
                insert_data(app3, data)

                NOAccount(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3")

                # acc = NOAccount()
                # acc.update_RDS_ECS_ODS_bd_SupplierDetail()

                print(f"{data_info_list[i]['FNUMBER']}插入成功")
            else:

                insert_log2(app3, f"{data_info_list[i]['FNUMBER']}已存在数据库", data['FNumber'])

                print(f"{data_info_list[i]['FNUMBER']}已存在数据库")
    else:

        insert_log2(app3, f"{FNumber}请求数据为空", FNumber)


    # 写入金蝶
    judgeDetailData(option1, app2=app2,app3=app3)


def FDate_get_supplier_bydate(app2, app3, option1, Fdate):
    # 新账套

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    data_info_list = ECS_post_info(url, 1, 1000, "like", "v_supplier", Fdate, "UPDATETIME")

    if data_info_list:
        for i in range(len(data_info_list)):

            data_bank = ECS_post_info(url, 1, 1000, "eq", "v_supplier_bank_property", data_info_list[i]['FNUMBER'],
                                      "FNUMBER")

            data_base = ECS_post_info(url, 1, 1000, "eq", "v_supplier_base_property", data_info_list[i]['FNUMBER'],
                                      "FNUMBER")

            data_contact = ECS_post_info(url, 1, 1000, "eq", "v_supplier_contact", data_info_list[i]['FNUMBER'],
                                         "FNUMBER")

            data_business = ECS_post_info(url, 1, 1000, "eq", "v_supplier_business_property",
                                          data_info_list[i]['FNUMBER'],
                                          "FNUMBER")
            if len(data_bank) == 0:
                data_bank = [{}]
            if len(data_base) == 0:
                data_base = [{}]
            if len(data_contact) == 0:
                data_contact = [{}]
            if len(data_business) == 0:
                data_business = [{}]

            data = combination(data_info_list[i], data_bank[0], data_base[0], data_contact[0], data_business[0])

            if data['FUniversalCode'] == '':
                ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '纳税登记号为空'}]}}}
                insert_log(app3, ero, data['FNumber'])
                print(f"{data['FName']}纳税登记号为空")
                continue

            sql = f"""select FName from RDS_ECS_SRC_bd_SupplierDetail"""
            res = app3.select(sql)
            fnames = []
            for name_date in res:
                fnames.append(name_date['FName'])

            if data['FName'] not in fnames:
                insert_data(app3, data)

                NOAccount(FToken="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3")

                # acc = NOAccount()
                # acc.update_RDS_ECS_ODS_bd_SupplierDetail()

                print(f"{data_info_list[i]['FNUMBER']}插入成功")
            else:
                print(f"{data_info_list[i]['FNUMBER']}已存在数据库")

    # 写入金蝶
    judgeDetailData(option1, app2,app3)
