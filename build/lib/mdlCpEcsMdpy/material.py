from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk
import json
import time
import hashlib
import requests


# ECS的token加密
from pyecsbill.threebasicdata import NOAccount


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
    m.update(token.encode())
    md5 = m.hexdigest()
    return md5


# 请求ECS数据
def ECS_post_infoFNumber(url, pageNum, pageSize, qw, tableName, updateTime, key):
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


    return data_info_list


# 查询最大FInterId
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


# 修改Fisdo
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


# ECS数据组成SRC.model
def combination(data_info, data_bace, data_stock):
    '''
    组装数据
    :return:
    '''

    model = {
        # [FInterId]
        "FDeptId": '',  # 申请部门
        "FUserId": data_info['FUserId'],  # 申请人
        "FApplyOrgName": '苏州生科云选生物科技有限公司',  # 申请组织
        "FVarDateTime": data_info['FVarDateTime'],  # 申请日期
        "FNumber": data_info['FNumber'],  # 物料编码
        "FName": data_info['FName'],  # 物料名称
        "FSpecification": data_info['FSpecification'],  # 规格型号
        "FDescription": data_info['Fdescription'],  # 物料描述
        "FTaxRateId": data_bace.get('FTaxRateId'),  # 默认税率%
        "FGROSSWEIGHT": data_bace.get('FGROSSWEIGHT'),  # 毛重
        "FNETWEIGHT": data_bace.get('FNETWEIGHT'),  # 净重
        "FLENGTH": data_bace.get('FLENGTH'),  # 长
        "FWIDTH": data_bace.get('FWIDTH'),  # 宽
        "FHEIGHT": data_bace.get('FHEIGHT'),  # 高
        "FVOLUME": data_bace.get('FVOLUME'),  # 体积
        "FSafeStock": data_stock.get('FSafeStock'),  # 安全库存
        "FMinPackCount": '',  # 最小包装数
        "FPlanningStrategy": '',  # 计划策略
        "FOrderPolicy": '',  # 订货策略
        "FFixLeadTime": '',  # 固定提前期
        "FFixLeadTimeType": '',  # 固定提前期单位
        "FVarLeadTime": '',  # 变动提前期
        "FVarLeadTimeType": '',  # 变动提前期单位
        "FOrderIntervalTimeType": '',  # 订货间隔期单位
        "FOrderIntervalTime": '',  # 订货间隔期
        "FMaxPOQty": '',  # 最大订货量
        "FMinPOQty": '',  # 最小订货量
        "FIncreaseQty": '',  # 最小包装量
        "FEOQ": '',  # 固定/经济批量
        "FVarLeadTimeLotSize": '',  # 变动提前期批量
        "FFinishReceiptOverRate": '',  # 入库超收比例(%
        "FFinishReceiptShortRate": '',  # 入库欠收比例(%
        "FMinIssueQty": '',  # 最小发料批量
        "F_SZSP_CheckBox": '',  # 启用条码管理
        "FISMinIssueQty": '',  # 领料考虑最小发料批量
        "FIsBatchManage": data_stock.get('FBatchIf'),  # 启用批号管理
        "FOverControlMode": '',  # 超发控制方式
        "FIsKFPeriod": data_stock.get('FQuanlity'),  # 启用保质期
        "FCheckOut": '',  # 检验设置
        "FErpClsID": data_bace.get('FErpClsNo'),  # 物料属性编码
        "FCategoryID": data_bace.get('FCategoryNo'),  # 存货类别编码
        "FTaxCategoryCodeId": '',  # 税收分类编码
        "FWEIGHTUNITID": data_bace.get('FWEIGHTUNITNO'),  # 重量单位编码
        "FBaseUnitId": data_bace.get('FBaseUnitNo'),  # 基本单位编码
        "FVOLUMEUNITID": data_bace.get('FVOLUMEUNITNO'),  # 尺寸单位编码
        "FBatchRuleID": data_stock.get('FBatchRuleNo'),  # 批号规则编码
        "F_SZSP_Assistant": data_info['FSZSPCPDLNO'],  # 产品大类
        # "FUploadDate": data_info['UPDATETIME'], # 修改时间
        "FUploadDate": "",  # 上传时间
        # "Fisdo":0
        "FChecker": '',
        "FOldNumber": '',  # 旧物料编码
        "FIsPurchase": 'null',  # 允许采购
        "F_SZSP_Decimal": 'null',  # 中小包装换算比
        "F_SZSP_Decimal1": 'null',  # 大中包装换算比
        "F_SZSP_SKUnumber": '',  # SKU编码
        "F_SZSP_PackCoefficient": '',  # 包装规格
        "FExpUnit": '',  # 保质期单位
        "FExpPeriod": 'null',  # 保质期
        "FIsEnableSafeStock": 'null',  # 启用安全库存
        "FIsEnableMinStock": 'null',  # 启用最小库存
        "FMinStock": 'null',  # 最小库存
        "FIsEnableMaxStock": 'null',  # 启用最大库存
        "FMaxStock": 'null',  # 最大库存
        "F_SZSP_Itemnumber": 'null',
        'FBaseProperty': data_info['FMaterialGroupCode'],  # 物料分组编码
        'FMaterialGroup': data_info['FMaterialGroupNo'],  # 物料分组
        'FSupplierMaterialCode': data_info['FSupplierMaterialCode'],  # 赛普物料编码
        "FMaterialGroupName": data_info['FMaterialGroupName'],
        "FParentMaterialGroupCode": data_info['FParentMaterialGroupCode'],
        "FParentMaterialGroupName": data_info['FParentMaterialGroupName'],
    }
    for key in model:
        if model.get(key) == None:
            model[key] = ''
    return model


# 判断数据是否存在SRC
def dataExistDms(app3, FNumber):
    '''
    判断数据是否存在SRC
    :return:
    '''

    sql = f"""select FNumber from RDS_ECS_SRC_bd_MaterialDetail where FNumber='{FNumber}'"""

    res = app3.select(sql)

    if res:

        return False

    else:

        return True

    pass


# ECS数据插入SRC
def insert_data(app3, data):
    '''
    将数据写入DMS
    :param app2:
    :param data:
    :return:
    '''

    sql = f"""insert into RDS_ECS_SRC_bd_MaterialDetail(FInterId,FDeptId,FUserId,FApplyOrgName,FVarDateTime,FNumber,FName,FSpecification,
        FDescription,FTaxRateId,FGROSSWEIGHT,FNETWEIGHT,FLENGTH,FWIDTH,FHEIGHT,FVOLUME,FSafeStock,FMinPackCount,
        FPlanningStrategy,FOrderPolicy,FFixLeadTime,FFixLeadTimeType,FVarLeadTime,FVarLeadTimeType,FOrderIntervalTimeType,
        FOrderIntervalTime,FMaxPOQty,FMinPOQty,FIncreaseQty,FEOQ,FVarLeadTimeLotSize,FFinishReceiptOverRate,
        FFinishReceiptShortRate,FMinIssueQty,F_SZSP_CheckBox,FISMinIssueQty,FIsBatchManage,FOverControlMode,FIsKFPeriod,
        FCheckOut,FMaterialGroup,FErpClsID,FCategoryID,FTaxCategoryCodeId,FWEIGHTUNITID,FBaseUnitId,FVOLUMEUNITID,
        FBatchRuleID,F_SZSP_Assistant,FUploadDate,Fisdo,FChecker,FOldNumber,FIsPurchase,F_SZSP_Decimal,F_SZSP_Decimal1,
        F_SZSP_SKUnumber,F_SZSP_PackCoefficient,FExpUnit,FExpPeriod,FIsEnableSafeStock,FIsEnableMinStock,FMinStock,
        FIsEnableMaxStock,FMaxStock,F_SZSP_Itemnumber,FBaseProperty,FSupplierMaterialCode,FMaterialGroupName,FMaterialGroupCode,FParentMaterialGroupCode,FParentMaterialGroupName) values({getFinterId(app3, 'RDS_ECS_SRC_bd_MaterialDetail') + 1},
        '{data.get('FDeptId', '')}','{data.get('FUserId', '')}','{data.get('FApplyOrgName', '')}', '{data.get('FVarDateTime', '')}',
        '{data.get('FNumber', '')}', '{data.get('FName', '').replace("'","''")}','{data.get('FSpecification', '')}', '{data.get('FDescription', '')}', 
        '{data.get('FTaxRateId', '')}', '{data.get('FGROSSWEIGHT', '')}','{data.get('FNETWEIGHT', '')}', '{data.get('FLENGTH', '')}', 
        '{data.get('FWIDTH', '')}','{data.get('FHEIGHT', '')}', '{data.get('FVOLUME', '')}', '{data.get('FSafeStock', '')}',
        '{data.get('FMinPackCount', '')}', '{data.get('FPlanningStrategy', '')}', '{data.get('FOrderPolicy', '')}',
        '{data.get('FFixLeadTime', '')}','{data.get('FFixLeadTimeType', '')}', '{data.get('FVarLeadTime', '')}', 
        '{data.get('FVarLeadTimeType', '')}', '{data.get('FOrderIntervalTimeType', '')}','{data.get('FOrderIntervalTime', '')}', 
        '{data.get('FMaxPOQty', '')}', '{data.get('FMinPOQty', '')}', '{data.get('FIncreaseQty', '')}', '{data.get('FEOQ', '')}', 
        '{data.get('FVarLeadTimeLotSize', '')}', '{data.get('FFinishReceiptOverRate', '')}','{data.get('FFinishReceiptShortRate', '')}',
        '{data.get('FMinIssueQty', '')}','{data.get('F_SZSP_CheckBox', '')}','{data.get('FISMinIssueQty', '')}',
        '{data.get('FIsBatchManage', '')}','{data.get('FOverControlMode', '')}','{data.get('FIsKFPeriod', '')}',
        '{data.get('FCheckOut', '')}','{data.get('FMaterialGroup', '')}','{data.get('FErpClsID', '')}','{data.get('FCategoryID', '')}',
        '{data.get('FTaxCategoryCodeId', '')}','{data.get('FWEIGHTUNITID', '')}',
        '{data.get('FBaseUnitId', '')}','{data.get('FVOLUMEUNITID', '')}','{data.get('FBatchRuleID', '')}',
        '{data.get('F_SZSP_Assistant', '')}',getdate(),0,'{data.get('FChecker', '')}',
        '{data.get('FOldNumber', '')}',{data.get('FIsPurchase', '')},{data.get('F_SZSP_Decimal', '')},{data.get('F_SZSP_Decimal1', '')},
        '{data.get('F_SZSP_SKUnumber', '')}','{data.get('F_SZSP_PackCoefficient', '')}','{data.get('FExpUnit', '')}',
        {data.get('FExpPeriod', '')},{data.get('FIsEnableSafeStock', '')},{data.get('FIsEnableMinStock', '')},
        {data.get('FMinStock', '')},{data.get('FIsEnableMaxStock', '')},{data.get('FMaxStock', '')},
        {data.get('F_SZSP_Itemnumber', '')},'{data.get('FBaseProperty', '')}','{data.get('FSupplierMaterialCode')}','{data.get('FMaterialGroupName')}','{data.get('FMaterialGroupCode')}','{data.get('FParentMaterialGroupCode')}','{data.get('FParentMaterialGroupName')}')"""


    print(sql)

    app3.insert(sql)




# 插入错误日志
def insert_log(app2, res, FNumber, cp='赛普'):
    FMessages = []

    for errors in res['Result']['ResponseStatus']['Errors']:
        FMessages.append(errors['Message'])

    FMessages = ','.join(FMessages)

    FOccurrenceTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    sql = f"""insert into RDS_ECS_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName) 

    values('ECS物料','{FNumber}','{FMessages}','{FOccurrenceTime}','{cp}')"""

    app2.insert(sql)

def insert_log2(app2, res, FNumber, cp='赛普'):

    sql = f"""insert into RDS_ECS_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName) 

    values('ECS物料','{FNumber}','{res}',getdate(),'{cp}')"""

    app2.insert(sql)


# 编码请求ECS数据，插入SRC
def ecsToDmsByFNumber(app3, codeList):
    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    for fnumber in codeList:

        data_info_lists = ECS_post_infoFNumber(url, 1, 1000, "eq", "v_material", fnumber, "FNumber")

        for i in range(len(data_info_lists)):
            data_info_list = data_info_lists[i]

            data_base = ECS_post_infoFNumber(url, 1, 1000, "eq", "v_material_base", data_info_list['FNumber'],
                                                "FNumber")

            data_stock = ECS_post_infoFNumber(url, 1, 1000, "eq", "v_material_stock", data_info_list['FNumber'],
                                                 "FNumber")

            if len(data_base) == 0:
                data_base = [{}]

            if len(data_stock) == 0:
                data_stock = [{}]

            data = combination(data_info_list, data_base[0], data_stock[0])

            print(data)

            if (data['FBaseProperty'] == '' or data['FBaseProperty'] == None) \
                    and (data['FSupplierMaterialCode'] == '' or data['FSupplierMaterialCode'] == None):

                ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '物料无分组信息'}]}}}

                insert_log(app3, ero, data['FNumber'])

                print(f"{data['FNumber']}物料无分组信息")

                continue

            else:

                checkRes = dataExistDms(app3, data['FNumber'])

                if checkRes:

                    insert_data(app3, data)

                    acc = NOAccount()
                    acc.update_RDS_ECS_ODS_bd_MaterialDetail()

                    print(f"{data_info_list['FNumber']}插入成功")

                else:

                    insert_log2(app3, f"{data_info_list['FNumber']}已存在src", data['FNumber'])



# 日期请求ECS数据，插入SRC
def ecsToDmsByFDate(app3, date):
    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    data_info_lists = ECS_post_infoFNumber(url, 1, 1000, "like", "v_material", date, "UPDATETIME")

    for i in range(len(data_info_lists)):
        data_info_list = data_info_lists[i]

        data_base = ECS_post_infoFNumber(url, 1, 1000, "eq", "v_material_base", data_info_list['FNumber'],
                                            "FNumber")

        data_stock = ECS_post_infoFNumber(url, 1, 1000, "eq", "v_material_stock", data_info_list['FNumber'],
                                             "FNumber")

        if len(data_base) == 0:
            data_base = [{}]

        if len(data_stock) == 0:
            data_stock = [{}]

        data = combination(data_info_list, data_base[0], data_stock[0])

        if (data['FBaseProperty'] == '' or data['FBaseProperty'] == None) \
                and (data['FSupplierMaterialCode'] == '' or data['FSupplierMaterialCode'] == None):

            ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '物料无分组信息'}]}}}

            insert_log(app3, ero, data['FNumber'])

            print(f"{data['FNumber']}物料无分组信息")

            continue

        else:

            checkRes = dataExistDms(app3, data['FNumber'])

            if checkRes:

                insert_data(app3, data)

                print(f"{data_info_list['FNumber']}插入成功")

            else:

                print(f"{data_info_list['FNumber']}已存在src")


# ODS到ERP
def dmsToErp(app2, app3, option, api_sdk):
    '''
    DMS数据进ERP
    :param option:
    :return:
    '''

    odsResult = getOdsData(app3)

    if odsResult:

        erp_save(app2=app2, app3=app3, option=option, api_sdk=api_sdk, data=odsResult)

        pass

    else:

        return "没有数据需要同步"


# 查询ODS待处理数据
def getOdsData(app3):
    '''
    获得ODS表中FIsdo未0的数据
    :param app3:
    :return:
    '''

    sql = """select FDeptId,FUserId,FApplyOrgName,FVarDateTime,FNumber,FName,FSpecification,
        FDescription,FTaxRateId,FGROSSWEIGHT,FNETWEIGHT,FLENGTH,FWIDTH,FHEIGHT,FVOLUME,FSafeStock,FMinPackCount,
        FPlanningStrategy,FOrderPolicy,FFixLeadTime,FFixLeadTimeType,FVarLeadTime,FVarLeadTimeType,FOrderIntervalTimeType,
        FOrderIntervalTime,FMaxPOQty,FMinPOQty,FIncreaseQty,FEOQ,FVarLeadTimeLotSize,FFinishReceiptOverRate,
        FFinishReceiptShortRate,FMinIssueQty,F_SZSP_CheckBox,FISMinIssueQty,FIsBatchManage,FOverControlMode,FIsKFPeriod,
        FCheckOut,FMaterialGroup,FErpClsID,FCategoryID,FTaxCategoryCodeId,FWEIGHTUNITID,FBaseUnitId,FVOLUMEUNITID,
        FBatchRuleID,F_SZSP_Assistant,FUploadDate,Fisdo,FChecker,FOldNumber,FIsPurchase,F_SZSP_Decimal,F_SZSP_Decimal1,
        F_SZSP_SKUnumber,F_SZSP_PackCoefficient,FExpUnit,FExpPeriod,FIsEnableSafeStock,FIsEnableMinStock,FMinStock,
        FIsEnableMaxStock,FMaxStock,F_SZSP_Itemnumber,FBaseProperty,FSupplierMaterialCode,FMaterialGroupName,FMaterialGroupCode,FParentMaterialGroupCode,FParentMaterialGroupName from RDS_ECS_ODS_bd_MaterialDetail where FIsdo=0"""

    res = app3.select(sql)

    return res


# 通过名称获取编码
def getcode(app2, FName):
    '''
    获取对应的编码
    :param app2:
    :return:
    '''

    if FName == 'g':
        FName = '克'

    if FName == 'kg':
        FName = '千克'

    if FName == 'ml':
        FName = '毫升'

    if FName == '只':
        FName = 'Pcs'

    sql = f"select FNumber from rds_vw_unit where FNAME='{FName}'"

    res = app2.select(sql)

    if res:

        return res[0]['FNumber']

    else:

        return False


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


# ERP是，否编码转换
def exchangeBooleanValue(param):
    '''
    逻辑值转换
    :param param:
    :return:
    '''

    if param == "是":
        return True
    elif param == "否":
        return False


# ERP天，周，月，编码转换
def exchangeDateCode(param):
    if param == "天":
        return "1"
    elif param == "周":
        return "2"
    elif param == "月":
        return "3"


# 物料分组最大号加一
def get_max_number(app, FNUMBER):
    '''
    通过分组来获取物料编码
    :param app:
    :param FNUMBER:
    :return:
    '''

    sql = f'''select max(a.FNUMBER) as maxnumber,c.FNUMBER as 'groupNumber',a.FUSEORGID from T_BD_MATERIAL a
    inner join T_BD_MATERIAL_P b
    on b.FMATERIALID=a.FMATERIALID
    inner join rds_vw_materialgrouping c
    on c.FID=a.FMATERIALGROUP
    where a.FUSEORGID=1 and a.FFORBIDSTATUS='A' and c.FNUMBER='{FNUMBER}'
    group by a.FUSEORGID,a.FFORBIDSTATUS,c.FNUMBER'''

    data = app.select(sql)

    if data:

        maxnumber = data[0]['maxnumber']

        maxnumber_list = maxnumber.split('.')

        maxnumber_list[-1] = str(int(maxnumber_list[-1]) + 1).rjust(6, '0')

        fnumber = '.'.join(maxnumber_list)

    else:
        sql2 = f"""select * from rds_vw_materialgrouping where FNUMBER = '{FNUMBER}'"""
        group_date = app.select(sql2)
        if group_date:
            fnumber = FNUMBER + '.000001'
        else:
            fnumber = ''
        # print('查无此分组')

    return fnumber


# 提交
def erp_submit(api_sdk, FNumber):
    '''
    调用保存接口将物料提交
    :param api_sdk:
    :param FNumber:
    :return:
    '''

    model = {
        "CreateOrgId": 0,
        "Numbers": [FNumber],
        "Ids": "",
        "SelectedPostId": 0,
        "NetworkCtrl": "",
        "IgnoreInterationFlag": ""
    }

    res = api_sdk.Submit("BD_MATERIAL", model)

    return res


# 审核
def erp_audit(formid, api_sdk, FNumber):
    '''
    调用审核接口将数据审核
    :param api_sdk:
    :param FNumber:
    :return:
    '''

    model = {
        "CreateOrgId": 0,
        "Numbers": [FNumber],
        "Ids": "",
        "InterationFlags": "",
        "NetworkCtrl": "",
        "IsVerifyProcInst": "",
        "IgnoreInterationFlag": ""
    }

    res = api_sdk.Audit(formid, model)

    return res


# 分配
def erp_allocation(forbid, PkIds, TOrgIds, api_sdk):
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


# 非赛普物料，通过sku判断是否存在ERP
def isExistErp(app2, FNumber, FOrg):
    '''
    判断数据是否存在erp系统
    :param app2:
    :param FNumber:
    :param FOrg:
    :return:
    '''

    sql = f"select FNUMBER from rds_vw_material where F_SZSP_SKUNUMBER='{FNumber}' and FOrgNumber='{FOrg}'"

    res = app2.select(sql)

    if res:

        return False

    else:

        return True


# 赛普物料，通过赛普编码判断是否存在ERP
def FNumberisExistErp(app2, FNumber, FOrg):
    '''
    判断数据是否存在erp系统
    :param app2:
    :param FNumber:
    :param FOrg:
    :return:
    '''

    sql = f"select FNUMBER from rds_vw_material where FNUMBER='{FNumber}' and FOrgNumber='{FOrg}'"

    res = app2.select(sql)

    if res:

        return False

    else:

        return True


# 查100里的sku
def F_SZSP_SKUNUMBER_get_100(app2, FNumber, FOrg):
    '''
    查sku
    :param app2:
    :param FNumber:
    :param FOrg:
    :return:
    '''

    sql = f"select F_SZSP_SKUNUMBER from rds_vw_material where FNUMBER='{FNumber}' and FOrgNumber='{FOrg}'"

    res = app2.select(sql)

    if res:

        return res[0]['F_SZSP_SKUNUMBER']

    else:

        return ''


# 分配审核后修改SKU编码，物料属性。存货类别
def self_madeAllocation(app2, FSkuNumber, FNumber, F_SZSP_SKUNUMBER_100):
    '''
    该SKU编码，物料属性。存货类别
    :param app2:
    :param FSkuNumber:
    :param FAttribute:
    :param FCategory:
    :return:
    '''
    if F_SZSP_SKUNUMBER_100:
        sql = f"""update a set a.F_SZSP_SKUNUMBER='{F_SZSP_SKUNUMBER_100}' from T_BD_MATERIAL a
                inner join T_BD_MATERIALBASE b
                on a.FMATERIALID=b.FMATERIALID
                where a.FNUMBER='{FNumber}'"""

        app2.update(sql)

    else:
        sql = f"""update a set a.F_SZSP_SKUNUMBER='{FSkuNumber}' from T_BD_MATERIAL a
                        inner join T_BD_MATERIALBASE b
                        on a.FMATERIALID=b.FMATERIALID
                        where a.FNUMBER='{FNumber}'"""

        app2.update(sql)

    sql2 = f"""update a set a.FERPCLSID=1,a.FCATEGORYID=151346,a.FISPURCHASE=1 from T_BD_MATERIALBASE a
                inner join T_BD_MATERIAL b
                on a.FMATERIALID=b.FMATERIALID
                where b.FNUMBER='{FNumber}' and b.FUSEORGID='111785'"""

    app2.update(sql2)


# ERP保存，提交，审核，分配，保存，提交，审核
def erp_save(app2, app3, option, api_sdk, data):
    '''
    在erp系统新建数据
    :param app2:
    :param option:
    :param api_sdk:
    :param data:
    :return:
    '''

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    for i in data:

        if i['FSupplierMaterialCode'] == '':
            # 不是赛普的物料，走新建路线

            if isExistErp(app2, i['FNumber'], "100"):
                # 不存在新建

                action(i, app2, api_sdk, app3)

                pass

            else:

                if isExistErp(app2, i['FNumber'], "104"):

                    # 这种情况一般不可能
                    try:
                        result = erp_allocation('BD_MATERIAL', getCodeByView('BD_MATERIAL',
                                                                             i['FNumber'],
                                                                             api_sdk),
                                                getOrganizationCode(app2, i['FApplyOrgName']), api_sdk)
                    except Exception as e:
                        # ero = ['Result']['ResponseStatus']['Errors']
                        ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '查无此内码'}]}}}
                        insert_log(app3, ero, i['FNumber'])
                        continue

                    erp_submit(api_sdk, i['FNumber'])
                    erp_audit('BD_MATERIAL', api_sdk, i['FNumber'])

                    changeStatus(app3, "1", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
                    changeStatus(app3, "1", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])


                else:

                    # 物料已存在erp系统，跳过本次循环
                    changeStatus(app3, "1", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
                    changeStatus(app3, "1", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])
                    continue

                pass

        else:

            # 是赛普的物料，走分配路线

            if FNumberisExistErp(app2, i['FSupplierMaterialCode'], "104"):
                # 物料存在100组织，不存在104组织，把物料分配到104

                # 查100里的sku
                F_SZSP_SKUNUMBER_100 = F_SZSP_SKUNUMBER_get_100(app2, i['FSupplierMaterialCode'], "100")

                try:
                    result = erp_allocation('BD_MATERIAL', getCodeByView('BD_MATERIAL',
                                                                         i['FSupplierMaterialCode'],
                                                                         api_sdk),
                                            getOrganizationCode(app2, i['FApplyOrgName']), api_sdk)
                except Exception as e:
                    # ero = ['Result']['ResponseStatus']['Errors']
                    ero = {'Result': {'ResponseStatus': {'Errors': [{'Message': '查无此内码'}]}}}
                    insert_log(app3, ero, i['FSupplierMaterialCode'])
                    changeStatus(app3, "2", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
                    changeStatus(app3, "2", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])
                    continue

                erp_submit(api_sdk, i['FSupplierMaterialCode'])
                erp_audit('BD_MATERIAL', api_sdk, i['FSupplierMaterialCode'])

                print(F"{i['FSupplierMaterialCode']}分配结果为{result}")

                self_madeAllocation(app2, i['FNumber'], i['FSupplierMaterialCode'], F_SZSP_SKUNUMBER_100)

                changeStatus(app3, "1", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
                changeStatus(app3, "1", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])

                pass

            else:

                # 物料已存在erp系统，跳过本次循环
                print(f"{i['FSupplierMaterialCode']}已存在ERP")
                changeStatus(app3, "1", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
                changeStatus(app3, "1", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])
                continue

            pass

        pass

    pass


def getGroupNumber(app2, FNumber):
    '''
    获取父分组的id
    :param app2:
    :param FNumber:
    :return:
    '''

    sql = f"""select FID from rds_vw_materialgrouping where FNUMBER='{FNumber}'"""

    res = app2.select(sql)

    if res:

        return res[0]['FID']

    else:

        return []


def materialGroupExist(app2, FNumber, FPNumber, FName, api_sdk):
    '''
    查看分组是否存在
    :param app2:
    :param FNumber:
    :return:
    '''
    sql = f"select * from rds_vw_materialgrouping where FNUMBER='{FNumber}'"

    res = app2.select(sql)

    if res == []:

        res2 = getGroupNumber(app2, FPNumber)

        if res2:

            return create_materialGroup(api_sdk=api_sdk, FParentId=res2, FNumber=FNumber, FName=FName)

        else:

            return False

    else:

        return True


def create_materialGroup(api_sdk,  FParentId, FNumber, FName):


    model = {

        "GroupFieldKey": "",
        "GroupPkId": 0,
        "FParentId": int(FParentId),
        "FNumber": FNumber,
        "FName": FName,
        "FDescription": ""

    }

    res = api_sdk.GroupSave("BD_MATERIAL", model)

    return json.loads(res)["Result"]["ResponseStatus"]["IsSuccess"]

# 非赛普物料新建
def action(i, app2, api_sdk, app3):
    '''
    执行操作
    :return:
    '''

    if materialGroupExist(app2, i['FBaseProperty'], i['FParentMaterialGroupCode'], i['FMaterialGroupName'], api_sdk):

        i['MaterialID'] = get_max_number(app2, i['FBaseProperty'])

        if i['MaterialID']:

            unit = getcode(app2, i['FBaseUnitId'])

            model = {
                "Model": {
                    "FMATERIALID": 0,
                    "FCreateOrgId": {
                        "FNumber": "100"
                    },
                    "FUseOrgId": {
                        "FNumber": "100"
                    },
                    "FMaterialGroup": {
                        "FNumber": i['FBaseProperty']
                    },
                    "FNumber": i['MaterialID'],
                    "FName": i['FName'],
                    "FSpecification": i['FSpecification'],
                    "FDescription": i['FDescription'].replace("&nbsp", "").replace("<br>", "\n"),
                    "FDSMatchByLot": False,
                    "FImgStorageType": "A",
                    "F_SZSP_Assistant": {
                        # "FNumber": i['F_SZSP_Assistant']
                        "FNumber": 11  # 未分类
                    },
                    "FIsSalseByNet": False,
                    # "F_SZSP_Decimal": 1.0,
                    # "F_SZSP_Decimal1": 1.0,
                    "F_SZSP_Decimal": '',
                    "F_SZSP_Decimal1": '',
                    "F_SZSP_SKUnumber": i['FNumber'],
                    "FSubHeadEntity": {
                        "FIsControlSal": False,
                        "FIsAutoRemove": False,
                        "FIsMailVirtual": False,
                        "FTimeUnit": "H",
                        "FIsPrinttAg": False,
                        "FIsAccessory": False
                    },
                    "SubHeadEntity": {
                        "FErpClsID": "1",
                        "FFeatureItem": "1",
                        "FCategoryID": {
                            "FNumber": 'CHLB08_SYS'
                        },
                        "FTaxType": {
                            "FNumber": i['FTaxCategoryCodeId']
                        },
                        "FTaxRateId": {
                            "FNUMBER": getcode(app2, i['FTaxRateId'])
                        },
                        "FBaseUnitId": {
                            "FNumber": unit
                        },
                        "FGROSSWEIGHT": i['FGROSSWEIGHT'],
                        "FNETWEIGHT": i['FNETWEIGHT'],
                        "FWEIGHTUNITID": {
                            "FNUMBER":
                                i['FWEIGHTUNITID']
                        },
                        "FLENGTH": i['FLENGTH'],
                        "FWIDTH": i['FWIDTH'],
                        "FHEIGHT": i['FHEIGHT'],
                        "FVOLUME": i['FVOLUME'],
                        "FVOLUMEUNITID": {
                            "FNUMBER":
                            # "m" if i['FVOLUMEUNITID'] == '' else
                                i['FVOLUMEUNITID']
                        }
                    },
                    "SubHeadEntity1": {
                        "FStoreUnitID": {
                            "FNumber": unit
                        },
                        "FUnitConvertDir": "1",
                        "FIsLockStock": True,
                        "FIsCycleCounting": False,
                        "FCountCycle": "1",
                        "FCountDay": 1,
                        "FIsMustCounting": False,
                        # "FIsBatchManage": False if i['FIsBatchManage'] == "" else rc.exchangeBooleanValue(
                        #     i['FIsBatchManage']),
                        "FIsBatchManage": True,
                        "FBatchRuleID": {
                            "FNumber": "PHBM001"
                        },
                        "FIsKFPeriod":
                        # True if i['FIsKFPeriod'] == "" else
                            exchangeBooleanValue(i['FIsKFPeriod']),
                        "FIsExpParToFlot": False,
                        "FExpPeriod": 1095,
                        "FCurrencyId": {
                            "FNumber": "PRE001"
                            # if i['FTRADINGCURRNO'] == '' else  get_FCurrencyNo(app3,i['FTRADINGCURRNO'])
                        },
                        "FIsEnableMinStock": False,
                        "FIsEnableMaxStock": False,
                        "FIsEnableSafeStock": False,
                        "FIsEnableReOrder": False,
                        "FSafeStock": i['FSafeStock'],
                        "FIsSNManage": False,
                        "FIsSNPRDTracy": False,
                        "FSNManageType": "1",
                        "FSNGenerateTime": "1"
                    },
                    "SubHeadEntity2": {
                        "FSaleUnitId": {
                            "FNumber": unit
                        },
                        "FSalePriceUnitId": {
                            "FNumber": unit
                        },
                        "FMaxQty":
                        # 100000.0 if i['FMaxPOQty'] == '' else
                            i['FMaxPOQty'],
                        "FIsATPCheck": False,
                        "FIsReturnPart": False,
                        "FIsInvoice": False,
                        "FIsReturn": True,
                        "FAllowPublish": False,
                        "FISAFTERSALE": True,
                        "FISPRODUCTFILES": True,
                        "FISWARRANTED": False,
                        "FWARRANTYUNITID": "D",
                        "FOutLmtUnit": "SAL",
                        "FIsTaxEnjoy": False,
                        "FUnValidateExpQty": False
                    },
                    "SubHeadEntity3": {
                        "FPurchaseUnitId": {
                            "FNumber": unit
                        },
                        "FPurchasePriceUnitId": {
                            "FNumber": unit
                        },
                        "FIsQuota": False,
                        "FQuotaType": "1",
                        "FIsVmiBusiness": False,
                        "FEnableSL": False,
                        "FIsPR": False,
                        "FIsReturnMaterial": True,
                        "FIsSourceControl": False,
                        "FPOBillTypeId": {
                            "FNUMBER": "CGSQD01_SYS"
                        },
                        "FPrintCount": 1,
                        "FMinPackCount":
                        # 1.0 if i['FMinPackCount'] == '' else
                            i['FMinPackCount']
                    },
                    "SubHeadEntity4": {
                        "FPlanningStrategy":
                        # "1" if i['FPlanningStrategy'] == '' else
                            i['FPlanningStrategy'],
                        "FMfgPolicyId": {
                            "FNumber": "ZZCL001_SYS"
                        },
                        "FFixLeadTime": i['FFixLeadTime'],
                        "FFixLeadTimeType":
                        # "1" if i['FFixLeadTimeType'] == '' else
                            exchangeDateCode(i['FFixLeadTimeType']),
                        "FVarLeadTime": i['FVarLeadTime'],
                        "FVarLeadTimeType":
                        # "1" if i['FVarLeadTimeType'] == '' else
                            exchangeDateCode(i['FVarLeadTimeType']),
                        "FCheckLeadTime": i['FOrderIntervalTime'],
                        "FCheckLeadTimeType":
                        # "1" if i['FOrderIntervalTimeType'] == '' else
                            exchangeDateCode(i['FOrderIntervalTimeType']),
                        "FOrderIntervalTimeType": "3",
                        "FMaxPOQty":
                        # 100000.0 if i['FMaxPOQty'] == '' else
                            i['FMaxPOQty'],
                        "FMinPOQty":
                        # 0.0 if i['FMinPOQty'] == '' else
                            i['FMinPOQty'],
                        "FEOQ":
                            1.0 if i['FEOQ'] == '' else i['FEOQ'],
                        "FVarLeadTimeLotSize":
                            1.0 if i['FVarLeadTimeLotSize'] == '' else i['FVarLeadTimeLotSize'],
                        "FIsMrpComBill": True,
                        "FIsMrpComReq": False,
                        "FReserveType": "1",
                        "FAllowPartAhead": False,
                        "FCanDelayDays": 999,
                        "FAllowPartDelay": True,
                        "FPlanOffsetTimeType": "1",
                        "FWriteOffQty": 1.0
                    },
                    "SubHeadEntity5": {
                        "FProduceUnitId": {
                            "FNumber": unit
                        },
                        "FFinishReceiptOverRate":
                        # 0.0 if i['FFinishReceiptOverRate'] == "" else
                            i['FFinishReceiptOverRate'],
                        "FFinishReceiptShortRate":
                        # 0.0 if i['FFinishReceiptShortRate'] == "" else
                            i['FFinishReceiptShortRate'],
                        "FProduceBillType": {
                            "FNUMBER": "SCDD01_SYS"
                        },
                        "FIsSNCarryToParent": False,
                        "FIsProductLine": False,
                        "FBOMUnitId": {
                            "FNumber": unit
                        },
                        "FIsMainPrd": True,
                        "FIsCoby": False,
                        "FIsECN": False,
                        "FIssueType": "1",
                        "FOverControlMode":
                        # "1" if i['FOverControlMode'] == "" else
                            i['FOverControlMode'],
                        "FMinIssueQty":
                            1.0 if i['FMinIssueQty'] == "" else i['FMinIssueQty'],
                        "FISMinIssueQty":
                        # False if i['FISMinIssueQty'] == '' else
                            exchangeBooleanValue(i['FISMinIssueQty']),
                        "FIsKitting": False,
                        "FIsCompleteSet": False,
                        "FMinIssueUnitId": {
                            "FNUMBER": unit
                        },
                        "FStandHourUnitId": "3600",
                        "FBackFlushType": "1",
                        "FIsEnableSchedule": False
                    },
                    "SubHeadEntity7": {
                        "FSubconUnitId": {
                            "FNumber": unit
                        },
                        "FSubconPriceUnitId": {
                            "FNumber": unit
                        }
                    },
                    "SubHeadEntity6": {
                        "FCheckIncoming": False,
                        "FCheckProduct": True,
                        "FCheckStock": False,
                        "FCheckReturn": False,
                        "FCheckDelivery": False,
                        "FEnableCyclistQCSTK": False,
                        "FEnableCyclistQCSTKEW": False,
                        "FCheckEntrusted": False,
                        "FCheckOther": False,
                        "FIsFirstInspect": False,
                        "FCheckReturnMtrl": False
                    },
                    "FEntityInvPty": [
                        {
                            "FInvPtyId": {
                                "FNumber": "01"
                            },
                            "FIsEnable": True,
                            "FIsAffectPrice": False,
                            "FIsAffectPlan": False,
                            "FIsAffectCost": False
                        },
                        {
                            "FInvPtyId": {
                                "FNumber": "02"
                            },
                            "FIsEnable": True,
                            "FIsAffectPrice": False,
                            "FIsAffectPlan": False,
                            "FIsAffectCost": False
                        },
                        {
                            "FInvPtyId": {
                                "FNumber": "03"
                            },
                            "FIsEnable": False,
                            "FIsAffectPrice": False,
                            "FIsAffectPlan": False,
                            "FIsAffectCost": False
                        },
                        {
                            "FInvPtyId": {
                                "FNumber": "04"
                            },
                            "FIsEnable":
                            # False if i['FIsBatchManage'] == "" else
                                exchangeBooleanValue(i['FIsBatchManage']),
                            "FIsAffectPrice": False,
                            "FIsAffectPlan": False,
                            "FIsAffectCost": False
                        },
                        {
                            "FInvPtyId": {
                                "FNumber": "06"
                            },
                            "FIsEnable": False,
                            "FIsAffectPrice": False,
                            "FIsAffectPlan": False,
                            "FIsAffectCost": False
                        }
                    ]
                }
            }

            savedResultInformation = api_sdk.Save("BD_MATERIAL", model)
            sri = json.loads(savedResultInformation)

            if sri['Result']['ResponseStatus']['IsSuccess']:

                submittedResultInformation = erp_submit(api_sdk,
                                                        sri['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'])

                subri = json.loads(submittedResultInformation)

                if subri['Result']['ResponseStatus']['IsSuccess']:

                    auditResultInformation = erp_audit("BD_MATERIAL", api_sdk,
                                                       subri['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'])

                    auditres = json.loads(auditResultInformation)

                    if auditres['Result']['ResponseStatus']['IsSuccess']:

                        result = erp_allocation('BD_MATERIAL', getCodeByView('BD_MATERIAL',
                                                                             auditres['Result']['ResponseStatus'][
                                                                                 'SuccessEntitys'][0]['Number'],
                                                                             api_sdk),
                                                getOrganizationCode(app2, i['FApplyOrgName']), api_sdk)

                        erp_submit(api_sdk, auditres['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'])
                        erp_audit('BD_MATERIAL', api_sdk,
                                  auditres['Result']['ResponseStatus']['SuccessEntitys'][0]['Number'])

                        changeStatus(app3, "1", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
                        changeStatus(app3, "1", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])

                        insert_log2(app3, "数据同步成功", i['FNumber'])



                    else:
                        insert_log(app3, auditres, i['FNumber'])
                        changeStatus(app3, "2", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
                        changeStatus(app3, "2", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])
                else:
                    insert_log(app3, subri, i['FNumber'])
                    changeStatus(app3, "2", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
                    changeStatus(app2, "2", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])
            else:
                insert_log(app3, sri, i['FNumber'])
                changeStatus(app3, "2", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
                changeStatus(app3, "2", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])
                print(sri)
    else:
        changeStatus(app3, "2", "RDS_ECS_ODS_bd_MaterialDetail", "FNumber", i['FNumber'])
        changeStatus(app3, "2", "RDS_ECS_SRC_bd_MaterialDetail", "FNumber", i['FNumber'])


# 编码入口，app3中台，app2金蝶
def performFNumber(app2,app3,option1,codeList):

    if type(codeList) == list:

        ecsToDmsByFNumber(app3, codeList)

    else:

        print("codeList不是一个列表")

    api_sdk = K3CloudApiSdk()

    dmsToErp(app2=app2, app3=app3, option=option1, api_sdk=api_sdk)

    print("运行结束")


def byFNumber_sync(app2,app3,option,codeList):

    if type(codeList) == list:

        ecsToDmsByFNumber(app3, codeList)

    else:

        print("codeList不是一个列表")

    odsResult = getOdsDataByFNumber(app3=app3,FNumber=codeList[0])

    api_sdk = K3CloudApiSdk()

    if odsResult:

        erp_save(app2=app2, app3=app3, option=option, api_sdk=api_sdk, data=odsResult)

        pass

    else:

        return "没有数据需要同步"


def getOdsDataByFNumber(app3,FNumber):
    '''
    通过单据编号获得数据源
    :param app3:
    :return:
    '''

    sql = f"""select FDeptId,FUserId,FApplyOrgName,FVarDateTime,FNumber,FName,FSpecification,
            FDescription,FTaxRateId,FGROSSWEIGHT,FNETWEIGHT,FLENGTH,FWIDTH,FHEIGHT,FVOLUME,FSafeStock,FMinPackCount,
            FPlanningStrategy,FOrderPolicy,FFixLeadTime,FFixLeadTimeType,FVarLeadTime,FVarLeadTimeType,FOrderIntervalTimeType,
            FOrderIntervalTime,FMaxPOQty,FMinPOQty,FIncreaseQty,FEOQ,FVarLeadTimeLotSize,FFinishReceiptOverRate,
            FFinishReceiptShortRate,FMinIssueQty,F_SZSP_CheckBox,FISMinIssueQty,FIsBatchManage,FOverControlMode,FIsKFPeriod,
            FCheckOut,FMaterialGroup,FErpClsID,FCategoryID,FTaxCategoryCodeId,FWEIGHTUNITID,FBaseUnitId,FVOLUMEUNITID,
            FBatchRuleID,F_SZSP_Assistant,FUploadDate,Fisdo,FChecker,FOldNumber,FIsPurchase,F_SZSP_Decimal,F_SZSP_Decimal1,
            F_SZSP_SKUnumber,F_SZSP_PackCoefficient,FExpUnit,FExpPeriod,FIsEnableSafeStock,FIsEnableMinStock,FMinStock,
            FIsEnableMaxStock,FMaxStock,F_SZSP_Itemnumber,FBaseProperty,FSupplierMaterialCode,FMaterialGroupName,FMaterialGroupCode,FParentMaterialGroupCode,FParentMaterialGroupName from RDS_ECS_ODS_bd_MaterialDetail where FNumber='{FNumber}'"""

    res = app3.select(sql)

    return res



def performFNumber_bydate(app2,app3,option1,FDate):

    ecsToDmsByFDate(app3, FDate)

    api_sdk = K3CloudApiSdk()

    dmsToErp(app2=app2, app3=app3, option=option1, api_sdk=api_sdk)

    print("运行结束")







