#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mdlCpEcsMdpy

# res=mdlCpEcsMdpy.FBillNo_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="物料",FNumber="NZ0101")

# res=mdlCpEcsMdpy.FBillStatus_upload(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="物料",FNumber="NZ0101")

# res=mdlCpEcsMdpy.SRCTable_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="物料",FNumber="NZ0101")

# res=mdlCpEcsMdpy.log_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="NZ0101")
#
# print(res)

# # res=mdlCpEcsMdpy.FBillNo_sync_byDate(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="供应商",FDate="2023-07-11")
# #
# # print(res)
#
# l=['四川省机械进出口有限公司',
# '苏州赛福医学检验有限公司',
# '广西南宁市天地扬生物科技有限公司',
# '山东海笑天生物科技有限公司',
# '浴室',
# '江苏冠纶医疗器械有限公司',
# '山东千舒达医疗科技有限公司',
# 'american-european products co.,ltd',
# 'pallaslash',
# '北京沃比森科技有限公司',
# '凌美智能生物科技（杭州）有限公司',
# 'Zedan Hassan',
# 'Lisa Reese-Doran',
# '福州彭瀚贸易有限公司',
# 'ZERBO Hamidou',
# 'Mariana Gonzalez',
# 'Fermify',
# 'Tchimino',
# 'DVL',
# 'BSD Lab',
# 'RMV Protection Pty Ltd',
# 'So Khetfc',
# 'Cahaya Rejeki Seng PT.',
# 'Felipe PonceHerrera',
# 'basheer mateen',
# 'Миронов Александр',
# 'TRIPP LLC',
# 'Dan Schildgen',
# 'Ali Nur Abdirahim',
# 'Model trading',
# 'Warren F',
# 'Oyindamola Helen',
# 'Max Ziegler',
# '南京安杰优生物科技有限公司',
# '浙江凯壹仪器仪表有限公司',
# '江苏派沃精准医学科技有限公司',
# '深圳汇芯生物医疗科技有限公司',
# '江苏赛尔普生物科技有限公司',
# '杭州皓才生物科技有限公司',
# 'Zedan Hassan'
# ]
#
# for i in l:
#
#     res=mdlCpEcsMdpy.FBillNo_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="客户",FNumber="i")
#
#     print(res)


res=mdlCpEcsMdpy.FBillNo_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="客户",FNumber="凌美智能生物科技（杭州）有限公司")

print(res)
# 四川省机械进出口有限公司





