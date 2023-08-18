#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mdlCpEcsMdpy
# 按日期同步
# res=mdlCpEcsMdpy.materialByDate_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate='2023-07-06')
#
# print(res)

# res=mdlCpEcsMdpy.materialByDate_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate='')
#
# print(res)

res=mdlCpEcsMdpy.customerByDate_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="2022-12-01")

print(res)

res=mdlCpEcsMdpy.customerByDate_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="")

print(res)


# res=mdlCpEcsMdpy.supplierByDate_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="2023-02-06")
#
# print(res)
#
#
# res=mdlCpEcsMdpy.supplierByDate_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="")
#
# print(res)