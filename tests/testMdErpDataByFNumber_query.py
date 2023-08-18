#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mdlCpEcsMdpy

# 按单号查询Erp

res=mdlCpEcsMdpy.materialErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="1.1.1.01.01.001.000007")

print(res)

res=mdlCpEcsMdpy.materialErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="")

print(res)

res=mdlCpEcsMdpy.customerErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="C000186")

print(res)

res=mdlCpEcsMdpy.customerErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="")

print(res)


res=mdlCpEcsMdpy.supplierErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="S001162")

print(res)



res=mdlCpEcsMdpy.supplierErpDataByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="")

print(res)