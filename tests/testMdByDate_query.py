#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mdlCpEcsMdpy

# 按日期查找同步

res=mdlCpEcsMdpy.materialByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="2023-07-06")

print(res)

res=mdlCpEcsMdpy.customerByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="2023-07-03")

print(res)

res=mdlCpEcsMdpy.supplierByDate_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FDate="2023-07-04")

print(res)