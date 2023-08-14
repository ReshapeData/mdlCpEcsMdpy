#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mdlCpEcsMdpy

# 按单号查询日志

res=mdlCpEcsMdpy.materialLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="K86-10015S-H")

print(res)

res=mdlCpEcsMdpy.customerLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="C15276")

print(res)

res=mdlCpEcsMdpy.supplierLog_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="S00085")

print(res)