#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mdlCpEcsMdpy

# 按单号同步

res=mdlCpEcsMdpy.materialByFNumber_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="P8240-250g")

print(res)

res=mdlCpEcsMdpy.materialByFNumber_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="")

print(res)

res=mdlCpEcsMdpy.customerByFNumber_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="C13434")

print(res)

res=mdlCpEcsMdpy.customerByFNumber_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="")

print(res)


res=mdlCpEcsMdpy.supplierByFNumber_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="S00091")

print(res)

res=mdlCpEcsMdpy.supplierByFNumber_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="")

print(res)