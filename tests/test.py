#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mdlCpEcsMdpy

# res=mdlCpEcsMdpy.FBillNo_sync(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="物料",FNumber="NZ0101")

# res=mdlCpEcsMdpy.FBillStatus_upload(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="物料",FNumber="NZ0101")

# res=mdlCpEcsMdpy.SRCTable_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="物料",FNumber="NZ0101")

# res=mdlCpEcsMdpy.log_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="NZ0101")
#
# print(res)

res=mdlCpEcsMdpy.FBillNo_sync_byDate(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",category="供应商",FDate="2023-07-11")

print(res)



