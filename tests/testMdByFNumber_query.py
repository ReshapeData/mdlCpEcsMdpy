#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mdlCpEcsMdpy

import mdlCpEcsMdpy

# 按单号查询

# res=mdlCpEcsMdpy.materialByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="K86-10015S-H")
#
# print(res)

# res=mdlCpEcsMdpy.customerByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="C15276")
#
# print(res)
#
res=mdlCpEcsMdpy.supplierByFNumber_query(token="9B6F803F-9D37-41A2-BDA0-70A7179AF0F3",FNumber="S00030")

print(res)