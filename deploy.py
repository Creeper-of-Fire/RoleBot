#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RoleBot 一键部署 wrapper。

实际部署逻辑在 `shared.scripts.deploy`；本文件不传参数（默认 run_remote_deploy=True）。

RoleBot 有 alembic 配置，remote_deploy 步骤会跑迁移。
"""
import sys

from shared.scripts.deploy import main as _main

if __name__ == "__main__":
    sys.exit(_main())