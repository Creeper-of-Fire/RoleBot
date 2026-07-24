#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RoleBot 远程部署 wrapper（容器内执行）。

由 deploy.py 通过 `docker compose run ... python3 /app/remote_deploy.py` 调用。
实际逻辑在 `shared.scripts.remote_deploy`。
"""
from shared.scripts.remote_deploy import main

main()