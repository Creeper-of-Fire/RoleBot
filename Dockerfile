# 使用官方Python运行时作为基础镜像
FROM python:3.12-slim-bookworm

ENV TZ=Asia/Shanghai

# 设置工作目录
WORKDIR /app

# 复制项目的依赖文件到容器中
# 如果你使用 requirements.txt 管理依赖
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制所有项目文件到容器中
# 确保你的主应用文件（例如 main.py 或 main.py）也在复制范围内
COPY . .

# 暴露你的应用可能使用的端口 (如果你的Bot有Web界面或API)
# 对于Discord Bot，通常不需要暴露端口，除非你有额外的Web服务器
# EXPOSE 8000

ENV PYTHONPATH=/app

# 定义容器启动时执行的命令
# 假设你的Bot入口文件是 main.py
CMD ["python", "main.py"]