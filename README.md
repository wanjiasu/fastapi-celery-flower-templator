# FastAPI Celery Flower Templator

这是一个基于 **FastAPI**、**Celery**、**Redis**、**PostgreSQL** 和 **Flower** 的异步任务处理模板项目。

## 🚀 项目简介

本项目提供了一个完整的异步任务架构模板，适用于需要处理耗时任务（如数据抓取、大规模计算、邮件发送等）的 Web 应用。它集成了以下功能：

- **FastAPI**: 高性能的异步 Web 框架，负责 API 接口。
- **Celery**: 强大的分布式任务队列，处理后台异步任务。
- **Redis**: 作为 Celery 的消息代理（Broker）和后端（Backend）。
- **PostgreSQL**: 用于持久化存储任务结果和其他业务数据（如股票数据）。
- **Flower**: Celery 的实时监控工具，提供 Web 界面查看任务状态。
- **Docker**: 支持容器化部署，方便环境迁移。

## 🛠️ 技术栈

- **Language**: Python 3.10+
- **Web Framework**: FastAPI
- **Task Queue**: Celery
- **Message Broker/Backend**: Redis
- **Database**: PostgreSQL (psycopg2)
- **Monitoring**: Flower
- **Package Manager**: [uv](https://github.com/astral-sh/uv)
- **Deployment**: Docker & Docker Compose

## 📂 项目结构

```text
.
├── app/
│   ├── stock_data_fetcher/  # 股票数据抓取模块
│   │   ├── get_stock_list.py
│   │   └── utils.py
│   ├── celery_app.py        # Celery 实例配置
│   ├── db.py                # 数据库初始化及操作
│   ├── main.py              # FastAPI 主入口
│   ├── settings.py          # 配置文件 (Pydantic Settings)
│   └── tasks.py             # Celery 任务定义
├── Dockerfile               # Docker 镜像构建文件
├── docker-compose.yml       # 多容器启动配置
├── env.example              # 环境变量模板
├── pyproject.toml           # 项目依赖配置
└── uv.lock                  # uv 锁定文件
```

## ⚙️ 快速开始

### 1. 克隆项目并配置环境

```bash
cp env.example .env
```

请根据实际情况修改 `.env` 文件中的 `REDIS_URL` 和 `POSTGRES_DSN`。**注意：默认配置假设 Redis 和 PostgreSQL 在外部服务器运行。**

### 2. 使用 Docker Compose 启动

```bash
docker-compose up --build
```

启动后：
- **API 服务**: [http://localhost:8000](http://localhost:8000)
- **Flower 监控**: [http://localhost:5555](http://localhost:5555)
- **API 文档 (Swagger)**: [http://localhost:8000/docs](http://localhost:8000/docs)

### 3. 本地运行 (开发环境)

如果你想在本地开发而不使用 Docker，可以使用 `uv`:

```bash
# 安装依赖
uv sync

# 启动 FastAPI
uv run uvicorn app.main:app --reload

# 启动 Celery Worker
uv run celery -A app.celery_app.celery worker -l info

# 启动 Flower
uv run celery -A app.celery_app.celery flower --port=5555
```

## 🛣️ API 接口说明

- **GET `/health`**: 健康检查。
- **POST `/tasks/add`**: 触发一个简单的加法异步任务。
    - 参数: `x`, `y` (int)
- **GET `/tasks/{task_id}`**: 从 PostgreSQL 获取指定任务的运行结果。
- **POST `/stocks/sync`**: 同步股票基础信息到数据库。
    - 参数: `exchange` (交易所), `list_status` (上市状态)

## 📝 核心流程说明

1. 用户调用 API (`/tasks/add`)。
2. FastAPI 将任务分发给 Celery (`add.delay(x, y)`)。
3. Celery Worker 从 Redis 获取任务并执行。
4. 任务执行完成后，结果会自动保存到 PostgreSQL (`save_result`)。
5. 用户可以通过 `/tasks/{task_id}` 查询数据库中的结果。
