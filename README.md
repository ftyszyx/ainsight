
1. 新债上市有些机会 
2. 可转债配股

我想做一个工具，用ai帮分析a股的全部上市公司
通过每天的股票行情和和历史行情。
通过分析公司的研报和互联网的新闻。
找到有价值的好公司，找到好的投资机会，提供给用户。
需要有一个后台用于分析数据
一个前端网页，用来和用户交互和展示给用户数据

## 约束
- 对象存储使用阿里云OSS
- 数据库使用PostgreSQL，允许安装TimescaleDB插件
- 前期不引入ClickHouse与Elasticsearch，系统结构保持简单

## 目标拆解
1. 全量采集沪深上市公司行情、财务、研报、新闻等数据
2. 将结构化与非结构化信息融合，输出可解释的投资机会与风险提示
3. 为用户提供可视化界面与报告，支持检索、筛选、提醒

## 第一步：需求与合规调研
- 明确角色与权限：投资分析师、普通用户、运营、管理员；需要账号管理与操作日志
- 覆盖数据范围：沪深全量公司，日/分钟行情、财报、公告、融资融券、研报、新闻、产业链信息
- 候选数据源：Tushare Pro、聚宽、巨潮资讯、新华社、证券时报、东方财富；评估授权条款与费用
- 合规要求：核对数据使用许可，准备用户协议与隐私政策，满足《数据安全法》《网络安全法》与金融信息服务监管
- MVP 边界：提供日行情+关键财务+研报新闻摘要、多因子评分、风险提示、基本筛选与公司画像
- SLA 指标：日行情 T+0 收盘5分钟内写库，财报 T+1，研报/新闻 30 分钟内；平台可用性目标 99.5%
- 风险清单：数据授权不完整、研报版权、模型输出误导、延迟或缺失、LLM 错误
- 下一步行动：与数据供应商接洽签约、编制数据字典、撰写产品需求文档与流程原型、制定合规方案并对接法务

## 第二步：数据采集脚本与流程
- 环境准备：使用 uv 创建虚拟环境 `uv venv` 并激活 `source .venv/bin/activate`，执行 `uv add psycopg2-binary tushare pandas sqlalchemy jieba httpx pgvector python-dotenv`
- 配置管理：在项目根目录创建 `.env`，写入 `DATABASE_URL`、`TUSHARE_TOKEN`、`LLM_ENDPOINT`、`LLM_API_KEY` 等变量；脚本通过 `python-dotenv` 自动加载
- 数据访问封装：在 `market/services/` 目录编写模块 `db.py`（返回 SQLAlchemy Engine/Session）、`tushare_client.py`、`storage.py`，统一处理重试、限频、日志；前期文件直接存本地 `data/raw/{date}/` 目录
- 脚本清单：在 `market/jobs/` 下新增可手动运行的脚本
  - `fetch_daily.py`：拉取指定日期行情 → 保存 CSV → 写入 `daily_prices`
  - `sync_financials.py`：同步财报与关键指标 → 写入 `financial_metrics`
  - `sync_reports.py`：下载研报/新闻文件 → 保存到本地 → 调用 LLM 生成摘要、情绪、风险标签 → 更新 `reports`、`news`
  - `calc_features.py`：汇总行情+财务+情绪 → 写入 `feature_snapshots`
- 手动执行方式：在需要时直接运行 `python -m market.jobs.fetch_daily --date 2024-09-10`，各脚本统一支持参数（日期、证券代码、是否覆盖等），日志写入 `logs/{job}.log`
- 数据校验：每个脚本最后执行校验函数（例如检查缺失值、涨跌幅异常、研报解析是否成功）；若失败打印原因并 `sys.exit(1)`，方便人工复核
- 结果记录：创建 `docs/run-log.md`，记录每次手动执行时间、脚本、结果、备注，为后续自动化留档
- 后续迭代：当脚本数量和依赖变复杂时，可再引入定时/并发调度（如 cron、Prefect、Dagster），现阶段保持流程可控、易调试；后期如需云端共享，再迁移到 OSS 或其他对象存储