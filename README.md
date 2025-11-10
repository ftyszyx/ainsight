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

## 第二步：数据管道搭建计划
- 环境准备：部署 Airflow（PostgreSQL 元数据库+Celery Executor）、配置阿里云 OSS Bucket、创建 Git 仓库与基础代码骨架、在 PostgreSQL 启用 TimescaleDB 与扩展（pg_trgm、pgvector、zhparser）
- 调度编排：设计 DAG `daily_price_ingest`（收盘后拉取日行情并校验写库）、`minute_price_ingest`（交易时段聚合分钟数据）、`financial_report_sync`（解析财报公告）、`research_report_ingest`（研报下载+OCR+摘要队列）、`news_ingest`（新闻拉取+情绪打分）、`feature_calculation`（行情与财务特征生成）、`quality_monitor`（缺失率与延迟告警）
- 采集实现：封装数据源适配器支持 Tushare Pro 与聚宽接口限频控制，研报与新闻接口统一落地 OSS，并记录证券代码、来源、拉取时间等元信息；Airflow 任务失败自动重试并写日志
- 数据建模：在 PostgreSQL 建表 `company_master`、`daily_prices`（Timescale hypertable）、`minute_prices`、`financial_metrics`、`reports`、`news`、`feature_snapshots`，定义主键与索引策略，编写数据字典
- NLP 与异步处理：Airflow 触发 Celery 任务进行文本分词、情绪分类、摘要生成、向量化，结果回写 `reports`、`news`、`text_embeddings` 表并持久化错误记录
- 监控与日志：接入 Prometheus 导出 Airflow DAG 指标、任务耗时、失败次数，配置报警渠道；集中存储任务日志便于追溯
- 里程碑：第1周完成环境搭建与 `daily_price_ingest` 骨架，第2周接入财报与研报采集并跑通 OSS 落地，第3周上线文本处理与特征计算任务，第4周完成数据质量仪表盘与首版管道验收

### 第二步实施指引
- 环境搭建
  1. 使用 uv 创建虚拟环境 `uv venv` 并激活 `source .venv/bin/activate`，
  1. uv add "apache-airflow[celery]==3.0.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.10.txt"
  1. uv add psycopg2-binary oss2 tushare pandas sqlalchemy jieba httpx pgvector

psycopg2-binary：PostgreSQL 驱动，Airflow 任务用它把行情、财报等数据写入数据库。
oss2：阿里云 OSS SDK，用于把原始行情/研报文件上传到对象存储并下载回查。
tushare：Tushare Pro 接口库，负责从数据源拉取沪深 A 股行情、财务等数据。
pandas：数据处理基础库，清洗与转换接口返回的数据后再入库。
sqlalchemy：数据库 ORM/引擎层，配合 pandas/自定义代码写入 PostgreSQL、管理连接。
jieba：中文分词库，可用于关键词抽取或与 LLM 输出结合生成结构化标签。
httpx：异步/同步 HTTP 客户端，调用 DeepSeek 等大模型推理 API。
pgvector：PostgreSQL 扩展的 Python 客户端，配合数据库中的 pgvector 扩展存储文本向量，用于语义检索和相似度查询。

Airflow 不支持windows

  2. 初始化 Airflow `export AIRFLOW_HOME=~/airflow && airflow db init`，创建管理员 `airflow users create --username admin --password ****** --role Admin --email admin@example.com`
  3. 配置 Celery Executor（Redis 或 RabbitMQ），在 `airflow.cfg` 设置 `executor = CeleryExecutor` 并写入连接串
  4. PostgreSQL 启用扩展
```
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgvector;
CREATE EXTENSION IF NOT EXISTS zhparser;
```
  5. 阿里云 OSS 创建 Bucket `ainsight`, 设置跨域访问与生命周期策略，准备 `OSS_ENDPOINT`, `OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`
- 核心表结构
```
CREATE TABLE company_master (
  security_id VARCHAR(12) PRIMARY KEY,
  exchange VARCHAR(10),
  symbol VARCHAR(10),
  name_zh TEXT,
  industry_lv1 TEXT,
  industry_lv2 TEXT,
  list_date DATE,
  status VARCHAR(10),
  updated_at TIMESTAMP DEFAULT NOW()
);
SELECT create_hypertable('daily_prices','trade_date',chunk_time_interval => INTERVAL '7 days',if_not_exists => TRUE);
CREATE TABLE daily_prices (
  security_id VARCHAR(12),
  trade_date DATE,
  open NUMERIC(18,4),
  close NUMERIC(18,4),
  high NUMERIC(18,4),
  low NUMERIC(18,4),
  volume NUMERIC(20,2),
  amount NUMERIC(20,2),
  adj_close NUMERIC(18,4),
  PRIMARY KEY(security_id,trade_date)
);
CREATE TABLE reports (
  id BIGSERIAL PRIMARY KEY,
  security_id VARCHAR(12),
  publish_time TIMESTAMP,
  source VARCHAR(64),
  title TEXT,
  summary TEXT,
  sentiment NUMERIC(6,4),
  risk_tags TEXT[],
  oss_path TEXT
);
```
- Airflow DAG 示意
```
with DAG('daily_price_ingest',schedule='0 15 * * 1-5',start_date=datetime(2024,1,1),catchup=False) as dag:
    fetch = PythonOperator(task_id='fetch_raw',python_callable=fetch_daily_prices,op_kwargs={'trade_date':'{{ ds }}'})
    upload = PythonOperator(task_id='upload_to_oss',python_callable=upload_raw_to_oss)
    load = PythonOperator(task_id='load_postgres',python_callable=load_daily_prices)
    quality = PythonOperator(task_id='quality_check',python_callable=run_price_quality_checks)
    fetch >> upload >> load >> quality
```
- 数据源适配器示例
```
class TushareClient:
    def __init__(self, token:str):
        self.pro = ts.pro_api(token)
    def daily(self,date:str,limit:int=4000)->pd.DataFrame:
        return self.pro.daily(trade_date=date,fields='ts_code,trade_date,open,high,low,close,vol,amount')
def fetch_daily_prices(trade_date:str):
    df = TushareClient(os.environ['TUSHARE_TOKEN']).daily(trade_date)
    df['ts_code'] = df['ts_code'].str.replace('.SZ','SZ').str.replace('.SH','SH')
    df.to_csv(local_path,index=False)
```
- OSS 落地与入库
```
def upload_raw_to_oss(**context):
    oss.put_object_from_file(f"raw/daily_price/{context['ds']}/{filename}",local_path)
def load_daily_prices(**context):
    engine = create_engine(os.environ['DATABASE_URL'])
    df = pd.read_csv(local_path)
    df.rename(columns={'ts_code':'security_id','vol':'volume'},inplace=True)
    df.to_sql('daily_prices',engine,if_exists='append',index=False,method='multi')
```
- 数据质量校验
```
def run_price_quality_checks(**context):
    sql = "SELECT COUNT(*) FROM daily_prices WHERE trade_date=%s AND (high<low OR close>high*1.2)"
    issues = execute_sql(sql,[context['ds']])
    if issues>0:
        send_alert(f"Daily price anomaly {context['ds']} count={issues}")
        raise AirflowFailException("Quality check failed")
```
- 文本处理任务
```
def process_report(report_id:int):
    text = load_from_oss(report_id)
    prompt = [
        {"role":"system","content":"你是证券研报分析助手，请生成300字以内摘要并给出情绪评分[-1,1]以及风险要点列表。"},
        {"role":"user","content":text}
    ]
    resp = httpx.post(
        os.environ["LLM_ENDPOINT"],
        headers={"Authorization":f"Bearer {os.environ['LLM_API_KEY']}","Content-Type":"application/json"},
        json={"model":"deepseek-chat","messages":prompt,"temperature":0.2},
        timeout=60
    )
    result = resp.json()["choices"][0]["message"]["content"]
    summary, sentiment, risk_tags = parse_llm_result(result)
    keywords = jieba.analyse.extract_tags(text,topK=10)
    upsert_report(report_id,summary,sentiment,risk_tags,keywords)
```
- 监控部署
  1. 安装 `pip install apache-airflow-providers-prometheus` 并开启 `StatsD`，Prometheus 抓取 `statsd_exporter`
  2. 配置报警：Airflow EmailOperator 或企业微信机器人，在 `quality_monitor` DAG 里发送告警
  3. 建立数据质量看板：Grafana 展示任务成功率、数据缺失率、延迟
- 里程碑验收标准
  1. `daily_price_ingest` 可稳定写入 PostgreSQL，OSS 保留原始文件
  2. 财报、研报、新闻流水线跑通且文本处理结果可查询
  3. 特征快照生成表 `feature_snapshots` 每日刷新
  4. Prometheus 报表显示任务指标，无重大告警