# 裁判文书角色提取系统 v6.0

从中国裁判文书 CSV 数据中自动提取角色签名信息（审判长、审判员、书记员等），输出为结构化宽表。

采用 **规则提取 + AI精修** 两阶段混合方案：Phase 1 用规则提取高确定性姓名（~92%），Phase 2 将不确定片段交给大模型提取。

---

## 目录

- [业务逻辑](#业务逻辑)
- [项目结构](#项目结构)
- [安装依赖](#安装依赖)
- [配置说明](#配置说明)
- [使用方法](#使用方法)
- [输出文件说明](#输出文件说明)
- [性能指标](#性能指标)
- [角色字典](#角色字典)
- [常见问题](#常见问题)

---

## 业务逻辑

### 总体流程

```
输入 CSV（含"全文"列）
        │
        ▼
  ┌─────────────────┐
  │  Phase 1: 规则提取 │  ← python run_pipeline.py phase1
  │  全文扫描角色关键词  │
  │  百家姓验证 + 边界判断│
  └────────┬────────┘
           │
     ┌─────┴──────┐
     │            │
  确定(~92%)   不确定(~8%)
  名字后是标点    名字后粘着中文字
  换行/日期      （如"审判员韦圆圆独任"）
     │            │
     ▼            ▼
  result.csv   ai_queue.csv
  来源=规则     状态=待处理
     │            │
     │      ┌─────┴──────┐
     │      │ Phase 2: AI │  ← python run_pipeline.py phase2
     │      │ 批量发给大模型 │
     │      │ 按ID反填结果  │
     │      └─────┬──────┘
     │            │
     ▼            ▼
     ┌────────────┐
     │ 合并到 result.csv │
     │ 来源=规则 / AI     │
     └────────────┘
```

### Phase 1 边界判断规则

名字后面的**第一个非空白字符**决定确定性：

| 边界字符 | 示例 | 判定 |
|----------|------|------|
| 换行 `\n` | `审判员王军\n` | ✅ 确定 |
| 标点符号 | `审判员王军，` | ✅ 确定 |
| 日期 | `审判员王军二〇一六年` | ✅ 确定 |
| 角色关键词 | `审判员王军书记员张三` | ✅ 确定 |
| 其他中文字 | `审判员王军独任审理本案` | ❓ 不确定 → AI |

### Phase 2 AI打包策略

- 按 **token 估算**打包（中文字×2 + 英文×0.5）
- 每请求最多 **500条** 或 **80K input token**
- 每个片段有唯一 `snippet_id`（如 `42_1580`），AI 返回 JSON 按 ID 反填
- 支持 **ThreadPoolExecutor** 并发（多个大请求时）

---

## 项目结构

```
Referee-documents/
├── config/
│   ├── config.json          # API 和处理参数（模型/超时/并发等）
│   └── roles.json           # 角色字典（24种角色 + 优先级）
├── src/
│   ├── run_pipeline.py      # 统一入口（phase1 / phase2 / retry / status）
│   ├── core/
│   │   ├── config.py        # 配置管理（读取 config.json）
│   │   ├── rule_extractor.py # 规则提取器 v5.0（全文扫描 + 边界判断）
│   │   ├── progress_db.py   # SQLite 断点续传
│   │   ├── utils.py         # 宽表输出 + 增量CSV写入器
│   │   └── __init__.py      # 模块导出
│   └── models/
│       └── person.py        # Person(name, role) 数据模型
├── data/output/              # 输出目录（自动创建）
├── input/                    # 放入待处理的 CSV 文件
├── requirements.txt          # Python 依赖
└── README.md
```

---

## 安装依赖

```bash
pip install pandas tqdm httpx
```

或者：

```bash
pip install -r requirements.txt
```

---

## 配置说明

### `config/config.json`

```json
{
  "api": {
    "base_url": "https://vectorengine.ai/v1",   // API 地址
    "api_key": "sk-xxx",                         // API Key
    "model": "glm-4",                            // 模型名称
    "timeout": 120,                              // 单次请求超时（秒）
    "max_retries": 3,                            // 失败重试次数
    "concurrency": 10                            // 并发线程数
  },
  "processing": {
    "chunk_size": 5000,                          // CSV 分块读取大小
    "ai_batch_size": 30,                         // （已废弃，现用token估算）
    "confidence_threshold": 0.5                  // （保留字段）
  }
}
```

### 如何更换模型

编辑 `config/config.json` 中的 `model` 字段：

| 模型 | 特点 | 输出上限 | 建议场景 |
|------|------|----------|----------|
| `glm-4` | 质量高，速度适中 | 16K token | ✅ 推荐，131条/请求 10秒 |
| `glm-4-flash` | 速度快，免费 | 4K token | 小批量测试 |
| `glm-4.5-flash` | 最新版 | 96K token | 如果代理支持 |

> **注意**：输出 token 上限决定每请求最多能塞多少条。`glm-4` 的 16K 输出≈640条/请求。

### 如何更换 API 地址

如果使用智谱官方 API：
```json
"base_url": "https://open.bigmodel.cn/api/paas/v4"
```

如果使用第三方中转站（如 vectorengine）：
```json
"base_url": "https://vectorengine.ai/v1"
```

> 不同中转站可能需要不同的 URL 格式，建议依次尝试：
> - `https://xxx.ai/v1`
> - `https://xxx.ai/v1/chat/completions`
> - `https://xxx.ai`

### 如何调整 AI 每请求条数

编辑 `src/run_pipeline.py` 中的常量（约第76行）：

```python
MAX_ITEMS_PER_REQUEST = 500   # 每请求最多条数（受模型输出token限制）
MAX_INPUT_TOKENS = 80000      # 每请求最大输入token
```

---

## 使用方法

### 阶段一：规则提取

```powershell
# 处理 CSV 文件（全量）
python src/run_pipeline.py phase1 --input "input/2016年裁判文书数据/2016年裁判文书数据_马克数据网/2016年01月裁判文书数据.csv"

# 限制行数（测试用）
python src/run_pipeline.py phase1 --input "input/.../xxx.csv" --limit 1000

# 指定输出目录
python src/run_pipeline.py phase1 --input "input/.../xxx.csv" --output-dir "data/output"
```

**输出**：
- `*_result.csv` — 确定提取结果（来源=规则）
- `*_ai_queue.csv` — 不确定片段（状态=待处理）
- `*_exceptions.csv` — 异常（空文本/无角色词）
- `progress.sqlite` — 断点续跑进度

### 阶段二：AI 精修

确认 Phase 1 结果后，手动执行：

```powershell
# 处理 ai_queue 中所有"待处理"的片段
python src/run_pipeline.py phase2 --output-dir "data/output"
```

**效果**：
- 读取 `*_ai_queue.csv` 中 `状态=待处理` 的条目
- 批量发送给 AI，按 `snippet_id` 反填人名
- 合并到 `*_result.csv`，标记 `来源=AI`
- 更新 `*_ai_queue.csv` 状态为 `已处理` 或 `失败`

### 重试失败项

```powershell
# 仅重试 ai_queue 中"失败"的片段
python src/run_pipeline.py retry --output-dir "data/output"
```

### 查看状态

```powershell
python src/run_pipeline.py status --output-dir "data/output"
```

输出示例：
```
  Phase 1 (规则提取):
    已完成      :       93
    待AI处理    :        0
    无角色词    :        3

  结果文件: xxx_result.csv (97 行)
        AI :       78
        规则 :       19

  AI队列: xxx_ai_queue.csv (131 条)
       已处理 :      131
```

---

## 断点续传

### Phase 1 断点续传

处理过程中可随时 `Ctrl+C` 中断，进度保存在 `progress.sqlite`。
再次运行**相同命令**，已处理的行自动跳过。

```powershell
# 第一次：跑10万条后中断
python src/run_pipeline.py phase1 --input "..." --limit 100000

# 第二次：自动从第100001条继续
python src/run_pipeline.py phase1 --input "..."
```

### Phase 2 断点续传

AI 队列的 `状态` 列记录了每条片段的处理状态：
- `待处理` — 尚未发给 AI
- `已处理` — AI 已返回结果（无论有无有效人名）
- `失败` — API 调用失败

重新运行 `phase2` 只会处理 `待处理` 的条目。运行 `retry` 只会处理 `失败` 的条目。

> **清空重来**：`Remove-Item -Recurse -Force data\output\*`

---

## 输出文件说明

### `*_result.csv` — 提取结果

| 文件 | 序号 | 案号 | 审判长 | 审判员 | 书记员 | ... | 来源 |
|------|------|------|--------|--------|--------|-----|------|
| 201601 | 1 | (2016)x民初字第1号 | 张三 | 李四;王五 | 陈七 | | 规则 |
| 201601 | 2 | (2016)x字第100号 | | 韦圆圆 | 赵六 | | AI |

- 同角色多人用 `;` 分隔
- `来源` 列：`规则`（Phase 1 确定提取）或 `AI`（Phase 2 AI 提取）
- 共 24 个角色列 + 文件/序号/案号/来源

### `*_ai_queue.csv` — AI 队列

| 文件 | 序号 | 案号 | 角色 | 片段 | 位置 | snippet_id | 状态 |
|------|------|------|------|------|------|------------|------|
| 201601 | 5 | ... | 审判员 | 审判员韦圆圆独任审理 | 1580 | 4_1580 | 已处理 |

- `snippet_id`：`行索引_位置`，用于 AI 反填
- `状态`：`待处理` / `已处理` / `失败`

### `*_exceptions.csv` — 异常文书

| 文件 | 序号 | 案号 | 标识 |
|------|------|------|------|
| 201601 | 10 | ... | 空文本 |
| 201601 | 25 | ... | 无角色词 |

---

## 性能指标

### Phase 1（规则提取）

| 指标 | 数值 |
|------|------|
| 确定提取率 | ~92% |
| 处理速度 | ~1,200 条/秒 |
| 内存占用 | ~200MB |
| 10万条耗时 | ~90秒 |

### Phase 2（AI 精修）

| 指标 | 数值（glm-4） |
|------|------|
| 131条不确定片段 | 1次API调用，10秒 |
| 万条不确定片段 | ~20次API调用，~2分钟 |
| API 费用 | 输入 ¥0.05/千token，输出 ¥0.05/千token |

---

## 角色字典

`config/roles.json` 定义了 24 种角色及优先级：

| 优先级 | 角色 |
|--------|------|
| 1 | 代理审判长、审判长 |
| 2 | 代理审判员、助理审判员 |
| 3 | 审判员、人民陪审员、合议庭成员、陪审员 |
| 4 | 代理书记员、代书记员、书记员 |
| 5 | 首席仲裁员、主审法官、执行法官、执行员 |
| 6 | 副主任、主任、副院长、院长、副庭长、庭长 |
| 7 | 仲裁员、法官 |
| 8 | 法警 |

- 优先级用于匹配冲突时的排序
- 长名优先匹配（如"人民陪审员"优先于"陪审员"）
- 可直接编辑 `roles.json` 新增角色

---

## 常见问题

### Q: 如何重头开始处理？

```powershell
Remove-Item -Recurse -Force data\output\*
```

### Q: API 一直超时怎么办？

1. 调大 `config.json` 中的 `timeout`（默认 120 秒）
2. 减小 `run_pipeline.py` 中的 `MAX_ITEMS_PER_REQUEST`（默认 500）
3. 检查网络代理是否正常

### Q: 如何只处理某个月的数据？

Phase 1 的 `--input` 参数指定具体 CSV 文件即可。输出文件名自动包含月份标识。

### Q: AI 提取的名字不准确怎么办？

1. 编辑 `run_pipeline.py` 中的 prompt 模板（约第115行）
2. 换用更好的模型（如 `glm-4` 替代 `glm-4-flash`）
3. 检查 `ai_queue.csv` 中的片段是否包含足够上下文

### Q: 如何添加新的角色？

编辑 `config/roles.json`，按格式添加：
```json
{
    "name": "新角色名",
    "priority": 5
}
```
