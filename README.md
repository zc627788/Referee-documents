# 裁判文书落款提取系统

从裁判文书 CSV 数据中自动提取落款签名信息（审判长、审判员、书记员等），输出为结构化宽表。

## 核心特性

- **规则优先**：基于物理切片法 + 百家姓验证提取，规则成功率 > 96%
- **断点续传**：SQLite 记录进度，中断后重跑自动跳过已处理行
- **流式处理**：`chunksize=5000` 分块读取，2.4GB / 60 万条文件仅占 ~200MB 内存
- **动态角色**：角色字典外置于 `config/roles.json`，可运行时审核追加新角色
- **宽表输出**：每个角色独立一列，同角色多人用 `;` 分隔

## 项目结构

```
Referee-documents/
├── config/
│   ├── config.json          # 基础配置（API/处理参数）
│   ├── roles.json           # 角色字典（24种角色 + 优先级）
│   └── .env                 # API_KEY（不入库）
├── src/
│   ├── run_pipeline.py      # 统一入口（process / status / review-roles）
│   ├── core/
│   │   ├── config.py        # 配置管理
│   │   ├── locator.py       # 落款区域定位（末尾1500字反向搜索）
│   │   ├── rule_extractor.py # 规则提取器 v4.1（物理切片+百家姓守门）
│   │   ├── progress_db.py   # SQLite 断点续传 + 角色发现
│   │   └── utils.py         # 宽表输出 + 增量CSV写入器
│   └── models/
│       └── person.py        # Person 数据模型
├── data/output/              # 输出目录（自动创建）
├── input/                    # 输入 CSV 文件
├── template.csv              # 输出格式模板
└── README.md
```

## 快速开始

### 1. 安装依赖

```bash
pip install pandas tqdm
```

### 2. 处理文书

```powershell
# 处理单个 CSV 文件（全量）
python src/run_pipeline.py process --input "input/2016年裁判文书数据/2016年裁判文书数据_马克数据网/2016年01月裁判文书数据.csv"

# 限制行数（测试用）
python src/run_pipeline.py process --input "input/.../2016年01月裁判文书数据.csv" --limit 100000

# 指定输出目录
python src/run_pipeline.py process --input "input/.../xxx.csv" --output-dir "data/output"
```

### 3. 查看进度

```powershell
python src/run_pipeline.py status
# 输出：
#   提取成功 (done)  : 487,099
#   落款有误         :   1,241
#   无落款           :  38,051
```

### 4. 审核新发现的角色

```powershell
python src/run_pipeline.py review-roles
# 交互式确认后自动写入 roles.json
```

## 断点续传

处理过程中可随时 `Ctrl+C` 中断，进度自动保存到 `data/output/progress.sqlite`。
再次运行**相同命令**，已处理的行会自动跳过，从中断处继续。

```powershell
# 第一次：跑10万条后中断
python src/run_pipeline.py process --input "..." --limit 100000

# 第二次：自动从第100001条继续
python src/run_pipeline.py process --input "..."
```

> **注意**：如需**重头开始**，先删除输出目录：`Remove-Item -Recurse -Force data\output\*`

## 输出格式

### 提取结果 (`*_result.csv`)

| 文件 | 序号 | 案号 | 审判长 | 审判员 | 人民陪审员 | 书记员 | ... |
|------|------|------|--------|--------|------------|--------|-----|
| 2016年01月... | 1 | (2016)x民初字第1号 | 张三 | 李四;王五 | 赵六 | 陈七 | |

- 同角色多人用 `;` 分隔
- 共 24 个角色列（代理审判长、审判长、审判员、... 法警）

### 异常文书 (`*_exceptions.csv`)

| 案号 | 标识 | 原文 |
|------|------|------|
| (2015)x字第402号 | 落款有误 | 全文内容... |
| (2016)x字第100号 | 无落款 | 全文内容... |

## 性能指标（2016年1月 · 526,391条）

| 指标 | 数值 |
|------|------|
| 规则成功率 | 96.1% |
| 无落款 | 3.6% |
| 落款有误 | 0.2% |
| 处理速度 | ~2,300 条/秒 |
| 内存占用 | ~200MB |
| 60万条耗时 | ~4分钟 |

## 角色字典

`config/roles.json` 定义了 24 种角色及优先级。规则提取时优先匹配最长角色名，
避免"人民陪审员"被错误截取为"陪审员"。

如需新增角色，可直接编辑 `roles.json`，或使用 `review-roles` 命令从数据中自动发现。
