# 海岬振动监测采集

Python 3.11 标准库应用，使用 SQLite 和确定性离线 OPC UA 业务端口。无需安装依赖，不连接真实设备。值班员登记泵组及试验台监测项，接收通知、补采、确认；质量工程师读取按来源顺序整理的观测与明确缺口，保存当班 JSON 报告；晚班从同一数据库继续采集。

## 运行

在项目根目录运行：

```sh
python3.11 -B -m unittest discover -s tests
python3.11 -B -m capture.service samples/day_capture.json samples/gap_repair.json samples/restart_audit.json --expected samples/expected_results.json
```

两份运行清单的 verify 和 demo 均为实际 argv。demo 每次为每个脚本创建自己的临时 SQLite 数据库，运行后清理；重开动作保留同一个来源对象。只有实际结果与静态独立账本一致后，才输出 POINTWISE_SCENARIO JSON。异常保留非零退出码。应用不在项目根目录创建数据库。

## 一天的工作

1. 早班以来源、订阅 ID、初始已结清序号及监测项映射登记任务；相同配置重复执行保留全部进度，不兼容配置拒绝。
2. 来源交付数据，完整校验监测项、精确十进制、质量和时间戳；一项非法整条拒绝。
3. 保活只报告连接活动，不占据通知位置。
4. 后继先到时，先持久保存该通知及其确认资格，将前方缺失位置登记为待补；读者仍停在连续前沿。
5. 补采逐次恢复待补；可用列表仅影响顺序，暂时失败继续等待。
6. 只有明确 MessageNotAvailable 才形成永久缺口。数据或缺口结清连续前缀后，读者按来源位置看到成果。
7. 本地提交后才向来源确认数据；确认回复丢失或进程中断后，晚班重开原库继续补采、重试确认。
8. 质量工程师按设备读取观测，联合导出覆盖、全局缺口、待补与被阻挡数据。日界由保存报告表达，不清库或重置订阅。

## 公开 API

```python
from capture.storage import CaptureDatabase
from capture.subscription import configure
from capture.receiver import pump
from capture.recovery import resume
from capture.readings import read
from capture.completeness import audit
from capture.export import export_snapshot

# spec 和 port 脚本的字段参见 samples/*.json。
# db = CaptureDatabase('业务自有目录/capture.sqlite')
# handle = configure(db, spec)
# result = pump(handle, port, event_limit=10)
# handle = resume(db, spec['subscription_id'], port)
# records = read(handle, device='pump-1')
# report_bytes = export_snapshot(handle)
# db.close()
```

CaptureDatabase 接受文件路径或 :memory:。configure 返回 SubscriptionHandle；配置为 SubscriptionSpec 或对应字典，版本固定为 1。监测项唯一绑定 item_id、device、channel、unit、quantity；设备通道也必须唯一。支持速度 mm/s、加速度 m/s2，不换算。首次登记原子保存配置；再次调用比较完整规范化配置，不改变历史、待补或确认。已有订阅不接受任意配置编辑。

pump(handle, port, event_limit) 返回 PumpResult(processed, state, last_keepalive)。event_limit 必须为非负整数，0 不消费来源、不补采、不确认。每个已消费事件之后，对持久待补执行一次有限补采遍历，再尝试未决确认；临时失败不会在同一次遍历无限重试。无新 Publish 事件时，可显式调用 capture.republish.repair_pending 和 capture.acknowledgments.flush_acknowledgments 继续工作，或通过 resume 完成同一轮恢复。来源异常向调用者传播，先前已提交结果保留；调用者关闭本地库后可重开，不能重置来源对象。

resume(db, subscription_id, port) 验证来源及订阅身份，从持久事实恢复一轮补采与确认并返回原订阅句柄。未知订阅、身份不符均为 ValueError。仍不可用的位置继续待补。

read 返回来源有序的不可变 ReadRecord 元组；数据记录含有序 Observation，缺口记录含原因。每条记录含从本次登记起算的 position 和原 sequence。device=None 保留全部通知，包括空载荷通知；指定设备只保留含该设备观测的数据记录，明确缺口仍按全局消息口径列出。已登记但没有观测的设备返回空观测集合，未知设备报错；全局缺口可能仍在结果中。

audit 返回 CompletenessResult：state 为 empty、waiting 或 results；分别包含可见数据通知数、visible_gaps、pending、blocked_data、covered_positions、frontier、known_positions。covered_positions 是可见通知加可见缺口数；不推断采样频率和未见尾部。存在待补时状态为 waiting，可同时有已发布成果及缺口。

capture.projection.load_snapshot 取得不可变一致快照；select_readings、summarize_coverage、encode_report 可共同消费该快照。export_snapshot 返回 UTF-8 JSON 字节和末尾换行；可传入先前快照保存特定核对时刻。快照不包含运行时间或随机 ID，原快照在后续采集后保持不变。报告含订阅、来源、设备条件、有序记录和全局覆盖口径。

## 通知与来源口径

通知序号是 1..4294967295，最大值后接 1；0、布尔值及非整数非法。初始序号表示最后已结清的位置，不是一条需要输出的数据。端口保证当前可交付与待补窗口小于半环；不支持来源重建、转移、第二采集器接管或旧于该活动窗口的重放。内部稳定位置相对初始化起点递增，重开不重新分配位置。

PublishEvent 的 kind 为 data 或 keepalive。数据 items 为有序列表，每项有 item_id、value、quality、timestamp。value 必须为有限十进制文本或合法空值，不接受二进制浮点。Good 不可为空；Bad 和 Uncertain 可为空，不补零。时间戳可为空；非空必须可解析并带时区，保留原文本，不用于排序。读取保留首次接纳的数值文本；重复比较按精确十进制值，1.00 和 1.0 相等。项序、质量及时间戳原文本均参与完整内容比较。同身份冲突抛 NotificationConflict，保留旧成果和确认状态。空 items 的 data 仍是真通知，不能用载荷真假值区分保活。

DeterministicPort 的构造参数是 source_id、subscription_id、events、republish、acknowledgments。Publish 数据可在事件表中重复；断线事件为 {"kind":"disconnect"}。Republish 脚本按序号提供逐次回复，支持 temporary、unavailable、disconnect，以及含 notification 的 data 对象。脚本耗尽后查询独立重传表，无数据返回 temporary，绝不凭缺席生成 unavailable。available 是调度提示。明确不可取得只适用于实际待补请求；错位、错误来源或矛盾回复均拒绝，不能覆盖已存数据。

确认脚本支持 success、already、temporary、disconnect 和 lost；lost 表示来源已接受并释放重传数据但回复丢失，抛 PortDisconnected。ack_calls 记录每次真实调用及是否被接受；republish_calls 记录实际请求。position 参数是离线端口的来源位置审计关联，不改变线上的序号。重开本地库不重置端口确认事实。

## 持久边界与模块

配置由 subscription/configuration/items/catalog 拥有；接入由 receiver/notifications/values/port 负责。positions 提供共同来源身份；pending/availability/republish/gaps/recovery 管理补采和恢复；archive/identity/frontier/acknowledgments/storage 管理原子接纳、完整内容、可见性与确认；projection/readings/completeness/export/service 形成下游核对闭环。

数据、前方待补、前沿、确认资格同事务提交；明确缺口、待补结清与前沿也同事务提交。已持久但不可见的数据可以确认，缺口与保活永不确认。来源调用期间没有本地事务；返回后复核身份及最新事实。CaptureDatabase.before_commit 为可选的本地提交边界回调，异常导致完整回滚，可用于验证持久性。每个句柄由一个调用方顺序驱动；支持其他 SQLite 连接在远端调用期间独立写入，不提供同一连接的跨线程并发调用。

audit 会检查已知位置覆盖、连续前沿、载荷配置、数据/缺口/待补互斥及数据确认资格；发现不一致抛 ValueError，不自动修复。存储、来源和解码错误不伪装成成功。完整套件包含文件重开、第二连接写事务和真实子进程提交前后终止测试，均只使用测试自建临时目录。

## 场景账本

day_capture 从 9 开始接纳 10、保活 11、数据 11，结果为两个通知，源项序和精确值不变。
gap_repair 从 4294967293 开始，依次收到 1、保活 2、4294967294；4294967295 前两次补采暂时失败，第三次明确不可取得，结果为 4294967294、缺口 4294967295、1，位置 2 不产生任何来源工作。
restart_audit 先持久保存 11，确认被来源接受但回复丢失；重开补齐 10，重试确认得到 already，后续断线再重开，最终仅有 10、11。

原回归定义在 tests/test_capture.py、test_repair.py、test_recovery.py 和 test_consumption.py，保留 v1—v8 的八个命名主场景并增加逐边界断言。样例账本直接按来源位置手算，不调用被验证的排序、前沿或覆盖函数计算预期。
