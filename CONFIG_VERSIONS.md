# 监测配置迟到时的通知绑定

## 已有系统与本次增量

README.md、原源码及 tests 中的全部原回归描述正常 V1；配置格式中的 version=1 仍不变。V1 已有采集、补采、原子接纳、来源位置、确认重试、重开和一致快照导出。本题增加订阅内不可变的监测配置版本及未知版本候选；不改变来源序号规则，不做单位换算、配置删除、历史通知更正、订阅转移或多采集器接管。允许全量扫描候选，不规定新增索引或性能上限。

## 登记接口

新增 capture.configuration.register_config(handle, config_id, items)。config_id 是订阅内按原文比较的非空字符串；items 沿用原 MonitoredItem 和 ItemRegistry 规则，登记比较按 item_id 排序后的完整配置，与输入项顺序无关。初始配置的保留 ID 为 initial，来自 configure 的 spec.items；configure 的原幂等及不兼容重配规则保留。

同 ID 同规范化配置幂等；异配置或非法配置抛 ValueError，整个调用不改变状态。新登记一个版本时，在同一事务中登记元数据并解释该版本的全部等待候选，每条单独判定合法与否。一条含未知 item_id 时整条变为 invalid，不阻止同版本其他合法候选变为合格数据。存储异常或 before_commit 回调异常则整个登记及本次转换回滚。

返回字典 {"promoted": [序号...], "invalid": [序号...]}，只列本次转换的通知，分别按稳定来源位置排序，不按回绕前后的序号整数排序。已存在同配置的幂等登记返回两个空列表，不改变任何进度。register_config 不调用来源；转换获得的确认工作随后由原 pump、resume 或 flush_acknowledgments 驱动。

实现落在 capture/config_versions.py（不可变版本存储、候选转换），由 capture/configuration.py 再导出 register_config。版本存入 config_versions 表；等待与非法候选存入 candidates 表，带 config_id、状态与首次来源内容；合格通知的 notifications 行新增 config_id，旧库迁移时缺省补为 initial。

## 通知接入与身份

原 PublishEvent、decode_publish(event, registry)、accept_notification(handle, notification, expected=None) 对 initial 的原用法继续有效，新内部表示由实现决定。新增源夹具 capture.version_fixture.VersionedPublishEvent 是 PublishEvent 的子类，只多一个 config_id 字段；VersionedPort 使用原来源的 Publish、Republish、重传和确认脚本语义。字典形式 data 可加 config_id；未携带该字段的旧数据绑定 initial。保活没有配置绑定，夹具拒绝带 config_id 的保活。Republish data 中 notification 的绑定与 Publish 完全相同。实现须支持这两个真实接入口，不能只在演示脚本处理新版事件。

未知版本的数据先校验与映射无关的部分：序号、配置 ID、完整有序项列表、非空字符串 item_id，以及原 value、quality、timestamp 的合法性。十进制精度、空值、原时间戳及省略 timestamp 等同于 None 的规则沿用 V1。任一无关字段非法抛 ValueError，整条不改变本地状态。通过该校验但版本未知时持久保存候选及其来源位置；不能猜测设备、通道或单位，不能因为未知版本丢弃它。

新位置若绑定已知配置，映射校验仍沿用 V1：任何未知监测项抛 ValueError，整条不接纳。invalid 是先合法接纳的等待候选在元数据到达后才发现映射不合法的持久结果，不能用抛异常将已收到的候选遗失。

同位置的身份由 config_id 及完整有序的来源内容决定。十进制按数值比较并保留首次文本；项序、质量、原时间戳等比较沿用 V1，不将派生的设备、通道或单位当成另一份来源内容。通过版本无关校验的重投，若命中等待、invalid 或合格通知，须先按同一身份判重；相同则幂等，不改变首次文本及状态，不重复创建确认工作，invalid 的相同重投也不抛新的映射错误。不同则抛 capture.identity.NotificationConflict，整次接纳调用无效。不能因登记了配置而改变判重结论。明确缺口仍不能被数据替换。

## 位置、资格与恢复

waiting_config、invalid、合格数据、真正缺失和明确缺口须在同一来源位置上保持一致归属。候选既不是真正缺失，也不是合格数据；收到候选后不得继续为该位置发起补采，已有未返回请求的不可取得回复也不能覆盖候选或 invalid。矛盾的不可取得回复抛 ValueError 并保全本地事实。其他真正缺失位置继续沿 V1 补采。

只有全部项按原绑定版本校验成功的整条通知才取得确认资格；waiting_config 和 invalid 永不确认。合格数据即使被前方未决位置阻挡也可确认。连续可见前沿只越过合格数据或原有明确缺口，不能越过 waiting_config 或 invalid。接纳及转换须将内容、位置归属、待补变化、确认资格和前沿原子提交。

重开保留版本、候选、invalid、首次内容及既有确认事实，不重新初始化，不靠重取通知恢复等待内容。确认已被来源接受但回复丢失时仍沿原协议重试，不重复观测。原来源身份、活动窗口和单调用方顺序驱动边界保持，来源调用不得持有本地事务。

## 读取与覆盖结果

read、select_readings 及导出按每条通知的原绑定版本解释设备、通道和单位；指定设备可来自当前快照内任一已登记版本。未知设备仍报 ValueError。旧快照的配置、读取结果和导出字节在之后登记或转换后保持不变。

audit 的 CompletenessResult 增加 waiting_config 和 invalid 两个序号元组；to_dict 和导出 coverage 对应为列表，均按来源位置排序。pending 只列真正缺失，blocked_data 只列合格但尚不可见的数据。存在 pending、waiting_config 或 invalid 任一种时 state 为 waiting；其他 empty/results 及覆盖计数沿 V1。等待或非法通知不算可见记录和 covered_positions。load_snapshot 与 validate_snapshot 须继续检查位置覆盖、互斥、正确版本解释、前沿和确认资格；发现事实不一致报错且不自动修复。

## 提供的素材与运行

samples/config_versions.json 是逐动作的手算账本。初始位置9，11绑定B先到，10绑定initial随后到；B让同一 item a 从 pump-1 的 mm/s 改绑 centrifuge-4 的 m/s2。登记前11等待且无确认，登记后10、11各用原版本解释；登记前取得的快照仍停在10。账本不调用目标实现计算预期。

capture/version_fixture.py 仅提供带绑定的确定性来源和动作驱动器，不提供版本存储、候选转换或新版读取。python3.11 -B -m capture.version_fixture --check-port 核对来源夹具，初始基线可运行。python3.11 -B -m capture.version_fixture 运行上述完整业务账本，待新增接口实现后才能成功；失败保持非零退出码。

原正常核验为 python3.11 -B -m unittest discover -s tests；原三个场景沿 README 命令运行。保留全部原测试字节。补充配置迟到、同身份跨状态重投、invalid与合法后继共存、登记冲突与提交失败回滚、重开及丢确认回复的业务回归；须独立核对实际来源确认调用、原版本观测和等待分类，不能只核对内部状态自洽。可复用 before_commit、原来源脚本和快照设施。

新增 tests/test_late_config.py 覆盖：候选不入补采不确认且阻挡前沿、登记后各归原设备并取得确认资格、源未知绑定 Republish 候选、映射无关字段逐条拒绝、同位置跨 waiting/invalid/合格判重、数字等值保留首次文本、非法候选整条 invalid 且不影响合法后继、登记幂等与冲突及 before_commit 回滚、重开保全与丢回复确认重试、设备筛选覆盖已登记版本、旧快照字节不变、版本事实损坏检测、候选提交前后子进程终止。

沿用两份现有运行清单的字段格式：version 为1，kind 为 cli，setup 为准备命令的 argv 数组列表，verify 和 demo 各为一个 argv 数组。完成后 verify 须实际覆盖原回归和新增业务核验，demo 须运行配置迟到后正确解释并确认的真实链路；可以复用本文件所列账本驱动器。交付简短说明，解释通知身份、候选状态与确认资格如何交接。

当前清单：verify 为原 unittest discover；demo 为 python3.11 -B -m capture.demo，依次跑三个原场景字面账本、来源夹具自检与 samples/config_versions.json 迟到绑定账本（11等B、10先可见、B到后各归原设备，并重开与确认）。

## 交接说明

- 通知身份由 config_id 加完整有序来源内容决定；十进制按数值比较，首次文本原样保留，项序、质量与时间戳文本都参与比较。同一身份重投不论落在 waiting、invalid 还是合格状态都幂等；任何不同内容或不同绑定都抛 NotificationConflict，整次调用不改状态。
- 未知绑定的数据只做映射无关校验后持久为该版本的 waiting 候选，占住来源位置：既不进入补采，也永不确认，且阻挡连续可见前沿；它不会被当作真正缺失，也不会被明确缺口覆盖。
- register_config 在同一事务里登记版本并逐条解释等待候选：合法者转成合格通知、获得确认资格并在同一事务中推进前沿；未知映射者整条留为 invalid，继续占位阻挡前沿但不影响其他位置或后续合法通知取得确认。
- 合格数据即使被前方 waiting_config 或 invalid 阻挡也可确认；确认工作由原 pump、resume、flush_acknowledgments 驱动，来源调用期间不持有本地事务。重开只从持久事实恢复，不重取来源内容。
