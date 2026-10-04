# 大型场馆人群安全与现场指挥

只使用Python标准库和SQLite的模块化服务，默认端口`8340`。支持场馆区域、入场口、容量、通道、安保岗位、医疗点、事件、限流、开放通道、疏散和医疗任务、人员到位、区域恢复、延迟重复事件、容量冲突和在途任务改派。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：容量计算、事件优先级、状态机和团队冲突约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等和审计查询。
- `src/service.py`：用例编排、权限校验和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8340
```

## 核心对象

`venue`为场馆，`zone`为区域，`gate`为入场口，`post`为安保岗位，`medical_point`为医疗点，`incident`为事件，`task`为现场任务。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 任务改派（reassign）

医疗点繁忙时，指挥员（`coordinator`/`admin`）可把在途（`enroute`）任务改派给空闲班组：

```json
{
  "action": "reassign",
  "data": {
    "new_team_id": "medic-team-2",
    "incident_version": 3,
    "reason": "医疗点饱和，临近班组承接",
    "reassigned_at": "2026-09-27T18:15:00Z"
  }
}
```

- 任务保持`enroute`，`team_id`换成新班组；原班组在同一事务内释放，新班组占用。
- 任务`data.reassignment_history`追加返工记录（原班组、新班组、原因、指挥员、事件版本与`incident_priority`）。
- 两个指挥员同时改派给同一班组时，后到者在事务锁内收到`409 ConflictError`；失败自动回滚，不会同时占住两个班组，可安全重试。
- `incident_version`必须是指挥员准备改派时看到的事件版本；事件状态一旦变化（处置、重开），旧改派按版本冲突拒绝；事件非`dispatched`/`reopened`时拒绝改派。
- 非`coordinator`/`admin`角色改派返回`403`；目标班组已有活跃任务、新旧班组相同返回冲突/校验错误。
- `GET /api/team_states`查看每队当前任务；旧数据库启动时由`PRAGMA user_version`驱动迁移，从历史活跃任务反推补齐。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

容量和调度规则为可运行的简化模型，不接入闸机、视频分析、室内定位、消防联动或真实应急指挥系统。
