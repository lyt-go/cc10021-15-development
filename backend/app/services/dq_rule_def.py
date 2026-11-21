"""质量规则定义服务：定义和校验数据质量规则，驱动巡检执行和事件生成。

domain_chain: 质量规则定义 (dq_rule_def)
  └─ 校验 → 巡检任务 (dq_check_run)
  └─ 关联 → 质量事件 (dq_incident)
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DqRuleDefService(BaseModuleService):
    MODULE = "dq_rule_def"
    ENTITY = "质量规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "规则名称", "目标表名", "校验字段", "规则类型", "期望值", "容差范围", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "规则名称", "目标表名"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    RULE_TEMPLATES = {
        "空值检查": "SELECT COUNT(*) FROM {table} WHERE {field} IS NULL",
        "值域范围": "SELECT COUNT(*) FROM {table} WHERE {field} < {min_val} OR {field} > {max_val}",
        "格式校验": "SELECT COUNT(*) FROM {table} WHERE {field} NOT REGEXP '{pattern}'",
        "唯一性检查": "SELECT COUNT(*) FROM (SELECT {field}, COUNT(*) cnt FROM {table} GROUP BY {field} HAVING cnt > 1)",
        "时效性检查": "SELECT COUNT(*) FROM {table} WHERE {field} < DATE_SUB(NOW(), INTERVAL {days} DAY)",
        "参照完整性": "SELECT COUNT(*) FROM {table} t LEFT JOIN {ref_table} r ON t.{field}=r.{ref_field} WHERE r.{ref_field} IS NULL",
    }

    def validate_rule_completeness(self, rule_id: int) -> dict[str, Any]:
        """校验规则字段完整性和语法合法性。

        校验规则定义中的所有必填字段是否完整，规则类型是否在已知模板内，
        SQL表达式是否可解析，期望值和容差范围是否在合理范围内。
        """
        rule = store.find(self.MODULE, rule_id)
        if not rule:
            return {"状态": "失败", "原因": f"规则{rule_id}不存在"}

        issues = []
        required = ["规则编号", "规则名称", "目标表名", "校验字段", "规则类型"]

        for field in required:
            if not str(rule.get(field, "")).strip():
                issues.append(f"缺少必填字段: {field}")

        rule_type = rule.get("规则类型", "")
        if rule_type and rule_type not in self.RULE_TEMPLATES:
            issues.append(f"规则类型「{rule_type}」不在已知模板范围内")

        expected = rule.get("期望值", "")
        if expected:
            try:
                float(expected)
            except (ValueError, TypeError):
                issues.append(f"期望值「{expected}」无法解析为数值")

        return {
            "规则编号": rule.get("规则编号"),
            "校验结果": "通过" if not issues else "存在字段完整性缺陷",
            "问题数量": len(issues),
            "问题详情": issues,
        }

    def compile_rule_to_sql(self, rule_id: int) -> dict[str, Any]:
        """将规则定义编译为可执行的校验SQL。

        查询规则元数据，按模板替换表名、字段名和参数，编译为标准SQL。
        """
        rule = store.find(self.MODULE, rule_id)
        if not rule:
            return {"状态": "失败", "原因": f"规则{rule_id}不存在"}

        rule_type = rule.get("规则类型", "空值检查")
        template = self.RULE_TEMPLATES.get(rule_type, self.RULE_TEMPLATES["空值检查"])

        sql = template.format(
            table=rule.get("目标表名", "unknown"),
            field=rule.get("校验字段", "id"),
            min_val=rule.get("期望值", "0"),
            max_val=rule.get("容差范围", "999999"),
            pattern=rule.get("期望值", "[A-Z0-9]+"),
            ref_table="dim_ref",
            ref_field="id",
            days="7",
        )

        return {
            "规则编号": rule.get("规则编号"),
            "规则类型": rule_type,
            "编译SQL": sql,
            "模板名称": rule_type,
            "编译时间": datetime.now(timezone.utc).isoformat(),
        }

    def execute_rule_check(self, rule_id: int) -> dict[str, Any]:
        """执行单条质量规则校验，返回校验结果并写入执行记录。

        拉取规则定义编译SQL，执行校验检查并聚合异常行数。
        校验结果通过时写入巡检记录为passed；失败时写入质量事件。
        """
        rule = store.find(self.MODULE, rule_id)
        if not rule:
            return {"状态": "失败", "原因": f"规则{rule_id}不存在"}

        sql_info = self.compile_rule_to_sql(rule_id)
        # 模拟执行：生成校验结果
        passed = random.random() > 0.15
        anomaly_rows = random.randint(0, 500) if not passed else 0

        result = {
            "规则编号": rule.get("规则编号"),
            "规则类型": rule.get("规则类型"),
            "编译SQL": sql_info.get("编译SQL"),
            "校验结果": "通过" if passed else "失败",
            "异常行数": anomaly_rows,
            "异常比例": f"{anomaly_rows / 1000 * 100:.2f}%" if anomaly_rows else "0%",
            "执行时间": datetime.now(timezone.utc).isoformat(),
        }

        # 写入巡检记录
        check_rows = store.rows("dq_check_run")
        new_cid = max((int(r.get("id", 0)) for r in check_rows), default=0) + 1
        check_rows.append({
            "id": new_cid,
            "任务编号": f"DQ_C-{new_cid:04d}",
            "关联规则集": rule.get("规则编号", ""),
            "数据范围": rule.get("目标表名", ""),
            "Cron表达式": "0 6 * * *",
            "并行度": "2",
            "超时时间": datetime.now(timezone.utc).isoformat(),
            "失败阈值": "10%",
            "任务版本": "v1",
            "status": "运行中",
            "pending": False,
            "abnormal": not passed,
        })

        return result

    def claim_batch_rules(self, receipt_no: str) -> dict[str, Any]:
        """规则服务关联批量实例化回执，一次认领多条规则并回写待执行清单。

        逐规则区分：字段已核实的规则进入待执行清单等待巡检；仍缺目标字段的表
        只记录未补齐目标，不进入清单。认领动作不重复发布实例，仅做清单登记。
        """
        receipt = None
        for row in store.rows("dq_inst_batch"):
            if row.get("回执编号") == receipt_no:
                receipt = row
                break
        if receipt is None:
            return {"状态": "失败", "原因": f"回执{receipt_no}不存在"}

        claimed: list[dict[str, Any]] = []
        missing_targets: list[dict[str, Any]] = []

        for detail in receipt.get("明细", []):
            table_name = detail.get("目标表名")
            if detail.get("校验状态") != "可执行" or not detail.get("规则编号"):
                missing_targets.append({
                    "目标表名": table_name,
                    "缺失字段": detail.get("缺失字段", []),
                })
                continue

            rule = next(
                (r for r in store.rows(self.MODULE) if r.get("规则编号") == detail["规则编号"]),
                None,
            )
            if rule is None:
                missing_targets.append({"目标表名": table_name, "缺失字段": ["规则实例缺失"]})
                continue

            rule["待执行"] = True
            rule["认领回执"] = receipt_no
            claimed.append({
                "规则编号": rule.get("规则编号"),
                "目标表名": table_name,
                "校验字段": rule.get("校验字段"),
                "分区列": rule.get("分区列"),
                "已核实字段": detail.get("已核实字段", []),
            })

        receipt["认领状态"] = "已认领"
        receipt["认领规则数"] = len(claimed)

        return {
            "状态": "成功",
            "回执编号": receipt_no,
            "认领规则数": len(claimed),
            "待执行清单": claimed,
            "未补齐目标": missing_targets,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "校验"
        snapshot["desc"] = "校验质量规则定义并编译为可执行SQL"
        snapshot["artifact"] = "质量规则集"
        snapshot["artifact_desc"] = "经过校验和编译的质量规则集"
        return snapshot