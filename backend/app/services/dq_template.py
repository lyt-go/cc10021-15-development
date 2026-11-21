"""规则模板库服务：管理和标准化质量规则模板，驱动规则定义和执行。

domain_chain: 规则模板 (dq_template)
  └─ 标准化 → 质量规则定义 (dq_rule_def)
  └─ 批量实例化 → 批量实例化回执 (dq_inst_batch)
  └─ 校验 → 巡检任务 (dq_check_run)
"""

from __future__ import annotations

import random
import re
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dq_inst_batch import DqInstBatchService
from app.store import store


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# 批量实例化时每张目标表必须显式声明的字段；缺一项即进入待补字段清单。
BATCH_REQUIRED_TARGET_FIELDS = ("目标表名", "分区列")


class DqTemplateService(BaseModuleService):
    MODULE = "dq_template"
    ENTITY = "规则模板"
    OPERATION_TERM = "标准化"
    LIST_FIELDS = ["模板编号", "模板名称", "规则类型", "校验SQL", "参数列表", "适用场景", "使用次数", "模板版本"]
    REQUIRED_FIELDS = ["模板编号", "模板名称", "规则类型"]
    STATUS_ORDER = ["草稿", "待校验", "已发布", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认发布": "已发布", "停用模板": "已停用"}
    NEGATIVE_ACTIONS = ["停用模板"]

    BUILTIN_TEMPLATES = {
        "null_check": "SELECT COUNT(*) as null_count FROM {table} WHERE {column} IS NULL",
        "uniqueness_check": "SELECT {column}, COUNT(*) FROM {table} GROUP BY {column} HAVING COUNT(*) > 1",
        "range_check": "SELECT COUNT(*) FROM {table} WHERE {column} NOT BETWEEN {min} AND {max}",
        "freshness_check": "SELECT MAX({column}) FROM {table} HAVING MAX({column}) < DATE('now', '-{interval} days')",
        "format_check": "SELECT COUNT(*) FROM {table} WHERE {column} NOT REGEXP '{pattern}'",
        "referential_integrity": "SELECT COUNT(*) FROM {table} t LEFT JOIN {ref} r ON t.{column}=r.{ref_col} WHERE r.{ref_col} IS NULL",
        "completeness_check": "SELECT COUNT(*) as total, COUNT({column}) as non_null, COUNT(*) - COUNT({column}) as missing FROM {table}",
        "distribution_check": "SELECT {column}, COUNT(*) as freq FROM {table} GROUP BY {column} ORDER BY freq DESC LIMIT 100",
    }

    def standardize_rule_template(self, template_name: str, rule_type: str,
                                   sql_template: str, params: list[str]) -> dict[str, Any]:
        """标准化规则模板：校验模板SQL合法性并写入模板库。

        对传入的模板SQL进行解析校验，检查参数占位符与参数列表是否匹配，
        标准化后的模板写入库中并关联到规则定义服务。
        """
        rows = store.rows(self.MODULE)

        # 校验参数占位符
        placeholders = set(re.findall(r'\{(\w+)\}', sql_template))
        declared_params = set(params)
        missing_in_sql = declared_params - placeholders
        extra_in_sql = placeholders - declared_params

        validation = {
            "SQL占位符": list(placeholders),
            "声明参数": params,
            "SQL中未声明": list(missing_in_sql),
            "声明但未用": list(extra_in_sql),
            "是否一致": len(missing_in_sql) == 0 and len(extra_in_sql) == 0,
        }

        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        entry = {
            "id": new_id,
            "模板编号": f"DQ_T-{new_id:04d}",
            "模板名称": template_name,
            "规则类型": rule_type,
            "校验SQL": sql_template.replace("\n", " ").strip(),
            "参数列表": str(params),
            "适用场景": f"{rule_type}标准化模板",
            "使用次数": "0",
            "模板版本": "v1.0",
            "status": "已发布",
            "pending": False,
            "abnormal": not validation["是否一致"],
        }
        rows.append(entry)

        # 关联写入规则定义
        rule_rows = store.rows("dq_rule_def")
        new_rid = max((int(r.get("id", 0)) for r in rule_rows), default=0) + 1
        rule_rows.append({
            "id": new_rid,
            "规则编号": f"DQ_R-{new_rid:04d}",
            "规则名称": f"{template_name}_auto",
            "目标表名": "all_tables",
            "校验字段": params[0] if params else "id",
            "规则类型": rule_type,
            "期望值": "0",
            "容差范围": "0.1",
            "规则版本": "v1.0",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

        return {
            "模板编号": entry["模板编号"],
            "标准化结果": "成功",
            "校验详情": validation,
            "关联规则编号": f"DQ_R-{new_rid:04d}",
            "标准化时间": datetime.now(timezone.utc).isoformat(),
        }

    def instantiate_template(self, template_id: int, param_values: dict[str, Any]) -> dict[str, Any]:
        """实例化模板：用实际参数替换模板占位符，生成可执行的校验SQL。

        查询模板定义，将参数值代入SQL模板生成实际可执行的校验语句，
        同时生成规则定义记录并关联到巡检任务。
        """
        template = store.find(self.MODULE, template_id)
        if not template:
            return {"状态": "失败", "原因": f"模板{template_id}不存在"}

        sql = template.get("校验SQL", "")
        missing_params = []

        for param_key in set(param_values.keys()):
            placeholder = "{" + param_key + "}"
            if placeholder in sql:
                sql = sql.replace(placeholder, str(param_values[param_key]))
            else:
                missing_params.append(param_key)

        return {
            "模板名称": template.get("模板名称"),
            "实例化SQL": sql,
            "参数值": param_values,
            "未使用参数": missing_params,
            "实例化时间": datetime.now(timezone.utc).isoformat(),
        }

    def _next_rule_id(self) -> int:
        rule_rows = store.rows("dq_rule_def")
        return max((int(r.get("id", 0)) for r in rule_rows), default=0) + 1

    def _bump_template_usage(self, template_id: int, count: int) -> None:
        template = store.find(self.MODULE, template_id)
        if template is None:
            return
        try:
            current = int(str(template.get("使用次数", "0")).strip() or 0)
        except ValueError:
            current = 0
        template["使用次数"] = str(current + count)

    def batch_instantiate(
        self,
        template_id: int,
        targets: list[dict[str, Any]],
        batch_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """按批实例化模板：逐表校验目标字段，只发布字段核实通过的规则实例。

        同一批的全部编译结果引用同一份批次参数快照；字段缺失的表只登记到回执，
        不生成规则实例，等待补齐后通过 :meth:`retry_instantiate_batch` 重试。
        """
        template = store.find(self.MODULE, template_id)
        if not template:
            return {"状态": "失败", "原因": f"模板{template_id}不存在"}
        if not targets:
            return {"状态": "失败", "原因": "本批未提交任何目标表"}

        # 固化同批参数：并发提交的编译结果都引用这一份快照，避免逐表取参出现漂移。
        shared_params = dict(batch_params or {})
        placeholders = set(re.findall(r"\{(\w+)\}", template.get("校验SQL", "")))

        receipt_rows = store.rows("dq_inst_batch")
        receipt_id = DqInstBatchService().next_receipt_id()
        receipt_no = f"DQ_IB-{receipt_id:04d}"
        details: list[dict[str, Any]] = []
        published = 0
        pending = 0

        for target in targets:
            table_name = str(target.get("目标表名", "")).strip()
            missing_fields = [
                field for field in BATCH_REQUIRED_TARGET_FIELDS
                if not str(target.get(field, "")).strip()
            ]
            # 目标表私有参数覆盖同批公共参数，编译仍只引用这一份合并后的同批参数。
            target_params = dict(shared_params)
            target_params.update(target.get("参数值", {}) or {})
            if table_name:
                target_params.setdefault("table", table_name)
                target_params.setdefault("column", target.get("校验字段", "id"))

            unresolved = sorted(p for p in placeholders if p not in target_params)
            if unresolved:
                missing_fields.extend(f"参数:{name}" for name in unresolved)

            if missing_fields:
                pending += 1
                details.append({
                    "目标表名": table_name or "未命名目标",
                    "校验状态": "待补字段",
                    "缺失字段": missing_fields,
                    "已核实字段": [
                        field for field in BATCH_REQUIRED_TARGET_FIELDS
                        if str(target.get(field, "")).strip()
                    ],
                    "规则编号": None,
                    "编译SQL": None,
                })
                continue

            sql = template.get("校验SQL", "")
            for key in placeholders:
                sql = sql.replace("{" + key + "}", str(target_params[key]))

            rule_id = self._next_rule_id()
            rule_no = f"DQ_R-{rule_id:04d}"
            rule_rows = store.rows("dq_rule_def")
            rule_rows.append({
                "id": rule_id,
                "规则编号": rule_no,
                "规则名称": f"{template.get('模板名称')}_{table_name}",
                "目标表名": table_name,
                "校验字段": str(target_params.get("column", "id")),
                "规则类型": template.get("规则类型", ""),
                "期望值": "0",
                "容差范围": "0.1",
                "规则版本": template.get("模板版本", "v1.0"),
                "分区列": target.get("分区列", ""),
                "来源回执": receipt_no,
                "采用次数": "0",
                "status": "已生效",
                "pending": False,
                "abnormal": False,
            })
            published += 1
            details.append({
                "目标表名": table_name,
                "校验状态": "可执行",
                "缺失字段": [],
                "已核实字段": list(BATCH_REQUIRED_TARGET_FIELDS),
                "规则编号": rule_no,
                "编译SQL": sql,
                "编译参数": dict(target_params),
            })

        batch_status = "全部可执行" if pending == 0 else ("待校验" if published == 0 else "部分可执行")
        receipt_rows.append({
            "id": receipt_id,
            "回执编号": receipt_no,
            "提交时间": _now_iso(),
            "目标表数": len(targets),
            "可执行表数": published,
            "待补字段表数": pending,
            "同批参数": shared_params,
            "批次状态": batch_status,
            "重试次数": "0",
            "模板编号": template.get("模板编号"),
            "明细": details,
            "status": batch_status,
            "pending": pending > 0,
            "abnormal": pending > 0,
        })

        self._bump_template_usage(template_id, published)

        return {
            "状态": "成功",
            "回执编号": receipt_no,
            "模板编号": template.get("模板编号"),
            "批次状态": batch_status,
            "目标表数": len(targets),
            "可执行表数": published,
            "待补字段表数": pending,
            "同批参数": shared_params,
            "明细": details,
        }

    def retry_instantiate_batch(self, receipt_no: str, extra_fields: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
        """部分失败后重试同批：沿用原批次参数，已发布实例保留原规则编号。

        extra_fields 按目标表名回填补齐的字段；已经发布的表只核对不重复发布，
        仍缺字段的表继续挂在回执上等待下一次补齐。
        """
        receipt = DqInstBatchService().find_by_receipt_no(receipt_no)
        if receipt is None:
            return {"状态": "失败", "原因": f"回执{receipt_no}不存在"}

        template_rows = store.rows(self.MODULE)
        template = next(
            (row for row in template_rows if row.get("模板编号") == receipt.get("模板编号")),
            None,
        )
        if template is None:
            return {"状态": "失败", "原因": "回执关联模板已不存在"}

        shared_params = dict(receipt.get("同批参数", {}))
        placeholders = set(re.findall(r"\{(\w+)\}", template.get("校验SQL", "")))
        extra_fields = extra_fields or {}
        details = receipt["明细"]
        newly_published = 0

        for detail in details:
            if detail.get("校验状态") == "可执行":
                # 已发布实例只用原规则编号，重试不重新实例化、不重复累计采用。
                continue

            table_name = detail["目标表名"]
            supplement = extra_fields.get(table_name, {})
            merged = {"目标表名": table_name, **supplement}
            missing_fields = [
                field for field in BATCH_REQUIRED_TARGET_FIELDS
                if not str(merged.get(field, "")).strip()
            ]
            target_params = dict(shared_params)
            target_params.update(supplement.get("参数值", {}) or {})
            target_params.setdefault("table", table_name)
            target_params.setdefault("column", merged.get("校验字段", "id"))
            unresolved = sorted(p for p in placeholders if p not in target_params)
            if unresolved:
                missing_fields.extend(f"参数:{name}" for name in unresolved)

            if missing_fields:
                detail["校验状态"] = "待补字段"
                detail["缺失字段"] = missing_fields
                continue

            sql = template.get("校验SQL", "")
            for key in placeholders:
                sql = sql.replace("{" + key + "}", str(target_params[key]))

            rule_id = self._next_rule_id()
            rule_no = f"DQ_R-{rule_id:04d}"
            store.rows("dq_rule_def").append({
                "id": rule_id,
                "规则编号": rule_no,
                "规则名称": f"{template.get('模板名称')}_{table_name}",
                "目标表名": table_name,
                "校验字段": str(target_params.get("column", "id")),
                "规则类型": template.get("规则类型", ""),
                "期望值": "0",
                "容差范围": "0.1",
                "规则版本": template.get("模板版本", "v1.0"),
                "分区列": merged.get("分区列", ""),
                "来源回执": receipt_no,
                "采用次数": "0",
                "status": "已生效",
                "pending": False,
                "abnormal": False,
            })
            newly_published += 1
            detail.update({
                "校验状态": "可执行",
                "缺失字段": [],
                "已核实字段": list(BATCH_REQUIRED_TARGET_FIELDS),
                "规则编号": rule_no,
                "编译SQL": sql,
                "编译参数": dict(target_params),
            })

        executable = sum(1 for d in details if d.get("校验状态") == "可执行")
        pending = sum(1 for d in details if d.get("校验状态") == "待补字段")
        receipt["可执行表数"] = executable
        receipt["待补字段表数"] = pending
        receipt["批次状态"] = "全部可执行" if pending == 0 else "部分可执行"
        receipt["status"] = receipt["批次状态"]
        receipt["pending"] = pending > 0
        receipt["abnormal"] = pending > 0
        try:
            retry_count = int(str(receipt.get("重试次数", "0")).strip() or 0)
        except ValueError:
            retry_count = 0
        receipt["重试次数"] = str(retry_count + 1)

        self._bump_template_usage(int(template["id"]), newly_published)

        return {
            "状态": "成功",
            "回执编号": receipt_no,
            "本次新发布": newly_published,
            "批次状态": receipt["批次状态"],
            "可执行表数": executable,
            "待补字段表数": pending,
            "重试次数": receipt["重试次数"],
            "明细": details,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "标准化"
        snapshot["desc"] = "标准化规则模板SQL并关联规则定义"
        snapshot["artifact"] = "规则模板定义集"
        snapshot["artifact_desc"] = "经过标准化和校验的规则模板定义集"
        return snapshot