"""批量实例化回执服务：承载模板按批实例化后的字段校验回执。

domain_chain: 规则模板 (dq_template)
  └─ 批量实例化 → 批量实例化回执 (dq_inst_batch)
  └─ 认领 → 质量规则定义 (dq_rule_def)
  └─ 巡检 → 巡检任务 (dq_check_run)

回执逐表记录字段校验结论，巡检据此区分同批中可以执行的表和仍需补充字段的表；
批次的状态流转只允许在服务层修改，路由层不做业务判断。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DqInstBatchService(BaseModuleService):
    MODULE = "dq_inst_batch"
    ENTITY = "批量实例化回执"
    OPERATION_TERM = "批量实例化"
    LIST_FIELDS = [
        "回执编号", "提交时间", "目标表数", "可执行表数", "待补字段表数",
        "同批参数", "批次状态", "重试次数",
    ]
    REQUIRED_FIELDS = ["回执编号"]
    STATUS_ORDER = ["待校验", "部分可执行", "全部可执行", "已关闭"]
    ACTION_RULES = {"重新校验": "部分可执行", "确认完结": "已关闭"}
    NEGATIVE_ACTIONS = []

    def find_by_receipt_no(self, receipt_no: str) -> dict[str, Any] | None:
        """按回执编号而不是自增 id 定位批次，重试和巡检都靠业务编号关联。"""
        for row in store.rows(self.MODULE):
            if row.get("回执编号") == receipt_no:
                return row
        return None

    def next_receipt_id(self) -> int:
        rows = store.rows(self.MODULE)
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def summarize(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """汇总回执当前的可执行表、待补表和规则采用情况，供巡检准备阶段消费。"""
        details = receipt.get("明细", [])
        executable = [d for d in details if d.get("校验状态") == "可执行"]
        pending = [d for d in details if d.get("校验状态") == "待补字段"]
        adoption = {
            d["规则编号"]: self._rule_adoption(d["规则编号"])
            for d in executable if d.get("规则编号")
        }
        return {
            "回执编号": receipt.get("回执编号"),
            "批次状态": receipt.get("批次状态"),
            "目标表数": len(details),
            "可执行表": [d.get("目标表名") for d in executable],
            "待补字段表": [
                {"目标表名": d.get("目标表名"), "缺失字段": d.get("缺失字段", [])}
                for d in pending
            ],
            "规则采用次数": adoption,
        }

    @staticmethod
    def _rule_adoption(rule_no: str) -> int:
        for rule in store.rows("dq_rule_def"):
            if rule.get("规则编号") == rule_no:
                return int(rule.get("采用次数", 0))
        return 0

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "批量实例化"
        snapshot["desc"] = "按批校验模板目标字段并输出实例化回执"
        snapshot["artifact"] = "批量实例化回执集"
        snapshot["artifact_desc"] = "逐表记录字段核实结论与规则发布情况的回执集"
        return snapshot
