"""批量实例化回执链路测试。

场景：同一批提交两张目标表，一张字段已核实，另一张未声明分区列；
验证字段校验通过才发布规则实例、同批参数快照一致、部分失败重试沿用原
规则编号、巡检待执行清单回写，以及补齐前后另一张表的启动时间和采用次数。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.dq_check_run import DqCheckRunService
from app.services.dq_inst_batch import DqInstBatchService
from app.services.dq_rule_def import DqRuleDefService
from app.services.dq_template import DqTemplateService
from app.store import store


@pytest.fixture()
def published_template():
    """准备一张带占位符、已发布的模板，返回其自增 id。"""
    svc = DqTemplateService()
    result = svc.standardize_rule_template(
        "批量空值检查模板",
        "空值检查",
        "SELECT COUNT(*) FROM {table} WHERE {column} IS NULL",
        ["table", "column"],
    )
    template_rows = store.rows("dq_template")
    template = next(row for row in template_rows if row["模板编号"] == result["模板编号"])
    return int(template["id"])


def _rule(rule_no: str) -> dict:
    return next(row for row in store.rows("dq_rule_def") if row["规则编号"] == rule_no)


class TestBatchInstantiationReceipt:
    def test_batch_with_one_valid_and_one_missing_partition(self, published_template):
        svc = DqTemplateService()
        result = svc.batch_instantiate(
            published_template,
            targets=[
                {"目标表名": "ods_order_info", "分区列": "dt", "校验字段": "amount"},
                # 第二张表没有声明分区列
                {"目标表名": "dwd_trade_detail", "校验字段": "amount"},
            ],
            batch_params={"column": "amount"},
        )

        assert result["状态"] == "成功"
        assert result["目标表数"] == 2
        assert result["可执行表数"] == 1
        assert result["待补字段表数"] == 1
        assert result["批次状态"] == "部分可执行"

        receipt = DqInstBatchService().find_by_receipt_no(result["回执编号"])
        details = {d["目标表名"]: d for d in receipt["明细"]}
        assert details["ods_order_info"]["校验状态"] == "可执行"
        assert details["dwd_trade_detail"]["校验状态"] == "待补字段"
        assert "分区列" in details["dwd_trade_detail"]["缺失字段"]

        # 只有字段校验通过的表发布了规则实例
        assert details["ods_order_info"]["规则编号"] is not None
        assert details["dwd_trade_detail"]["规则编号"] is None
        rule = _rule(details["ods_order_info"]["规则编号"])
        assert rule["status"] == "已生效"
        assert rule["来源回执"] == result["回执编号"]

    def test_compiled_results_share_same_batch_params(self, published_template):
        svc = DqTemplateService()
        result = svc.batch_instantiate(
            published_template,
            targets=[
                {"目标表名": "ods_order_info", "分区列": "dt"},
                {"目标表名": "dwd_trade_detail", "分区列": "dt"},
            ],
            batch_params={"column": "amount"},
        )
        receipt = DqInstBatchService().find_by_receipt_no(result["回执编号"])
        for detail in receipt["明细"]:
            # 并发提交的每条编译结果都引用同批参数 column=amount
            assert detail["编译参数"]["column"] == "amount"
            assert "amount" in detail["编译SQL"]
        assert receipt["同批参数"] == {"column": "amount"}

    def test_template_usage_count_only_counts_published(self, published_template):
        svc = DqTemplateService()
        before = int(store.find("dq_template", published_template)["使用次数"])
        svc.batch_instantiate(
            published_template,
            targets=[
                {"目标表名": "t_a", "分区列": "dt"},
                {"目标表名": "t_b"},  # 缺分区列，不发布
            ],
            batch_params={"column": "id"},
        )
        after = int(store.find("dq_template", published_template)["使用次数"])
        assert after - before == 1

    def test_claim_writes_back_pending_list(self, published_template):
        template_svc = DqTemplateService()
        batch = template_svc.batch_instantiate(
            published_template,
            targets=[
                {"目标表名": "ods_order_info", "分区列": "dt"},
                {"目标表名": "dwd_trade_detail"},
            ],
            batch_params={"column": "amount"},
        )
        claim = DqRuleDefService().claim_batch_rules(batch["回执编号"])

        assert claim["认领规则数"] == 1
        assert len(claim["待执行清单"]) == 1
        assert claim["待执行清单"][0]["目标表名"] == "ods_order_info"
        assert "分区列" in claim["待执行清单"][0]["已核实字段"]
        assert claim["未补齐目标"][0]["目标表名"] == "dwd_trade_detail"

        rule_no = claim["待执行清单"][0]["规则编号"]
        assert _rule(rule_no)["待执行"] is True

    def test_inspection_before_and_after_supplement(self, published_template):
        template_svc = DqTemplateService()
        batch = template_svc.batch_instantiate(
            published_template,
            targets=[
                {"目标表名": "ods_order_info", "分区列": "dt"},
                {"目标表名": "dwd_trade_detail"},
            ],
            batch_params={"column": "amount"},
        )
        receipt_no = batch["回执编号"]
        valid_rule_no = DqInstBatchService().find_by_receipt_no(receipt_no)["明细"][0]["规则编号"]
        DqRuleDefService().claim_batch_rules(receipt_no)

        check_svc = DqCheckRunService()

        # 补齐前跑巡检：只有第一张表实际开始校验，第二张被跳过
        first = check_svc.run_batch_inspection(receipt_no)
        assert len(first["实际执行规则"]) == 1
        assert first["实际执行规则"][0]["目标表名"] == "ods_order_info"
        assert first["统计"]["跳过"] == 1
        assert first["跳过目标"][0]["目标表名"] == "dwd_trade_detail"
        first_start = first["实际执行规则"][0]["实际开始校验时间"]
        assert _rule(valid_rule_no)["采用次数"] == "1"

        # 补齐缺失分区列后重试同批：已发布实例沿用原规则编号，新表获得新编号
        retry = template_svc.retry_instantiate_batch(
            receipt_no,
            extra_fields={"dwd_trade_detail": {"分区列": "dt", "校验字段": "amount"}},
        )
        assert retry["批次状态"] == "全部可执行"
        assert retry["本次新发布"] == 1
        receipt = DqInstBatchService().find_by_receipt_no(receipt_no)
        details = {d["目标表名"]: d for d in receipt["明细"]}
        # 原规则编号不变
        assert details["ods_order_info"]["规则编号"] == valid_rule_no
        new_rule_no = details["dwd_trade_detail"]["规则编号"]
        assert new_rule_no is not None and new_rule_no != valid_rule_no
        assert int(receipt["重试次数"]) == 1

        # 重试后需要重新认领，新实例才会进入待执行清单
        claim = DqRuleDefService().claim_batch_rules(receipt_no)
        assert claim["认领规则数"] == 2
        assert claim["未补齐目标"] == []

        # 补齐后再跑巡检：第二张表此时才有实际开始校验时间
        second = check_svc.run_batch_inspection(receipt_no)
        executed = {item["目标表名"]: item for item in second["实际执行规则"]}
        assert set(executed) == {"ods_order_info", "dwd_trade_detail"}
        assert executed["dwd_trade_detail"]["实际开始校验时间"] >= first_start
        assert second["统计"]["跳过"] == 0

        # 本批规则的采用次数：第一张累计 2 次，第二张首次 1 次
        assert _rule(valid_rule_no)["采用次数"] == "2"
        assert _rule(new_rule_no)["采用次数"] == "1"

        summary = DqInstBatchService().summarize(receipt)
        assert summary["待补字段表"] == []
        assert summary["规则采用次数"] == {valid_rule_no: 2, new_rule_no: 1}

    def test_retry_keeps_original_rule_number_without_duplicate_publish(self, published_template):
        template_svc = DqTemplateService()
        batch = template_svc.batch_instantiate(
            published_template,
            targets=[
                {"目标表名": "t_ready", "分区列": "dt"},
                {"目标表名": "t_missing"},
            ],
            batch_params={"column": "id"},
        )
        receipt_no = batch["回执编号"]
        original_no = DqInstBatchService().find_by_receipt_no(receipt_no)["明细"][0]["规则编号"]
        rule_count_before = len(store.rows("dq_rule_def"))

        # 缺字段的表本次仍未补齐：已发布实例的编号保持不变，也不重复发布
        retry = template_svc.retry_instantiate_batch(receipt_no, extra_fields={})
        assert retry["批次状态"] == "部分可执行"
        assert len(store.rows("dq_rule_def")) == rule_count_before
        receipt = DqInstBatchService().find_by_receipt_no(receipt_no)
        assert receipt["明细"][0]["规则编号"] == original_no
        assert int(receipt["重试次数"]) == 1
