from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


JobRecord = dict[str, Any]


def _read_json(path: str | Path) -> Any:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _ms_to_iso(ms: int | float | None) -> str | None:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _uniq_strs(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        s = _as_str(v)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _get_path(obj: Any, *path: str) -> Any:
    cur: Any = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _filter_dict_items(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, dict):
        # 兼容某些“外层包一层 data”的写法
        maybe_list = raw.get("data")
        if isinstance(maybe_list, list):
            raw = maybe_list

    if not isinstance(raw, list):
        return []

    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        if not item:  # {}
            continue
        out.append(item)
    return out


def _dedupe_by(records: Iterable[JobRecord], key: str) -> list[JobRecord]:
    seen: set[str] = set()
    out: list[JobRecord] = []
    for r in records:
        rid = _as_str(r.get(key))
        if not rid or rid in seen:
            continue
        seen.add(rid)
        out.append(r)
    return out


def clean_bytedance_jobs_json(json_path: str | Path) -> list[JobRecord]:
    """
    字节跳动（jobs.bytedance.com）清洗策略：
    - 丢弃全是 null 的 job_post_info 等冗余嵌套
    - 提取：id/title/category/recruit_type/subject/城市/发布时间/描述/要求/岗位 code
    - 统一 locations 为字符串列表；增加 publish_time_iso（UTC）
    """
    raw = _read_json(json_path)

    # 兼容你当前抓取函数直接返回 job_post_list（list），也兼容未来如果外层包一层 dict
    items = _filter_dict_items(raw)
    if isinstance(raw, dict):
        maybe_list = _get_path(raw, "data", "job_post_list")
        if isinstance(maybe_list, list):
            items = _filter_dict_items(maybe_list)

    cleaned: list[JobRecord] = []
    for item in items:
        job_id = _as_str(item.get("id"))
        if not job_id:
            continue

        city_info_name = _as_str(_get_path(item, "city_info", "name"))
        city_list_names = []
        city_list = item.get("city_list")
        if isinstance(city_list, list):
            for c in city_list:
                if isinstance(c, dict):
                    city_list_names.append(c.get("name"))

        locations = _uniq_strs([*city_list_names, city_info_name])

        subject_name = _get_path(item, "job_subject", "name")
        subject_zh = None
        if isinstance(subject_name, dict):
            subject_zh = _as_str(subject_name.get("zh_cn") or subject_name.get("i18n"))

        publish_time_ms = item.get("publish_time")
        record: JobRecord = {
            "source": "bytedance",
            "job_id": job_id,
            "code": _as_str(item.get("code")),
            "title": _as_str(item.get("title")),
            "category": _as_str(_get_path(item, "job_category", "name")),
            "category_parent": _as_str(_get_path(item, "job_category", "parent", "name")),
            "recruit_type": _as_str(_get_path(item, "recruit_type", "name")),
            "subject": subject_zh,
            "primary_city": city_info_name,
            "locations": locations,
            "publish_time_ms": publish_time_ms,
            "publish_time_iso": _ms_to_iso(publish_time_ms if isinstance(publish_time_ms, (int, float)) else None),
            "description": _as_str(item.get("description")),
            "requirement": _as_str(item.get("requirement")),
        }
        cleaned.append(record)

    return _dedupe_by(cleaned, "job_id")


def clean_alibaba_star_jobs_json(json_path: str | Path) -> list[JobRecord]:
    """
    阿里星（talent.alibaba.com）清洗策略：
    - 删除 trackId/bucket/空 map 等“追踪/占位字段”
    - 提取：id/name/description/requirement/地点/类别/批次/组织(circles)/修改时间/毕业时间范围
    - 时间戳统一增加 *_iso（UTC）
    """
    items = _filter_dict_items(_read_json(json_path))

    cleaned: list[JobRecord] = []
    for item in items:
        job_id = _as_str(item.get("id"))
        if not job_id:
            continue

        grad = item.get("graduationTime") if isinstance(item.get("graduationTime"), dict) else {}
        grad_from = grad.get("from") if isinstance(grad, dict) else None
        grad_to = grad.get("to") if isinstance(grad, dict) else None

        modify_time = item.get("modifyTime")

        record: JobRecord = {
            "source": "alibaba_star",
            "job_id": job_id,
            "title": _as_str(item.get("name")),
            "status": _as_str(item.get("status")),
            "category": _as_str(item.get("categoryName")),
            "category_type": _as_str(item.get("categoryType")),
            "batch_name": _as_str(item.get("batchName")),
            "circle_names": item.get("circleNames") if isinstance(item.get("circleNames"), list) else [],
            "work_locations": item.get("workLocations") if isinstance(item.get("workLocations"), list) else [],
            "interview_locations": item.get("interviewLocations") if isinstance(item.get("interviewLocations"), list) else [],
            "channels": item.get("channels") if isinstance(item.get("channels"), list) else [],
            "position_url": _as_str(item.get("positionUrl")),
            "modify_time_ms": modify_time,
            "modify_time_iso": _ms_to_iso(modify_time if isinstance(modify_time, (int, float)) else None),
            "graduation_from_ms": grad_from,
            "graduation_from_iso": _ms_to_iso(grad_from if isinstance(grad_from, (int, float)) else None),
            "graduation_to_ms": grad_to,
            "graduation_to_iso": _ms_to_iso(grad_to if isinstance(grad_to, (int, float)) else None),
            "description": _as_str(item.get("description")),
            "requirement": _as_str(item.get("requirement")),
        }
        cleaned.append(record)

    return _dedupe_by(cleaned, "job_id")


def clean_tencent_jobs_json(json_path: str | Path) -> list[JobRecord]:
    """
    腾讯（join.qq.com）清洗策略：
    - 过滤结尾的 null / {} 脏条目
    - 将 intentionBGDList 里 departmentList 的 comment 大段文案剥离（保留结构信息）
    - 提取：postId/title/desc/request/城市/项目/标签/方向(tidName)/部门 id 列表/url
    """
    items = _filter_dict_items(_read_json(json_path))

    cleaned: list[JobRecord] = []
    for item in items:
        post_id = _as_str(item.get("postId"))
        if not post_id:
            continue

        # ideptId: "288,194,166" -> [288, 194, 166]
        dept_ids: list[int] = []
        idept = _as_str(item.get("ideptId"))
        if idept:
            for part in idept.split(","):
                part = part.strip()
                if not part:
                    continue
                try:
                    dept_ids.append(int(part))
                except ValueError:
                    continue

        # 压缩 intentionBGDList：保留 bgd/department 的结构字段，丢 comment 大文本
        compact_bgds: list[dict[str, Any]] = []
        bgds = item.get("intentionBGDList")
        if isinstance(bgds, list):
            for bgd in bgds:
                if not isinstance(bgd, dict) or not bgd:
                    continue
                departments_out: list[dict[str, Any]] = []
                departments = bgd.get("departmentList")
                if isinstance(departments, list):
                    for dep in departments:
                        if not isinstance(dep, dict) or not dep:
                            continue
                        departments_out.append(
                            {
                                "id": dep.get("id"),
                                "name": dep.get("name"),
                                "bgid": dep.get("bgid"),
                                "ordering": dep.get("ordering"),
                                "enableFlag": dep.get("enableFlag"),
                                "workCityList": dep.get("workCityList") if isinstance(dep.get("workCityList"), list) else [],
                            }
                        )

                compact_bgds.append(
                    {
                        "id": bgd.get("id"),
                        "title": bgd.get("title"),
                        "showTitle": bgd.get("showTitle"),
                        "showTxt": bgd.get("showTxt"),
                        "departmentList": departments_out,
                    }
                )

        work_city_list = item.get("workCityList") if isinstance(item.get("workCityList"), list) else []
        recruit_city_list = item.get("recruitCityList") if isinstance(item.get("recruitCityList"), list) else []

        record: JobRecord = {
            "source": "tencent",
            "job_id": post_id,
            "url": f"https://join.qq.com/post_detail.html?postid={post_id}",
            "title": _as_str(item.get("title")),
            "category": _as_str(item.get("tidName")),  # “技术/产品/设计...”
            "project_name": _as_str(item.get("projectName")),
            "recruit_label_name": _as_str(item.get("recruitLabelName")),
            "recruit_type": item.get("recruitType"),
            "work_locations": _uniq_strs(work_city_list),
            "recruit_city_list": _uniq_strs(recruit_city_list),
            "department_ids": dept_ids,
            "intention_bgds": compact_bgds,
            "description": _as_str(item.get("desc")),
            "requirement": _as_str(item.get("request")),
        }
        cleaned.append(record)

    return _dedupe_by(cleaned, "job_id")