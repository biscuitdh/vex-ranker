"""One-off transcription of photographed coach notes for Worlds 2026."""

from __future__ import annotations

from typing import Any


SOURCE_LABEL = "coach_sheet_2026_04_23"
CAPTURED_AT = "2026-04-23T12:33:28-04:00"

TAG_KEYWORDS: dict[str, str] = {
    "rollover bid": "rollover_bid",
    "state finalist": "state_finalist",
    "won states": "won_states",
    "design @ states": "design_states",
    "hk champs": "hk_champs",
    "innovate @ pr nats": "innovate_pr_nats",
    "excellence @ states": "excellence_states",
    "triple crown japan nats": "triple_crown_japan_nats",
    "#1 in world": "number_one_in_world",
    "1 sig": "signature_event_note",
}


def infer_comment_tags(raw_note: str) -> list[str]:
    """Infer bounded structured tags from raw handwritten note text."""
    normalized = " ".join(str(raw_note or "").strip().lower().split())
    tags = [tag for phrase, tag in TAG_KEYWORDS.items() if phrase in normalized]
    return sorted(set(tags))


def build_manual_note(
    *,
    team_number: str,
    raw_note: str,
    circled_rank: int | None = None,
    blue_record_text: str = "",
    blue_wp: int | None = None,
    skills_total_manual: float | None = None,
    region: str = "",
    confidence: str = "medium",
) -> dict[str, Any]:
    """Build one persisted coach-note entry from the static transcription."""
    return {
        "team_number": team_number,
        "raw_note": raw_note.strip(),
        "circled_rank": circled_rank,
        "blue_record_text": blue_record_text.strip(),
        "blue_wp": blue_wp,
        "skills_total_manual": skills_total_manual,
        "region": region.strip(),
        "comment_tags": infer_comment_tags(raw_note),
        "source_label": SOURCE_LABEL,
        "captured_at": CAPTURED_AT,
        "confidence": confidence,
    }


COACH_SHEET_NOTES: list[dict[str, Any]] = [
    build_manual_note(
        team_number="6700A",
        region="Tokyo",
        circled_rank=40,
        blue_record_text="3-2",
        blue_wp=6,
        raw_note="42-17, 133 sk, triple crown japan nats",
        skills_total_manual=133,
        confidence="high",
    ),
    build_manual_note(
        team_number="6219Z",
        region="Wisconsin",
        circled_rank=28,
        blue_record_text="4-2",
        blue_wp=9,
        raw_note="79-18, 100 sk, state finalist i think",
        skills_total_manual=100,
        confidence="medium",
    ),
    build_manual_note(
        team_number="28006B",
        region="Texas",
        circled_rank=17,
        blue_record_text="4-2",
        blue_wp=11,
        raw_note="68-19, 171 sk, skills and think at states",
        skills_total_manual=171,
        confidence="medium",
    ),
    build_manual_note(
        team_number="19026",
        region="California",
        circled_rank=82,
        blue_record_text="1-5",
        blue_wp=2,
        raw_note="38-11, 58 sk, rollover bid",
        skills_total_manual=58,
        confidence="high",
    ),
    build_manual_note(
        team_number="16620B",
        region="Ontario",
        circled_rank=7,
        blue_record_text="4-1",
        blue_wp=11,
        raw_note="#1 in world?, 223 skills",
        skills_total_manual=223,
        confidence="high",
    ),
    build_manual_note(
        team_number="2208D",
        region="Puerto Rico",
        circled_rank=43,
        blue_record_text="3-3",
        blue_wp=7,
        raw_note="11-13, 72 sk, innovate @ pr nats",
        skills_total_manual=72,
        confidence="high",
    ),
    build_manual_note(
        team_number="85202R",
        region="Hong Kong / China",
        circled_rank=78,
        blue_record_text="1-4",
        blue_wp=2,
        raw_note="32-11, 114 sk, hk champs",
        skills_total_manual=114,
        confidence="high",
    ),
    build_manual_note(
        team_number="65236X",
        region="Mississippi",
        circled_rank=60,
        blue_record_text="2-4",
        blue_wp=5,
        raw_note="26-8, 135 sk, excellence @ states",
        skills_total_manual=135,
        confidence="high",
    ),
    build_manual_note(
        team_number="91416X",
        region="Nebraska",
        circled_rank=54,
        blue_record_text="2-4",
        blue_wp=6,
        raw_note="61-23, 162 sk",
        skills_total_manual=162,
        confidence="high",
    ),
    build_manual_note(
        team_number="8110T",
        region="",
        circled_rank=25,
        blue_record_text="4-1",
        blue_wp=8,
        raw_note="43-20, 153 sk, 1 sig",
        skills_total_manual=153,
        confidence="high",
    ),
    build_manual_note(
        team_number="83995H",
        region="Illinois",
        circled_rank=35,
        blue_record_text="3-3",
        blue_wp=8,
        raw_note="36-16, 129 sk, roll over",
        skills_total_manual=129,
        confidence="medium",
    ),
    build_manual_note(
        team_number="719163D",
        region="Pennsylvania",
        circled_rank=47,
        blue_record_text="3-3",
        blue_wp=7,
        raw_note="32-7, 114 sk, design @ states, 1 sig",
        skills_total_manual=114,
        confidence="high",
    ),
    build_manual_note(
        team_number="3150V",
        region="Ontario",
        circled_rank=30,
        blue_record_text="4-2",
        blue_wp=9,
        raw_note="78-24, 166 sk, won states (prov)",
        skills_total_manual=166,
        confidence="high",
    ),
    build_manual_note(
        team_number="4327QX",
        region="Texas",
        raw_note="57-25, 105 sk, won states",
        skills_total_manual=105,
        confidence="medium",
    ),
    build_manual_note(
        team_number="5327K",
        region="California",
        circled_rank=37,
        blue_record_text="4-2",
        blue_wp=8,
        raw_note="81-37, 98 sk, with them by",
        skills_total_manual=98,
        confidence="medium",
    ),
]
