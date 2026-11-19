"""
아지트 내전 모집 봇
 - /모바시내전모집, /저티어내전모집, /고티어내전모집
 - 참가/대기 신청(신청 시각 표시) → 10명 마감 시 티어 정보 → 팀 입력 → 경기 진행 → 승리 기록
 - /강제취소 /내전현황 /내전모집취소 /뉴비정리 /순위 /전적 /전적수정 /전적초기화 /주사위 /취소명단
"""
import asyncio
import io
import json
import logging
import random
import re
import sqlite3
import time
import unicodedata
from collections import defaultdict
from datetime import date, datetime, time as clock_time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands, tasks

import config

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
FORMAT_NAME = {"bo3": "3판2선", "bo1": "단판"}
KIND_NAME = {"mobashi": "모바시", "reserve": "예약"}

GROUPS = {
    "single": {"name": "단판", "emoji": "🎯", "color": 0x3498DB,
               "channels": config.SINGLE_CHANNELS, "roles": []},
    "mobashi": {"name": "모바시", "emoji": "🔥", "color": 0x3498DB,
                "channels": config.MOBASHI_CHANNELS, "roles": []},
    "low": {"name": "저티어", "emoji": "🌱", "color": 0x2ECC71,
            "channels": config.LOW_CHANNELS, "roles": config.LOW_TIER_ROLES},
    "high": {"name": "고티어", "emoji": "👑", "color": 0x9B59B6,
             "channels": config.HIGH_CHANNELS, "roles": config.HIGH_TIER_ROLES},
}

LINES = ["TOP", "JG", "MID", "AD", "SUP", "ALL"]
LINE_ALIASES = {
    "TOP": "TOP", "탑": "TOP",
    "JG": "JG", "JUG": "JG", "JUNGLE": "JG", "정글": "JG",
    "MID": "MID", "미드": "MID",
    "AD": "AD", "ADC": "AD", "BOT": "AD", "원딜": "AD",
    "SUP": "SUP", "SPT": "SUP", "SUPPORT": "SUP", "서폿": "SUP", "서포터": "SUP",
    "ALL": "ALL", "올": "ALL", "올라인": "ALL",
}

TIER_RE = re.compile(r"^(GM|[IBSGPEDMC])(\d+)?[▼▲↓↑]?$", re.I)
TIER_RANK = {"I": 0, "B": 1, "S": 2, "G": 3, "P": 4, "E": 5, "D": 6, "M": 7, "GM": 8, "C": 9}
TIER_EMOJI = {"I": "⬛", "B": "🟫", "S": "⬜", "G": "🟨", "P": "🟦", "E": "🟩",
              "D": "💎", "M": "🟪", "GM": "🟥", "C": "🏆"}

ALLOW_USERS = discord.AllowedMentions(users=True, roles=False, everyone=False)
ALLOW_ROLES = discord.AllowedMentions(users=True, roles=True, everyone=False)


def now() -> datetime:
    return datetime.now(KST)


def hhmm(t: str) -> int:
    h, m = t.split(":")
    return int(h) * 60 + int(m)


# =====================================================================
#  DB
# =====================================================================
class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        c = self.conn
        c.execute("CREATE TABLE IF NOT EXISTS records (user_id INTEGER PRIMARY KEY,"
                  " wins INTEGER NOT NULL DEFAULT 0, losses INTEGER NOT NULL DEFAULT 0)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS recruitment_sessions (
                session_id INTEGER PRIMARY KEY,
                data TEXT NOT NULL
            )
        """)
        legacy = c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sessions'").fetchone()
        if legacy:
            migrated = []
            for row in c.execute("SELECT data FROM sessions"):
                session = json.loads(row["data"])
                session["session_id"] = session["message_id"]
                migrated.append((session["session_id"], json.dumps(session, ensure_ascii=False)))
            with c:
                c.executemany("INSERT OR IGNORE INTO recruitment_sessions VALUES(?,?)", migrated)
                c.execute("DELETE FROM sessions")

        c.execute("CREATE TABLE IF NOT EXISTS cancel_log (id INTEGER PRIMARY KEY AUTOINCREMENT,"
                  " guild_id INTEGER, user_id INTEGER, name TEXT, channel_id INTEGER,"
                  " at TEXT, count_at INTEGER, note TEXT)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS match_results (
                message_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                finished_at INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                won INTEGER NOT NULL,
                PRIMARY KEY (message_id, user_id)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS match_results_period ON match_results(guild_id, finished_at)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS weekly_reports (
                channel_id INTEGER NOT NULL,
                week_start TEXT NOT NULL,
                PRIMARY KEY (channel_id, week_start)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS daily_cancel_reports (
                channel_id INTEGER NOT NULL,
                report_date TEXT NOT NULL,
                PRIMARY KEY (channel_id, report_date)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS cancel_log_period ON cancel_log(guild_id, at)")
        c.commit()

    def record(self, uid: int) -> tuple[int, int]:
        r = self.conn.execute("SELECT wins, losses FROM records WHERE user_id=?", (uid,)).fetchone()
        return (r["wins"], r["losses"]) if r else (0, 0)

    def set_record(self, uid: int, w: int, l: int):
        self.conn.execute("INSERT INTO records(user_id, wins, losses) VALUES(?,?,?) "
                          "ON CONFLICT(user_id) DO UPDATE SET wins=excluded.wins, losses=excluded.losses",
                          (uid, w, l))
        self.conn.commit()

    def finish_match(self, session: dict, winner: int):
        updates = [
            (uid, int(team == winner), int(team != winner))
            for team in (1, 2)
            for uid in session[f"team{team}"]
        ]
        finished_at = int(now().timestamp())
        with self.conn:
            self.conn.executemany(
                "INSERT INTO match_results(message_id, guild_id, finished_at, user_id, won) VALUES(?,?,?,?,?)",
                [(session["message_id"], session["guild_id"], finished_at, uid, win)
                 for uid, win, _ in updates],
            )
            self.conn.executemany(
                "INSERT INTO records(user_id, wins, losses) VALUES(?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET wins=wins+excluded.wins, losses=losses+excluded.losses",
                updates,
            )
            self.conn.execute("UPDATE recruitment_sessions SET data=? WHERE session_id=?",
                              (json.dumps(session, ensure_ascii=False), session["session_id"]))

    def period_records(self, guild_id: int, start: date, end: date):
        start_at = datetime.combine(start, clock_time.min, KST).timestamp()
        end_at = datetime.combine(end, clock_time.min, KST).timestamp()
        return self.conn.execute(
            "SELECT user_id, COUNT(*) AS games, SUM(won) AS wins "
            "FROM match_results WHERE guild_id=? AND finished_at>=? AND finished_at<? "
            "GROUP BY user_id",
            (guild_id, start_at, end_at),
        ).fetchall()

    def weekly_report_posted(self, channel_id: int, start: date) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM weekly_reports WHERE channel_id=? AND week_start=?",
            (channel_id, start.isoformat()),
        ).fetchone() is not None

    def mark_weekly_report_posted(self, channel_id: int, start: date):
        with self.conn:
            self.conn.execute(
                "INSERT INTO weekly_reports(channel_id, week_start) VALUES(?,?)",
                (channel_id, start.isoformat()),
            )

    def ranking(self, by: str, limit: int = 10):
        if by == "rate":
            return self.conn.execute(
                "SELECT * FROM records WHERE wins+losses>=? "
                "ORDER BY wins*1.0/(wins+losses) DESC, wins DESC LIMIT ?",
                (config.RANK_MIN_GAMES, limit)).fetchall()
        return self.conn.execute(
            "SELECT * FROM records WHERE wins+losses>0 "
            "ORDER BY wins DESC, wins*1.0/(wins+losses) DESC LIMIT ?", (limit,)).fetchall()

    def save_session(self, s: dict):
        self.conn.execute("INSERT OR REPLACE INTO recruitment_sessions(session_id, data) VALUES(?,?)",
                          (s["session_id"], json.dumps(s, ensure_ascii=False)))
        self.conn.commit()

    def delete_session(self, sid: int):
        self.conn.execute("DELETE FROM recruitment_sessions WHERE session_id=?", (sid,))
        self.conn.commit()

    def load_sessions(self) -> dict[int, dict]:
        return {r["session_id"]: json.loads(r["data"])
                for r in self.conn.execute("SELECT * FROM recruitment_sessions")}

    def add_cancel(self, guild_id, user_id, name, channel_id, count_at, note, *, canceled_at):
        self.conn.execute("INSERT INTO cancel_log(guild_id,user_id,name,channel_id,at,count_at,note) "
                          "VALUES(?,?,?,?,?,?,?)",
                          (guild_id, user_id, name, channel_id, canceled_at.strftime("%Y-%m-%d %H:%M"),
                           count_at, note))
        self.conn.commit()

    def cancels(self, guild_id, user_id=None, limit=20):
        if user_id:
            return self.conn.execute("SELECT * FROM cancel_log WHERE guild_id=? AND user_id=? "
                                     "ORDER BY id DESC LIMIT ?", (guild_id, user_id, limit)).fetchall()
        return self.conn.execute("SELECT * FROM cancel_log WHERE guild_id=? ORDER BY id DESC LIMIT ?",
                                 (guild_id, limit)).fetchall()

    def period_cancels(self, guild_id: int, start: datetime, end: datetime):
        return self.conn.execute(
            "SELECT * FROM cancel_log WHERE guild_id=? AND at>=? AND at<? ORDER BY at, id",
            (guild_id, start.strftime("%Y-%m-%d %H:%M"), end.strftime("%Y-%m-%d %H:%M")),
        ).fetchall()

    def daily_cancel_report_posted(self, channel_id: int, report_date: date) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM daily_cancel_reports WHERE channel_id=? AND report_date=?",
            (channel_id, report_date.isoformat()),
        ).fetchone() is not None

    def mark_daily_cancel_report_posted(self, channel_id: int, report_date: date):
        with self.conn:
            self.conn.execute(
                "INSERT INTO daily_cancel_reports(channel_id, report_date) VALUES(?,?)",
                (channel_id, report_date.isoformat()),
            )


if config.os.getenv("RAILWAY_ENVIRONMENT_ID") and not config.volume:
    raise SystemExit("❌ Railway 서비스에 데이터 보존용 Volume을 연결해 주세요.")
db = DB(config.DB_PATH)
sessions: dict[int, dict] = {}
locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


def save(s: dict):
    db.save_session(s)
    sessions[s["session_id"]] = s


def end_session(sid: int):
    db.delete_session(sid)
    sessions.pop(sid, None)


def channel_sessions(channel_id: int) -> list[dict]:
    return [s for s in sessions.values() if s["channel_id"] == channel_id]


def session_for_message(channel_id: int, message_id: int) -> Optional[dict]:
    return next((s for s in channel_sessions(channel_id) if s["message_id"] == message_id), None)


# =====================================================================
#  닉네임 / 티어 파싱   (형식: 롤닉네임#태그/티어/라인)
# =====================================================================
def parse_nick(name: str):
    parts = [p.strip() for p in name.split("/")]
    riot, rest, tier = parts[0], parts[1:], ""
    if rest and TIER_RE.match(rest[0].replace(" ", "")):
        tier, rest = rest[0], rest[1:]
    lines = "/".join(rest).strip()
    main = None
    for tok in re.split(r"[\s,./·+()]+", lines):
        key = LINE_ALIASES.get(tok.upper()) or LINE_ALIASES.get(tok)
        if key:
            main = key
            break
    return riot, tier, lines, main


def tier_code(tier: str) -> Optional[str]:
    m = TIER_RE.match(tier.replace(" ", ""))
    return m.group(1).upper() if m else None


def tier_key(tier: str):
    m = TIER_RE.match(tier.replace(" ", ""))
    if not m:
        return (-1, 0)
    code, num = m.group(1).upper(), m.group(2)
    num = int(num) if num else 0
    if code in ("M", "GM", "C"):
        return (TIER_RANK[code], num)                # 마스터 이상: LP 높은 순
    return (TIER_RANK[code], (5 - num) if num else 0)  # 다이아 이하: 1단계가 높음


def tier_emoji(tier: str) -> str:
    return TIER_EMOJI.get(tier_code(tier) or "", "❔")


def dwidth(text: str) -> int:
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in text)


def pad(text: str, width: int) -> str:
    return text + " " * max(0, width - dwidth(text))


def norm(text: str) -> str:
    return re.sub(r"\s+", "", text).lower()


# =====================================================================
#  채널 / 권한
# =====================================================================
def find_conf(channel_id: int):
    for gkey, g in GROUPS.items():
        for idx, conf in enumerate(g["channels"]):
            if conf["text_id"] and conf["text_id"] == channel_id:
                return gkey, idx, conf
    return None


def has_role(member, names) -> bool:
    if not isinstance(member, discord.Member):
        return False
    return any(r.name in names for r in member.roles)


def is_staff(member) -> bool:
    return has_role(member, config.STAFF_ROLE_NAMES)


def in_cancel_window(canceled_at: datetime) -> bool:
    cur = canceled_at.hour * 60 + canceled_at.minute
    start, end = hhmm(config.CANCEL_RECORD_START), hhmm(config.CANCEL_RECORD_END)
    return (start <= cur or cur < end) if start > end else (start <= cur < end)


def parse_time(text: str) -> Optional[str]:
    """'22:00', '22시', '22시30분', '오후10시', '오후 10:30' 등 → 'HH:MM'  ('10시'는 오전 10시)"""
    t = text.strip().replace(" ", "")
    m = re.fullmatch(r"(오전|오후|AM|PM|am|pm)?(\d{1,2})(?:[:시](\d{1,2})?분?)?", t)
    if not m:
        return None
    ap, h, mi = m.group(1), int(m.group(2)), int(m.group(3) or 0)
    if ap and not 1 <= h <= 12:
        return None
    if ap in ("오후", "PM", "pm") and h < 12:
        h += 12
    elif ap in ("오전", "AM", "am") and h == 12:
        h = 0
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        return None
    return f"{h:02d}:{mi:02d}"


# =====================================================================
#  표시용 헬퍼
# =====================================================================
def name_of(guild: discord.Guild, entry: dict) -> str:
    m = guild.get_member(entry["id"])
    if m:
        entry["name"] = m.display_name
    return entry["name"]


def record_text(uid: int) -> str:
    w, l = db.record(uid)
    g = w + l
    return f"{g}전 {round(w * 100 / g)}%" if g else "0전 -"


async def remove_earned_newbie_role(member: discord.Member, role: discord.Role) -> bool:
    """전적에 기록된 누적 내전 판수가 기준에 도달한 멤버의 뉴비 역할을 회수한다."""
    wins, losses = db.record(member.id)
    if wins + losses < config.NEWBIE_GAMES:
        return False
    await member.remove_roles(role, reason="내전 판수 충족 - 뉴비 정리")
    return True


async def clean_match_newbies(guild: discord.Guild, players: list[int]) -> list[int]:
    role = discord.utils.get(guild.roles, name=config.NEWBIE_ROLE_NAME)
    if not role:
        return []

    failed = []
    for uid in players:
        member = guild.get_member(uid)
        if member is None or role not in member.roles:
            continue
        try:
            await remove_earned_newbie_role(member, role)
        except discord.HTTPException:
            logger.exception("뉴비 역할 자동 회수 실패 (guild=%s, user=%s)", guild.id, uid)
            failed.append(uid)
    return failed


def line_counts(guild, entries) -> dict:
    cnt = {k: 0 for k in LINES}
    for e in entries:
        main = parse_nick(name_of(guild, e))[3]
        if main:
            cnt[main] += 1
    return cnt


def title_of(s: dict) -> str:
    g = GROUPS[s["group"]]
    if s["group"] == "single":
        return f"{g['emoji']} 단판 내전 정보"
    return f"{g['emoji']} {s['label']} | {KIND_NAME[s['kind']]} {FORMAT_NAME[s['format']]} 내전 정보"


def gather_time(s: dict) -> str:
    t = datetime.strptime(s["reserve_time"], "%H:%M") - timedelta(minutes=10)
    return t.strftime("%H:%M")


def guide_text(s: dict) -> str:
    keys = ["GUIDE_CANCEL", "GUIDE_RATING_GAP", "GUIDE_8", "GUIDE_LATE"]
    values = dict(start=config.CANCEL_RECORD_START, end=config.CANCEL_RECORD_END,
                  count=config.CANCEL_WARN_COUNT)
    if s["group"] == "mobashi":
        keys.insert(2, "GUIDE_M700")
    if s["kind"] == "reserve":
        keys.insert(0, "GUIDE_RESERVE")
        values["gather"] = gather_time(s)
    templates = (getattr(config, key, "").strip() for key in keys)
    return "\n".join(f"• {template.format(**values)}" for template in templates if template)


def entry_line(guild, i: int, e: dict) -> str:
    name = name_of(guild, e)
    tier = parse_nick(name)[1]
    return (f"`{i:>2}.` {tier_emoji(tier)} **{discord.utils.escape_markdown(name)}**"
            f" · {record_text(e['id'])} · ⏰ {e['time']}")


# ---------------------------------------------------------------------
#  1) 모집글
# ---------------------------------------------------------------------
def recruit_embed(guild, s: dict) -> discord.Embed:
    p, w = s["participants"], s["waitlist"]
    n, mx = len(p), config.MAX_PLAYERS
    desc = [f"모집자 <@{s['host_id']}>"]
    if s["kind"] == "reserve":
        desc.append(f"시작 시간 : **{s['reserve_time']}** (집합 **{gather_time(s)}**)")
    desc.append(f"진행 방식 : **{FORMAT_NAME[s['format']]}**")
    desc.append(f"최대 **{mx}명** 참여 가능 · 대기 최대 **{config.MAX_WAITING}명**")
    desc.append(f"{'▰' * n}{'▱' * (mx - n)}  **{n}/{mx}** · {mx - n}자리 남음")

    player_lines = [entry_line(guild, i, e) for i, e in enumerate(p, 1)]
    participants = "\n".join(player_lines) or "아직 아무도 없습니다"
    desc.append(f"\n**🟢 내전 참가 ({n}/{mx})**\n{participants}")
    emb = discord.Embed(title=title_of(s), description="\n".join(desc), color=GROUPS[s["group"]]["color"])
    emb.add_field(name=f"🕒 대기열 ({len(w)}/{config.MAX_WAITING})",
                  value="\n".join(entry_line(guild, i, e) for i, e in enumerate(w, 1)) or "대기 인원이 없습니다",
                  inline=False)
    cnt = line_counts(guild, p)
    emb.add_field(name="📊 주라인 신청 현황",
                  value=" · ".join(f"{k} **{v}**" for k, v in cnt.items()), inline=False)
    cancels = s.get("cancels", [])
    if cancels:
        lines = [f"• {discord.utils.escape_markdown(c['name'])} · ⏰ {c['time']} · {c['note']}"
                 for c in cancels[-8:]]
        if len(cancels) > 8:
            lines.insert(0, f"(최근 8건 / 총 {len(cancels)}건)")
        emb.add_field(name=f"📝 취소 기록 ({len(cancels)})", value="\n".join(lines)[:1024], inline=False)
    guide = guide_text(s)
    if guide:
        emb.add_field(name="💌 안내", value=guide, inline=False)
    emb.set_footer(text="닉네임 형식 롤닉네임#태그/티어/라인 · 예) 완댕이#명명펀치/M350/MID.TOP")
    return emb


def make_view(*buttons) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    for b in buttons:
        if b:
            v.add_item(b)
    return v


def btn(s, action, label, style, emoji=None, row=None):
    return discord.ui.Button(label=label, style=style, emoji=emoji, row=row,
                             custom_id=f"nj:{action}:{s['channel_id']}")


def recruit_view(s: dict) -> discord.ui.View:
    return make_view(
        btn(s, "join", "내전 참가 등록", discord.ButtonStyle.success, "🟢", 0),
        btn(s, "wait", "대기열 등록", discord.ButtonStyle.secondary, "⏰", 0),
        btn(s, "cancel", "참여/대기 취소", discord.ButtonStyle.danger, "❌", 0),
        btn(s, "summon", "내전 참가자 소집", discord.ButtonStyle.secondary, "📣", 1) if s["kind"] == "mobashi" else None,
        
    )


# ---------------------------------------------------------------------
#  2) 참여자 티어 정보 (10명 마감 후)
# ---------------------------------------------------------------------
def tier_table(guild, entries) -> list[list[str]]:
    rows = []
    for e in entries:
        riot, tier, lines, _ = parse_nick(name_of(guild, e))
        games, rate = record_text(e["id"]).split()
        rows.append([riot.replace(chr(96), "ˋ"), tier or "-", lines.replace(chr(96), "ˋ") or "-",
                     f"{games} 승률 {rate if rate != '-' else '—'}"])
    return rows


def tier_embed(guild, s: dict) -> discord.Embed:
    players = sorted(s["participants"], key=lambda e: tier_key(parse_nick(name_of(guild, e))[1]), reverse=True)
    waiting = s["waitlist"]
    rows_p, rows_w = tier_table(guild, players), tier_table(guild, waiting)
    all_rows = rows_p + rows_w
    widths = [max(dwidth(r[i]) for r in all_rows) + 3 for i in range(3)] if all_rows else [0, 0, 0]

    def fmt(r):
        return "".join(pad(r[i], widths[i]) for i in range(3)) + r[3]

    table = "\n".join(fmt(r) for r in rows_p)
    if rows_w:
        table += "\n\n" + "\n".join(fmt(r) for r in rows_w)

    cnt = line_counts(guild, players)
    sub = f"{len(players)}명 · 티어 높은 순 · 주 라인 " + " · ".join(f"{k} {v}" for k, v in cnt.items() if v)
    if waiting:
        sub += f"\n(빈 줄 아래 {len(waiting)}명은 대기자)"
    guide = "관리자 전용 · 위 표에서 닉네임 줄을 복사해 팀별로 붙여넣으면 돼요."
    if s["format"] == "bo1":
        guide += "\n전적 미반영 내전이면 **기록 없이 종료**를 눌러 모집을 닫아 주세요."

    emb = discord.Embed(title=f"⭐ 참여자 티어 정보 · {s['label']}",
                        description=f"{sub}\n```\n{table}\n```\n{guide}", color=0xF1C40F)
    emb.set_footer(text=f"{KIND_NAME[s['kind']]} {FORMAT_NAME[s['format']]}")
    return emb


def tier_view(s: dict) -> discord.ui.View:
    return make_view(
        btn(s, "wait", "대기열 등록", discord.ButtonStyle.secondary, "⏰", 2),
        btn(s, "cancel", "참여/대기 취소", discord.ButtonStyle.danger, "❌", 2),
        btn(s, "start", "팀 입력하고 경기 시작", discord.ButtonStyle.success, "⚔️", 0),
        btn(s, "norecord", "기록 없이 종료", discord.ButtonStyle.secondary, "🔒", 0) if s["format"] == "bo1" else None,
        btn(s, "summon", "내전 참가자 소집", discord.ButtonStyle.secondary, "📣", 1) if s["kind"] == "mobashi" else None,
        btn(s, "calladmin", "관리자 호출", discord.ButtonStyle.primary, "🚨", 1) if s["group"] in ("low", "high") else None,
    )


# ---------------------------------------------------------------------
#  3) 경기 진행 중 / 4) 최종 결과
# ---------------------------------------------------------------------
def team_lines(guild, s, ids) -> str:
    roster = {e["id"]: e for e in s["participants"] + s["waitlist"]}
    return "\n".join(f"• {discord.utils.escape_markdown(name_of(guild, roster[i]))}" for i in ids if i in roster)


def playing_embed(guild, s: dict) -> discord.Embed:
    emb = discord.Embed(title="⚔️ 내전 경기 진행 중",
                        description=f"{s['label']} · {FORMAT_NAME[s['format']]}\n"
                                    "경기가 끝나면 결과를 요청하고, 확인된 승리 팀의 버튼을 눌러주세요 · 관리진 전용",
                        color=0xF1C40F)
    emb.add_field(name="🔵 1팀", value=team_lines(guild, s, s["team1"]), inline=False)
    emb.add_field(name="🔴 2팀", value=team_lines(guild, s, s["team2"]), inline=False)
    return emb


def playing_view(s: dict) -> discord.ui.View:
    return make_view(
        btn(s, "norecord", "전적 기록 없이 종료", discord.ButtonStyle.secondary, "🔒"),
        btn(s, "askwinner", "경기 종료 · 결과 요청", discord.ButtonStyle.secondary, "📣"),
        btn(s, "win1", "1팀 최종 승리", discord.ButtonStyle.primary, "🔵"),
        btn(s, "win2", "2팀 최종 승리", discord.ButtonStyle.danger, "🔴"),
    )


def result_embed(guild, s: dict, winner: int) -> discord.Embed:
    dot = "🔵" if winner == 1 else "🔴"
    emb = discord.Embed(title="🏆 내전 매치 최종 종료!",
                        description=f"🎉{dot} **{winner}팀 최종 승리!**\n승리팀 **+1승** · 패배팀 **+1패** 반영 완료",
                        color=0x2ECC71)
    emb.add_field(name="🔵 1팀" + (" 👑" if winner == 1 else ""), value=team_lines(guild, s, s["team1"]), inline=False)
    emb.add_field(name="🔴 2팀" + (" 👑" if winner == 2 else ""), value=team_lines(guild, s, s["team2"]), inline=False)
    emb.add_field(name="참가자 명단", value=" ".join(f"<@{i}>" for i in s["team1"] + s["team2"]), inline=False)
    return emb


def current_message(guild, s: dict):
    if s["stage"] == "recruit":
        return recruit_embed(guild, s), recruit_view(s)
    if s["stage"] == "full":
        return tier_embed(guild, s), tier_view(s)
    if s["stage"] == "finished":
        return result_embed(guild, s, s["winner"]), None
    return playing_embed(guild, s), playing_view(s)


# =====================================================================
#  봇
# =====================================================================
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)


async def delete_msg(channel, mid):
    if not mid:
        return
    try:
        await channel.get_partial_message(mid).delete()
    except discord.HTTPException:
        pass


def recruit_status_channel(guild) -> Optional[discord.TextChannel]:
    configured = guild.get_channel(config.STATUS_CHANNEL_ID) if config.STATUS_CHANNEL_ID else None
    if isinstance(configured, discord.TextChannel):
        return configured
    candidates = [
        channel for channel in guild.text_channels
        if re.sub(r"[^가-힣]", "", channel.name) == "내전모집현황"
    ]
    return candidates[0] if len(candidates) == 1 else None


async def post_status(guild, s: dict) -> bool:
    if s.get("status_message_id"):
        return True
    channel = recruit_status_channel(guild)
    if not channel:
        logger.warning("모집 현황 채널을 찾지 못했어요 (guild=%s, STATUS_CHANNEL_ID=%s)",
                       guild.id, config.STATUS_CHANNEL_ID)
        return False
    if s["group"] == "single":
        text = f"<#{s['channel_id']}> 단판 내전 인원 모집 중"
    elif s["kind"] == "reserve":
        text = f"<#{s['channel_id']}> {s['reserve_time']} 예약 {FORMAT_NAME[s['format']]} 내전 인원 모집 중"
    else:
        text = f"<#{s['channel_id']}> 모바시 {FORMAT_NAME[s['format']]} 내전 인원 모집 중"
    try:
        msg = await channel.send(text)
    except discord.HTTPException:
        logger.exception("모집 현황 알림 게시 실패 (channel=%s)", channel.id)
        return False
    s.update(status_message_id=msg.id, status_channel_id=channel.id, status_text=text)
    save(s)
    return True


async def mark_status_done(guild, s: dict, label="✅ 모집 완료"):
    if not s.get("status_message_id"):
        return
    channel_id = s.get("status_channel_id")
    channel = guild.get_channel(channel_id) if channel_id else recruit_status_channel(guild)
    if not isinstance(channel, discord.TextChannel):
        return
    text = s["status_text"]
    try:
        await channel.get_partial_message(s["status_message_id"]).edit(
            content=f"~~{text}~~ {label}" if label else text
        )
    except discord.HTTPException:
        logger.exception("모집 현황 알림 수정 실패 (channel=%s)", channel.id)


def recruitment_roles(guild, group: str):
    roles = []
    for name in GROUPS[group]["roles"]:
        role = discord.utils.get(guild.roles, name=name)
        if name == "플래티넘":
            role = guild.get_role(config.PLATINUM_ROLE_ID) if config.PLATINUM_ROLE_ID else role
            if role is None:
                role = discord.utils.find(
                    lambda r: re.sub(r"[^a-z가-힣]", "", r.name.casefold())
                    in {"플래티넘", "플레티넘", "플래", "플레", "platinum"},
                    guild.roles,
                )
        if role is not None:
            roles.append(role)
        else:
            logger.warning("모집 멘션 역할을 찾지 못했어요 (guild=%s, role=%s)", guild.id, name)
    return roles


async def open_recruit(channel: discord.TextChannel, gkey: str, label: str, kind: str, fmt: str,
                       reserve_time: Optional[str], host_id: int, ping: bool = True):
    s = {
        "channel_id": channel.id, "guild_id": channel.guild.id, "group": gkey, "label": label,
        "kind": kind, "format": fmt, "reserve_time": reserve_time, "host_id": host_id,
        "stage": "recruit", "participants": [], "waitlist": [], "team1": [], "team2": [],
        "message_id": None, "status_message_id": None, "status_text": "", "last_call": 0,
        "cancels": [],
    }
    content = "아래 버튼을 눌러 내전에 참여해주세요!"
    if ping and GROUPS[gkey]["roles"]:
        roles = recruitment_roles(channel.guild, gkey)
        mentions = " ".join(r.mention for r in roles if r)
        if mentions:
            content = f"{mentions}\n{content}"
    msg = await channel.send(content=content, embed=recruit_embed(channel.guild, s),
                             view=recruit_view(s), allowed_mentions=ALLOW_ROLES)
    s["session_id"] = s["message_id"] = msg.id
    save(s)
    await post_status(channel.guild, s)
    return s


async def open_next(guild: discord.Guild, s: dict, host_id: int):
    """경기 시작 시 같은 그룹의 다음 빈 채널(A→E 순)에 동일한 모집글 자동 생성"""
    if s["group"] != "mobashi":
        return None
    channels = GROUPS["mobashi"]["channels"]
    index = next((i for i, conf in enumerate(channels) if conf["text_id"] == s["channel_id"]), None)
    if index is None:
        return None
    for conf in channels[index + 1:] + channels[:index]:
        cid = conf["text_id"]
        found = find_conf(cid)
        if not found or found[0] != "mobashi":
            continue
        ch = guild.get_channel(cid)
        if not isinstance(ch, discord.TextChannel):
            continue
        async with locks[cid]:
            if not channel_sessions(cid):
                return await open_recruit(ch, "mobashi", conf["label"], "mobashi",
                                          s["format"], None, host_id)
    return None


async def replace_post(channel, s: dict, updated: dict):
    emb, view = current_message(channel.guild, updated)
    msg = await channel.send(embed=emb, view=view)
    updated["message_id"] = msg.id
    save(updated)
    await delete_msg(channel, s["message_id"])


async def refresh(channel, s: dict, message: Optional[discord.Message] = None):
    save(s)
    if s["stage"] in ("recruit", "full"):
        stage = "full" if len(s["participants"]) == config.MAX_PLAYERS else "recruit"
        if s["stage"] != stage:
            await replace_post(channel, s, {**s, "stage": stage})
            await mark_status_done(channel.guild, sessions[s["session_id"]],
                                   "✅ 모집 완료" if stage == "full" else "")
            return
    emb, view = current_message(channel.guild, s)
    try:
        if message:
            await message.edit(embed=emb, view=view)
        else:
            await channel.get_partial_message(s["message_id"]).edit(embed=emb, view=view)
    except discord.NotFound:
        msg = await channel.send(embed=emb, view=view)
        s["message_id"] = msg.id
    save(s)


def log_channel(guild: discord.Guild):
    """취소 기록용 관리자 채널: ID 우선, 없으면 이름으로 찾기"""
    ch = guild.get_channel(config.CANCEL_LOG_CHANNEL_ID) if config.CANCEL_LOG_CHANNEL_ID else None
    if not ch and config.CANCEL_LOG_CHANNEL_NAME:
        ch = discord.utils.get(guild.text_channels, name=config.CANCEL_LOG_CHANNEL_NAME)
    return ch


async def remove_player(guild, channel, s: dict, uid: int, forced: bool = False) -> Optional[str]:
    """참가자/대기자 제거 + 대기자 자동 승급 + 취소 명단 기록. 제거된 위치('참가'/'대기') 반환"""
    for e in s["participants"]:
        if e["id"] == uid:
            count_before = len(s["participants"])
            s["participants"].remove(e)
            canceled_at = now()
            if not forced and count_before >= config.CANCEL_WARN_COUNT and in_cancel_window(canceled_at):
                note = (f"{config.CANCEL_RECORD_START}~{config.CANCEL_RECORD_END} 전 · "
                        f"{config.CANCEL_WARN_COUNT}명 이상 모집 후 참가 취소")
                name = name_of(guild, e)
                db.add_cancel(guild.id, uid, name, channel.id, count_before, note, canceled_at=canceled_at)
                # 모집글에 표시할 취소 기록
                s.setdefault("cancels", []).append(
                    {"id": uid, "name": name, "time": canceled_at.strftime("%H:%M"), "note": note})
                # 관리자 채널 자동 기록
                log = log_channel(guild)
                if log:
                    try:
                        await log.send(f"📝 **취소 명단 기록** · <@{uid}> ({discord.utils.escape_markdown(name)})\n"
                                       f"{channel.mention} · {canceled_at.strftime('%m/%d %H:%M')} · "
                                       f"취소 당시 {count_before}명 · {note}", allowed_mentions=discord.AllowedMentions.none())
                    except discord.HTTPException:
                        pass
            if s["waitlist"]:
                promoted = s["waitlist"].pop(0)
                s["participants"].append(promoted)
                try:
                    await channel.send(f"🔼 <@{promoted['id']}> 님이 대기열에서 **내전 참가**로 자동 이동했어요!",
                                       allowed_mentions=ALLOW_USERS, delete_after=60)
                except discord.HTTPException:
                    pass
            return "참가"
    for e in s["waitlist"]:
        if e["id"] == uid:
            s["waitlist"].remove(e)
            return "대기"
    return None


# =====================================================================
#  모달
# =====================================================================
class SummonModal(discord.ui.Modal, title="내전 참가자 소집"):
    def __init__(self, channel_id: int, session_id: int, message_id: int):
        super().__init__()
        self.channel_id = channel_id
        self.session_id = session_id
        self.message_id = message_id
        default = (now() + timedelta(minutes=config.SUMMON_DEFAULT_MINUTES)).strftime("%H시 %M분")
        self.when = discord.ui.TextInput(label="모일 시간", default=default, max_length=20)
        self.add_item(self.when)

    async def on_submit(self, inter: discord.Interaction):
        when = parse_time(self.when.value)
        if not when:
            return await eph(inter, "모일 시간을 HH:MM 형식으로 입력해 주세요.")
        if not is_staff(inter.user):
            return await eph(inter, "관리진만 소집할 수 있어요.")
        await inter.response.defer(ephemeral=True)
        async with locks[self.channel_id]:
            s = sessions.get(self.session_id)
            if (not s or s["message_id"] != self.message_id or s["kind"] != "mobashi"
                    or s["stage"] not in ("recruit", "full") or not s["participants"]):
                return await eph(inter, "현재 모집글에서 다시 소집해 주세요.")
            conf = find_conf(self.channel_id)
            lobby = f" (<#{conf[2]['lobby_id']}>)" if conf and conf[2].get("lobby_id") else ""
            mentions = " ".join(f"<@{e['id']}>" for e in s["participants"])
            hour, minute = when.split(":")
            await inter.channel.send(f"{mentions}\n📢 **{hour}시 {minute}분**까지 내전 대기실{lobby}로 모여주세요!",
                                     allowed_mentions=ALLOW_USERS)
        await eph(inter, "소집 메시지를 보냈어요.")


def match_team(guild, text: str, roster: list[dict]):
    eligible = {entry["id"] for entry in roster}
    names = defaultdict(list)
    table_rows = defaultdict(list)
    for entry, row in zip(roster, tier_table(guild, roster)):
        uid = entry["id"]
        riot = parse_nick(name_of(guild, entry))[0]
        for name in {norm(riot), norm(row[0])}:
            names[name].append(uid)
        table_rows[norm("".join(row[:3]))].append(uid)
    ids, bad = [], []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line == "```":
            continue
        marker = re.search(r"\|\s*(\d+)$", line) or re.fullmatch(r"<@!?(\d+)>", line)
        if marker:
            uid = int(marker.group(1))
            matches = [uid] if uid in eligible else []
        else:
            matches = names[norm(line)]
            if not matches:
                stats = re.search(r"\s+\d+전\s+승률\s+(?:\d+%|—)$", line)
                if stats:
                    matches = table_rows[norm(line[:stats.start()])]
            if not matches:
                # 닉네임·태그 전체가 경계까지 일치해야 하며, 태그 일부 일치는 제외한다.
                normalized = line.replace(chr(96), "ˋ").casefold()
                matches = list({
                    uid
                    for name, users in names.items() if "#" in name
                    if re.search(r"(?<![\w#])" + r"\s*".join(re.escape(char) for char in name) + r"(?![\w#])", normalized)
                    for uid in users
                })
        if len(matches) == 1:
            ids.append(matches[0])
        else:
            bad.append(line)
    return ids, bad


class TeamModal(discord.ui.Modal, title="팀 입력하고 경기 시작"):
    team1 = discord.ui.TextInput(label="1팀 (5명)", style=discord.TextStyle.paragraph, max_length=1500,
                                 placeholder="티어 정보 표에서 1팀 5명의 줄을 복사해 붙여넣으세요")
    team2 = discord.ui.TextInput(label="2팀 (5명)", style=discord.TextStyle.paragraph, max_length=1500,
                                 placeholder="티어 정보 표에서 2팀 5명의 줄을 복사해 붙여넣으세요")

    def __init__(self, channel_id: int, session_id: int, message_id: int):
        super().__init__()
        self.channel_id = channel_id
        self.session_id = session_id
        self.message_id = message_id

    async def on_submit(self, inter: discord.Interaction):
        guild, channel = inter.guild, inter.channel
        if not is_staff(inter.user):
            return await eph(inter, "관리진만 경기를 시작할 수 있어요.")
        await inter.response.defer(ephemeral=True)
        async with locks[self.channel_id]:
            s = sessions.get(self.session_id)
            if not s or s["stage"] != "full" or s["message_id"] != self.message_id:
                return await eph(inter, "현재 티어 정보 게시글에서 다시 팀을 입력해 주세요.")
            roster = s["participants"]
            t1, bad1 = match_team(guild, self.team1.value, roster)
            t2, bad2 = match_team(guild, self.team2.value, roster)
            errors = []
            if bad1:
                errors.append("1팀에서 명단에 없는 줄: " + ", ".join(f"`{b}`" for b in bad1))
            if bad2:
                errors.append("2팀에서 명단에 없는 줄: " + ", ".join(f"`{b}`" for b in bad2))
            if len(set(t1)) != len(t1) or len(set(t2)) != len(t2):
                errors.append("같은 팀에 같은 사람이 두 번 들어갔어요.")
            if set(t1) & set(t2):
                errors.append("1팀과 2팀에 동시에 들어간 사람이 있어요.")
            if len(t1) != 5 or len(t2) != 5:
                errors.append(f"각 팀은 5명이어야 해요. (현재 1팀 {len(t1)}명 / 2팀 {len(t2)}명)")
            if errors:
                return await eph(inter, ("⚠️ 팀 입력을 확인해 주세요.\n" + "\n".join(errors))[:1900])
            await replace_post(channel, s, {**s, "team1": t1, "team2": t2, "stage": "playing"})
        try:
            nxt = await open_next(guild, s, inter.user.id)
        except discord.HTTPException:
            return await eph(inter, "경기를 시작했어요. 다음 채널 모집글 게시에 실패했어요.")
        extra = f"\n다음 모집글을 <#{nxt['channel_id']}> 에 올렸어요." if nxt else ""
        if nxt and not nxt.get("status_message_id"):
            extra += "\n다음 모집의 현황 알림은 올리지 못했어요. 현황 채널 ID와 권한을 확인해 주세요."
        await inter.followup.send(f"⚔️ 경기를 시작했어요!{extra}", ephemeral=True)


# =====================================================================
#  버튼 처리
# =====================================================================
async def eph(inter: discord.Interaction, text: str):
    if inter.response.is_done():
        await inter.followup.send(text, ephemeral=True)
    else:
        await inter.response.send_message(text, ephemeral=True)


@bot.listen("on_interaction")
async def on_button(inter: discord.Interaction):
    try:
        await handle_button(inter)
    except (discord.HTTPException, sqlite3.Error):
        logger.exception("내전 버튼 처리 실패")
        await eph(inter, "내전 처리 중 오류가 발생했어요. /내전현황으로 최신 게시글을 확인해 주세요.")


async def handle_button(inter: discord.Interaction):
    if inter.type != discord.InteractionType.component:
        return
    cid = (inter.data or {}).get("custom_id", "")
    if not cid.startswith("nj:"):
        return
    match = re.fullmatch(r"nj:([a-z0-9]+):(\d+)", cid)
    if not match or int(match[2]) != inter.channel_id or inter.message is None:
        return
    action, ch_id = match[1], int(match[2])
    guild, channel, user = inter.guild, inter.channel, inter.user

    # 모달을 띄우는 버튼은 락 없이 바로 응답 (3초 제한)
    if action in ("summon", "start"):
        s = session_for_message(ch_id, inter.message.id)
        if not s:
            return await eph(inter, "현재 내전 게시글의 버튼을 사용해 주세요.")
        if not is_staff(user):
            return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
        if action == "summon":
            if s["kind"] != "mobashi":
                return await eph(inter, "참가자 소집은 모바시 내전에서만 사용할 수 있어요.")
            if not s["participants"]:
                return await eph(inter, "아직 참가자가 없어요.")
            return await inter.response.send_modal(SummonModal(ch_id, s["session_id"], inter.message.id))
        if s["stage"] != "full":
            return await eph(inter, "참가자 10명이 모인 뒤에 경기를 시작할 수 있어요.")
        return await inter.response.send_modal(TeamModal(ch_id, s["session_id"], inter.message.id))

    if not isinstance(channel, discord.TextChannel) or guild is None:
        return
    await inter.response.defer(ephemeral=True, thinking=True)
    async with locks[ch_id]:
        await apply_button(inter, guild, channel, user, ch_id, action)


async def apply_button(inter, guild, channel, user, ch_id, action):
    s = session_for_message(ch_id, inter.message.id)
    if not s:
        return await eph(inter, "현재 내전 게시글의 버튼을 사용해 주세요.")
    uid = user.id
    entry = {"id": uid, "name": user.display_name, "time": now().strftime("%H:%M")}
    in_p = any(e["id"] == uid for e in s["participants"])
    in_w = any(e["id"] == uid for e in s["waitlist"])

    if action == "join":
        if s["stage"] != "recruit":
            return await eph(inter, "모집이 마감되었어요.")
        if in_p:
            return await eph(inter, "이미 내전 참가 명단에 있어요.")
        if len(s["participants"]) >= config.MAX_PLAYERS:
            return await eph(inter, "참가 정원이 찼어요. **대기열 등록**을 이용해 주세요.")
        if in_w:
            s["waitlist"] = [e for e in s["waitlist"] if e["id"] != uid]
        s["participants"].append(entry)
        await refresh(channel, s, inter.message)
        await eph(inter, "신청 현황을 업데이트했어요.")

    elif action == "wait":
        if s["stage"] not in ("recruit", "full"):
            return await eph(inter, "모집이 마감되었어요.")
        if in_p:
            return await eph(inter, "이미 참가 명단에 있어요. 대기로 바꾸려면 먼저 취소해 주세요.")
        if in_w:
            return await eph(inter, "이미 대기열에 있어요.")
        if len(s["waitlist"]) >= config.MAX_WAITING:
            return await eph(inter, f"대기열이 가득 찼어요. (최대 {config.MAX_WAITING}명)")
        s["waitlist"].append(entry)
        await refresh(channel, s, inter.message)
        await eph(inter, "신청 현황을 업데이트했어요.")

    elif action == "cancel":
        if s["stage"] not in ("recruit", "full"):
            return await eph(inter, "경기 시작 후에는 관리자에게 문의해 주세요.")
        if not (in_p or in_w):
            return await eph(inter, "신청 내역이 없어요.")
        await remove_player(guild, channel, s, uid)
        await refresh(channel, s, inter.message)
        await eph(inter, "신청 현황을 업데이트했어요.")

    elif action == "calladmin":
        if s["group"] not in ("low", "high"):
            return await eph(inter, "관리자 호출은 저티어/고티어 내전에서만 사용할 수 있어요.")
        if s["stage"] != "full":
            return await eph(inter, "참가자 10명이 모이면 관리자를 호출할 수 있어요.")
        left = config.CALL_ADMIN_COOLDOWN - (time.time() - s.get("last_call", 0))
        if left > 0:
            return await eph(inter, f"방금 호출했어요. {int(left)}초 뒤에 다시 눌러 주세요.")
        roles = [discord.utils.get(guild.roles, name=n) for n in config.CALL_ADMIN_ROLE_NAMES]
        mentions = " ".join(r.mention for r in roles if r)
        gname = GROUPS[s["group"]]["name"]
        await channel.send(f"{mentions}\n🚨 **{gname} 내전 참여자 모집이 완료되었습니다!** "
                           f"(참가 {len(s['participants'])}/{config.MAX_PLAYERS} · 호출: {user.mention})",
                           allowed_mentions=ALLOW_ROLES)
        s["last_call"] = time.time()
        save(s)
        await eph(inter, "관리자를 호출했어요.")

    elif action == "norecord":
        if not is_staff(user):
            return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
        if not (s["stage"] == "playing" or (s["format"] == "bo1" and s["stage"] == "full")):
            return await eph(inter, "경기 진행 중 또는 단판 팀 편성 단계에서 사용할 수 있어요.")
        await inter.message.edit(embed=discord.Embed(
            title="🔒 전적 기록 없이 종료", color=0x95A5A6,
            description=f"{s['label']} {FORMAT_NAME[s['format']]} 내전이 전적 반영 없이 종료되었어요. (처리: {user.mention})"),
            view=None)
        end_session(s["session_id"])

        await eph(inter, "전적 반영 없이 종료했어요.")

    elif action == "askwinner":
        if not is_staff(user):
            return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
        if s["stage"] != "playing":
            return await eph(inter, "진행 중인 경기가 아니에요.")
        if s.get("winner_prompt_id"):
            return await eph(inter, "이미 승리 팀 작성을 안내했어요.")
        message = await channel.send(config.WINNER_REPORT_PROMPT, allowed_mentions=discord.AllowedMentions.none())
        s["winner_prompt_id"] = message.id
        save(s)
        await eph(inter, "이 채널에 승리 팀 작성을 안내했어요.")

    elif action in ("win1", "win2"):
        if not is_staff(user):
            return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
        if s["stage"] not in ("playing", "finished"):
            return await eph(inter, "진행 중인 경기가 아니에요.")
        if s["stage"] == "playing":
            s = {**s, "stage": "finished", "winner": 1 if action == "win1" else 2}
            db.finish_match(s, s["winner"])
            sessions[s["session_id"]] = s
        failed = await clean_match_newbies(guild, s["team1"] + s["team2"])
        await inter.message.edit(embed=result_embed(guild, s, s["winner"]), view=None)
        end_session(s["session_id"])
        feedback = "경기 결과를 기록했어요."
        if failed:
            feedback += (f" 뉴비 역할 회수에 실패한 참가자 {len(failed)}명이 있어요. "
                         "봇 역할 권한을 확인한 뒤 /뉴비정리를 실행해 주세요.")
        await eph(inter, feedback)


# =====================================================================
#  슬래시 명령어 - 모집
# =====================================================================
FORMAT_CHOICES = [app_commands.Choice(name="3판2선", value="bo3"), app_commands.Choice(name="단판", value="bo1")]
KIND_CHOICES = [app_commands.Choice(name="모바시 내전", value="mobashi"),
                app_commands.Choice(name="예약 내전", value="reserve")]


async def start_recruit(inter: discord.Interaction, gkey: str, kind: str, fmt: str, when: Optional[str]):
    if not is_staff(inter.user):
        return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
    found = find_conf(inter.channel_id)
    if not found or found[0] != gkey:
        names = ", ".join(c["label"] for c in GROUPS[gkey]["channels"])
        return await eph(inter, f"이 명령어는 {GROUPS[gkey]['name']} 내전 신청방({names})에서만 사용할 수 있어요.")
    reserve_time = None
    if kind == "reserve":
        if not when:
            return await eph(inter, "예약 내전은 **모집시간**을 입력해 주세요. 예) 22:00, 오후10시")
        reserve_time = parse_time(when)
        if not reserve_time:
            return await eph(inter, "시간 형식을 알아보지 못했어요. 예) 22:00, 22시30분, 오후10시")
    await inter.response.defer(ephemeral=True)
    async with locks[inter.channel_id]:
        if channel_sessions(inter.channel_id):
            return await eph(inter, "이 신청방에 이미 모집 또는 진행 중인 내전이 있어요.")
        s = await open_recruit(inter.channel, gkey, found[2]["label"], kind, fmt, reserve_time, inter.user.id)
    notice = "" if s.get("status_message_id") else (
        "\n모집 현황 알림은 올리지 못했어요. STATUS_CHANNEL_ID와 현황 채널의 메시지 전송 권한을 확인해 주세요."
    )
    await eph(inter, "모집글을 생성했어요." + notice)


@bot.tree.command(name="모바시내전모집", description="[관리진] 모바시 내전 참가자 모집 시작")
@app_commands.describe(진행방식="3판2선 또는 단판")
@app_commands.choices(진행방식=FORMAT_CHOICES)
async def cmd_mobashi(inter: discord.Interaction, 진행방식: app_commands.Choice[str]):
    await start_recruit(inter, "mobashi", "mobashi", 진행방식.value, None)


@bot.tree.command(name="단판내전모집", description="[관리진] 단판 전용 신청방에서 단판 내전 모집 시작")
async def cmd_single(inter: discord.Interaction):
    await start_recruit(inter, "single", "mobashi", "bo1", None)


@bot.tree.command(name="저티어내전모집", description="[관리진] 저티어 내전 참가자 모집 시작")
@app_commands.describe(모집유형="모바시 / 예약", 진행방식="3판2선 또는 단판",
                       모집시간="예약 내전 시작 시간 (예: 22:00, 오후10시)")
@app_commands.choices(모집유형=KIND_CHOICES, 진행방식=FORMAT_CHOICES)
async def cmd_low(inter: discord.Interaction, 모집유형: app_commands.Choice[str],
                  진행방식: app_commands.Choice[str], 모집시간: Optional[str] = None):
    await start_recruit(inter, "low", 모집유형.value, 진행방식.value, 모집시간)


@bot.tree.command(name="고티어내전모집", description="[관리진] 고티어 내전 참가자 모집 시작")
@app_commands.describe(모집유형="모바시 / 예약", 진행방식="3판2선 또는 단판",
                       모집시간="예약 내전 시작 시간 (예: 22:00, 오후10시)")
@app_commands.choices(모집유형=KIND_CHOICES, 진행방식=FORMAT_CHOICES)
async def cmd_high(inter: discord.Interaction, 모집유형: app_commands.Choice[str],
                   진행방식: app_commands.Choice[str], 모집시간: Optional[str] = None):
    await start_recruit(inter, "high", 모집유형.value, 진행방식.value, 모집시간)


async def recruit_choices(inter: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
    options = []
    stages = {"recruit": "모집 중", "full": "모집 완료", "playing": "경기 중", "finished": "경기 종료"}
    for s in sorted(channel_sessions(inter.channel_id), key=lambda row: row["session_id"], reverse=True):
        created = discord.utils.snowflake_time(s["session_id"]).astimezone(KST)
        reserve = f" · 예약 {s['reserve_time']}" if s["kind"] == "reserve" else ""
        label = (f"{created:%m/%d %H:%M:%S} · {s['label']} · {FORMAT_NAME[s['format']]}"
                 f"{reserve} · {stages[s['stage']]} · {s['session_id']}")
        if current in label:
            options.append(app_commands.Choice(name=label[:100], value=str(s["session_id"])))
    return options[:25]


def command_session(channel_id: int, selection: Optional[str]) -> tuple[Optional[dict], str]:
    available = channel_sessions(channel_id)
    if selection:
        for s in available:
            if selection.strip() in (str(s["session_id"]), str(s["message_id"])):
                return s, ""
        return None, "이 채널에서 해당 모집글을 찾지 못했어요. 모집글 목록에서 다시 선택해 주세요."
    if len(available) == 1:
        return available[0], ""
    if available:
        return None, "이 채널에 모집글이 여러 개 있어요. 명령어의 ‘모집글’ 항목에서 대상을 선택해 주세요."
    return None, "이 채널에 진행 중인 내전이 없어요."


@bot.tree.command(name="강제취소", description="잠수/탈퇴 유저를 강제로 명단에서 제외 (모집자 또는 관리진)")
@app_commands.autocomplete(모집글=recruit_choices)
async def cmd_force(inter: discord.Interaction, 유저: Optional[discord.Member] = None,
                    유저아이디: Optional[str] = None, 모집글: Optional[str] = None):
    if (유저 is None) == (유저아이디 is None):
        return await eph(inter, "유저 또는 유저아이디 중 하나를 입력해 주세요.")
    if 유저아이디 is not None and not re.fullmatch(r"[0-9]{15,20}", 유저아이디.strip()):
        return await eph(inter, "디스코드 사용자 ID를 숫자로 입력해 주세요.")
    uid = 유저.id if 유저 else int(유저아이디)
    await inter.response.defer(ephemeral=True)
    async with locks[inter.channel_id]:
        s, error = command_session(inter.channel_id, 모집글)
        if not s:
            return await eph(inter, error)
        if s["stage"] not in ("recruit", "full"):
            return await eph(inter, "선택한 내전은 모집 중이 아니에요.")
        if not (is_staff(inter.user) or inter.user.id == s["host_id"]):
            return await eph(inter, "모집자 또는 관리진만 사용할 수 있어요.")
        where = await remove_player(inter.guild, inter.channel, s, uid, forced=True)
        if not where:
            return await eph(inter, "해당 유저는 명단에 없어요.")
        await refresh(inter.channel, s)
        await eph(inter, f"<@{uid}> 님을 {where} 명단에서 제외했어요.")


@bot.tree.command(name="내전현황", description="현재 모집 명단을 채팅창 아래에 새로 띄우기")
@app_commands.autocomplete(모집글=recruit_choices)
async def cmd_status(inter: discord.Interaction, 모집글: Optional[str] = None):
    async with locks[inter.channel_id]:
        s, error = command_session(inter.channel_id, 모집글)
        if not s:
            return await eph(inter, error)
        await inter.response.defer(ephemeral=True)
        if s["stage"] in ("recruit", "full"):
            await refresh(inter.channel, s)
        current = sessions[s["session_id"]]
        await replace_post(inter.channel, current, dict(current))
        notice = ""
        if current["stage"] == "recruit" and not await post_status(inter.guild, sessions[s["session_id"]]):
            notice = "\n모집 현황 알림을 올리지 못했어요. 채널 ID와 메시지 전송 권한을 확인해 주세요."
        await eph(inter, "명단을 다시 띄웠어요." + notice)
        if s["stage"] == "finished":
            end_session(s["session_id"])


@bot.tree.command(name="내전모집취소", description="[관리진] 이 채널의 내전 모집/경기를 기록 없이 닫기")
@app_commands.autocomplete(모집글=recruit_choices)
async def cmd_close(inter: discord.Interaction, 모집글: Optional[str] = None):
    if not is_staff(inter.user):
        return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
    async with locks[inter.channel_id]:
        s, error = command_session(inter.channel_id, 모집글)
        if not s:
            return await eph(inter, error)
        if s["stage"] == "finished":
            return await eph(inter, "전적이 이미 반영됐어요. /내전현황으로 결과를 다시 표시할 수 있어요.")
        await inter.response.defer()
        await delete_msg(inter.channel, s["message_id"])
        await mark_status_done(inter.guild, s, "❌ 모집 취소")
        end_session(s["session_id"])
        await inter.followup.send(f"🗑️ {s['label']} 내전 모집을 닫았어요. (전적 미반영)")


# =====================================================================
#  슬래시 명령어 - 전적 / 기타
# =====================================================================
@bot.tree.command(name="전적", description="유저의 내전 승/패 전적 및 승률을 조회합니다.")
async def cmd_record(inter: discord.Interaction, 유저: Optional[discord.Member] = None):
    m = 유저 or inter.user
    w, l = db.record(m.id)
    g = w + l
    rate = f"{w * 100 / g:.1f}%" if g else "-"
    emb = discord.Embed(title=f"📊 {m.display_name} 님의 내전 전적", color=0x3498DB,
                        description=f"**{g}전 {w}승 {l}패** · 승률 **{rate}**")
    await inter.response.send_message(embed=emb)


@bot.tree.command(name="순위", description="🏆 서버 내전 승리/승률 전체 랭킹 보드를 확인합니다.")
@app_commands.choices(기준=[app_commands.Choice(name="승리 수", value="wins"),
                          app_commands.Choice(name="승률", value="rate")])
async def cmd_rank(inter: discord.Interaction, 기준: Optional[app_commands.Choice[str]] = None):
    by = 기준.value if 기준 else "wins"
    rows = db.ranking(by)
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, r in enumerate(rows):
        m = inter.guild.get_member(r["user_id"])
        name = discord.utils.escape_markdown(m.display_name) if m else "(나간 유저)"
        g = r["wins"] + r["losses"]
        head = medals[i] if i < 3 else f"`{i + 1:>2}.`"
        lines.append(f"{head} **{name}** · {r['wins']}승 {r['losses']}패 · {r['wins'] * 100 / g:.1f}%")
    title = "🏆 내전 랭킹 · " + ("승률" if by == "rate" else "승리 수")
    desc = "\n".join(lines) or "아직 기록이 없어요."
    if by == "rate":
        desc += f"\n\n※ {config.RANK_MIN_GAMES}판 이상 참여자만 집계"
    await inter.response.send_message(embed=discord.Embed(title=title, description=desc, color=0xF1C40F))


@bot.tree.command(name="전적수정", description="[관리자] 특정 유저의 승/패 전적을 직접 수정합니다.")
async def cmd_edit(inter: discord.Interaction, 유저: discord.Member,
                   승: app_commands.Range[int, 0], 패: app_commands.Range[int, 0]):
    if not is_staff(inter.user):
        return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
    db.set_record(유저.id, 승, 패)
    await eph(inter, f"{유저.display_name} 님의 전적을 **{승}승 {패}패**로 수정했어요.")


@bot.tree.command(name="전적초기화", description="[관리자] 특정 유저의 전적을 0승 0패로 초기화합니다.")
async def cmd_reset(inter: discord.Interaction, 유저: discord.Member):
    if not is_staff(inter.user):
        return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
    db.set_record(유저.id, 0, 0)
    await eph(inter, f"{유저.display_name} 님의 전적을 초기화했어요.")


@bot.tree.command(name="뉴비정리", description=f"[관리자] 내전 {config.NEWBIE_GAMES}판 이상 완료한 유저들의 뉴비 역할을 일괄 회수합니다.")
async def cmd_newbie(inter: discord.Interaction):
    if not is_staff(inter.user):
        return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
    role = discord.utils.get(inter.guild.roles, name=config.NEWBIE_ROLE_NAME)
    if not role:
        return await eph(inter, f"'{config.NEWBIE_ROLE_NAME}' 역할을 찾지 못했어요.")
    await inter.response.defer(ephemeral=True)
    done = []
    for m in list(role.members):
        try:
            if await remove_earned_newbie_role(m, role):
                done.append(m.display_name)
        except discord.HTTPException:
            logger.exception("뉴비 역할 수동 회수 실패 (guild=%s, user=%s)", inter.guild.id, m.id)
    text = f"뉴비 역할 회수 완료: **{len(done)}명**"
    if done:
        text += "\n" + ", ".join(done[:50])
    await inter.followup.send(text, ephemeral=True)


@bot.tree.command(name="취소명단", description="[관리자] 내전 참여 취소 기록을 조회합니다.")
async def cmd_cancels(inter: discord.Interaction, 유저: Optional[discord.Member] = None):
    if not is_staff(inter.user):
        return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
    rows = db.cancels(inter.guild.id, 유저.id if 유저 else None)
    lines = [f"`{r['at']}` <@{r['user_id']}> · <#{r['channel_id']}> · 당시 {r['count_at']}명 · {r['note']}"
             for r in rows]
    emb = discord.Embed(title="📝 취소 명단" + (f" · {유저.display_name}" if 유저 else " (최근 20건)"),
                        description="\n".join(lines) or "기록이 없어요.", color=0xE74C3C)
    await inter.response.send_message(embed=emb, ephemeral=True)


@bot.tree.command(name="주사위", description="1부터 6까지 주사위를 던집니다.")
async def cmd_dice(inter: discord.Interaction):
    await inter.response.send_message(f"🎲 {inter.user.mention} 님의 주사위: **{random.randint(1, 6)}**",
                                      allowed_mentions=discord.AllowedMentions.none())


# 기간 집계와 주간 게시가 같은 날짜 범위 및 승률 기준을 사용한다.
def winrate_pages(guild_id: int, start: date, end: date) -> list[str]:
    records = db.period_records(guild_id, start, end)
    high = [r for r in records if 100 * r["wins"] > 80 * r["games"]]
    low = [r for r in records if 100 * r["wins"] <= 30 * r["games"]]
    high.sort(key=lambda r: (-r["wins"] / r["games"], -r["games"], r["user_id"]))
    low.sort(key=lambda r: (r["wins"] / r["games"], -r["games"], r["user_id"]))
    header = (f"📊 **내전 승률 | {start.isoformat()} ~ {(end - timedelta(days=1)).isoformat()}**\n"
              f"한국 시간 기준 · 모바시/저티어/고티어 합산 · 기록된 경기 참가자 {len(records)}명\n"
              "날짜별 기록 기능 도입 이후 확정된 경기만 집계합니다.")
    pages = [header]
    for label, rows in (("🔥 고승률 · 80% 초과", high), ("📉 저승률 · 30% 이하", low)):
        heading = f"**{label} ({len(rows)}명)**"
        lines = []
        for rank, r in enumerate(rows, 1):
            wins, games = r["wins"], r["games"]
            rate = 100 * wins / games
            lines.append(f"{rank}. <@{r['user_id']}> · **{rate:.2f}%** · {games}판 ({wins}승 {games-wins}패)")
        if not lines:
            pages.append(f"{heading}\n해당 없음")
        for offset in range(0, len(lines), 12):
            page_header = heading if offset == 0 else f"{heading} · 계속"
            pages.append(page_header + "\n" + "\n".join(lines[offset:offset + 12]))
    return pages


@bot.tree.command(name="합산내전승률조회", description="[관리자·보조관리자] 지정 기간의 고승률·저승률 이용자를 조회합니다.")
@app_commands.describe(시작일="YYYY-MM-DD (포함)", 종료일="YYYY-MM-DD (포함)")
async def cmd_period_winrate(inter: discord.Interaction, 시작일: str, 종료일: str):
    if not has_role(inter.user, config.CALL_ADMIN_ROLE_NAMES):
        return await eph(inter, "관리자·보조관리자만 사용할 수 있어요.")
    try:
        if not all(re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value.strip())
                   for value in (시작일, 종료일)):
            raise ValueError
        start, last = (date.fromisoformat(value.strip()) for value in (시작일, 종료일))
        end = last + timedelta(days=1)
    except (ValueError, OverflowError):
        return await eph(inter, "날짜를 YYYY-MM-DD 형식으로 입력해 주세요. 예) 2026-11-16")
    if start > last:
        return await eph(inter, "시작일은 종료일과 같거나 그 이전이어야 해요.")
    await inter.response.defer(ephemeral=True)
    await inter.followup.send(
        **winrate_embed_payload(inter.guild, start, end, title="기간별 내전 승률"),
        allowed_mentions=discord.AllowedMentions.none(), ephemeral=True,
    )


def winrate_embed_payload(guild: discord.Guild, start: date, end: date, *, title: str) -> dict:
    summary, *pages = winrate_pages(guild.id, start, end)
    title = f"📊 {title} · {start.isoformat()} ~ {(end - timedelta(days=1)).isoformat()}"
    embed = discord.Embed(title=title, description=summary.split("\n", 1)[1], color=0x8061C7)
    embeds = [embed]
    for page in pages:
        heading, body = page.split("\n", 1)
        heading = heading.replace("**", "")
        if len(body) <= 1024:
            embed.add_field(name=heading, value=body, inline=False)
        else:
            embeds.append(discord.Embed(title=heading, description=body, color=embed.color))
    if len(embeds) <= 10 and sum(map(len, embeds)) <= 6000 and len(embed.fields) <= 25:
        return {"embeds": embeds}

    report = "\n\n".join([summary, *pages]).replace("**", "")
    report = re.sub(r"<@(\d+)>", lambda match: report_member_name(guild, int(match[1])), report)
    embed.clear_fields()
    for page in pages:
        heading, body = page.split("\n", 1)
        if heading.endswith(" · 계속"):
            continue
        embed.add_field(name=heading.replace("**", ""), value="\n".join(body.splitlines()[:5]), inline=False)
    embed.set_footer(text="각 분류에서 최대 5명 표시 · 전체 명단은 첨부 파일")
    attachment = discord.File(io.BytesIO(report.encode("utf-8")), filename="winrate-report.txt")
    return {"embeds": [embed], "file": attachment}


def report_member_name(guild: discord.Guild, uid: int) -> str:
    member = guild.get_member(uid)
    return f"{member.display_name} ({uid})" if member else str(uid)


def weekly_period(current: datetime) -> tuple[date, date]:
    monday = current.date() - timedelta(days=current.weekday())
    if current < datetime.combine(monday, clock_time(0, 5), KST):
        monday -= timedelta(days=7)
    return monday - timedelta(days=7), monday


@tasks.loop(minutes=5)
async def publish_weekly_winrate():
    channel = bot.get_channel(config.WEEKLY_WINRATE_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        logger.warning("주간 승률 게시에 사용할 텍스트 채널을 찾지 못했어요. WEEKLY_WINRATE_CHANNEL_ID를 확인해 주세요.")
        return
    start, end = weekly_period(now())
    try:
        if db.weekly_report_posted(channel.id, start):
            return
        payload = winrate_embed_payload(channel.guild, start, end, title="주간 내전 승률")
        title = payload["embeds"][0].title
        async for message in channel.history(limit=100):
            if (message.author.id == bot.user.id
                    and message.embeds and message.embeds[0].title == title):
                db.mark_weekly_report_posted(channel.id, start)
                return
        await channel.send(**payload, allowed_mentions=discord.AllowedMentions.none())
        db.mark_weekly_report_posted(channel.id, start)
    except (discord.HTTPException, sqlite3.Error):
        logger.exception("주간 승률 게시 실패 (channel=%s). 다음 실행에 다시 시도합니다.", channel.id)


@publish_weekly_winrate.before_loop
async def wait_for_weekly_ready():
    await bot.wait_until_ready()


def daily_cancel_period(current: datetime) -> tuple[datetime, datetime]:
    end = current.replace(hour=2, minute=0, second=0, microsecond=0)
    if current < end:
        end -= timedelta(days=1)
    return end - timedelta(days=1), end


def daily_cancel_payload(guild_id: int, start: datetime, end: datetime) -> dict:
    rows = db.period_cancels(guild_id, start, end)
    embed = discord.Embed(
        title=f"📝 일일 내전 취소 명단 · {end:%Y-%m-%d}",
        description=f"**{start:%m/%d %H:%M} ~ {end:%m/%d %H:%M} (KST)** · 총 **{len(rows)}건**",
        color=0xE74C3C,
    )
    lines = [
        f"`{r['at'][5:]}` <@{r['user_id']}> · <#{r['channel_id']}> · {r['count_at']}명 · {r['note']}"
        for r in rows
    ]
    body = "\n".join(lines)
    if not lines:
        embed.description += "\n기록된 취소가 없어요."
    elif len(embed.description) + len(body) + 2 <= 4096:
        embed.description += "\n\n" + body
    else:
        embed.description += "\n\n" + "\n".join(lines[:10])
        embed.set_footer(text="최근 24시간 중 처음 10건 표시 · 전체 명단은 첨부 파일")
        report = "\n".join(
            f"{r['at']} · {r['name']} ({r['user_id']}) · 채널 {r['channel_id']} · {r['count_at']}명 · {r['note']}"
            for r in rows
        )
        attachment = discord.File(io.BytesIO(report.encode("utf-8")), filename="daily-cancel-report.txt")
        return {"embed": embed, "file": attachment}
    return {"embed": embed}


@tasks.loop(minutes=1)
async def publish_daily_cancels():
    start, end = daily_cancel_period(now())
    for guild in bot.guilds:
        channel = log_channel(guild)
        if not isinstance(channel, discord.TextChannel):
            continue
        try:
            if db.daily_cancel_report_posted(channel.id, end.date()):
                continue
            payload = daily_cancel_payload(guild.id, start, end)
            title = payload["embed"].title
            async for message in channel.history(limit=100, after=end):
                if message.author.id == bot.user.id and message.embeds and message.embeds[0].title == title:
                    break
            else:
                await channel.send(**payload, allowed_mentions=discord.AllowedMentions.none())
            db.mark_daily_cancel_report_posted(channel.id, end.date())
        except (discord.HTTPException, sqlite3.Error):
            logger.exception("일일 취소 명단 게시 실패 (channel=%s). 다음 실행에 다시 시도합니다.", channel.id)


@publish_daily_cancels.before_loop
async def wait_for_daily_cancels_ready():
    await bot.wait_until_ready()


@bot.tree.error
async def on_app_error(inter: discord.Interaction, error: app_commands.AppCommandError):
    print("[명령어 오류]", repr(error))
    try:
        await eph(inter, "명령어 처리 중 오류가 발생했어요.")
    except discord.HTTPException:
        pass


# =====================================================================
#  시작
# =====================================================================
@bot.event
async def setup_hook():
    sessions.update(db.load_sessions())
    if config.GUILD_ID:
        guild = discord.Object(id=config.GUILD_ID)
        bot.tree.copy_global_to(guild=guild)
        await bot.tree.sync(guild=guild)
    else:
        await bot.tree.sync()
    if config.WEEKLY_WINRATE_CHANNEL_ID:
        publish_weekly_winrate.start()
    publish_daily_cancels.start()


@bot.event
async def on_ready():
    print(f"✅ {bot.user} 로그인 완료 · 진행 중인 내전 {len(sessions)}건 복구")
    for g in bot.guilds:
        if not log_channel(g):
            print(f"⚠️ [{g.name}] 취소 명단 관리자 채널을 찾지 못했어요. "
                  f"config.py의 CANCEL_LOG_CHANNEL_ID 또는 CANCEL_LOG_CHANNEL_NAME 을 확인해 주세요.")


if __name__ == "__main__":
    if not config.TOKEN:
        raise SystemExit("❌ DISCORD_TOKEN 환경변수를 설정해 주세요. (.env.example 참고)")
    bot.run(config.TOKEN)
