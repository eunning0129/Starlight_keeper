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
import os

logger = logging.getLogger(__name__)

# 빌지코인 연동: 실패 시 예외를 기록하고 원래 봇의 기능은 유지합니다.
async def bilgecoin_event(endpoint, payload):
    url = os.getenv("BILGECOIN_URL", "").rstrip("/")
    secret = os.getenv("INTEGRATION_SECRET", "")
    if not url or not secret:
        logging.warning("빌지코인 연동 환경변수 미설정: %s", endpoint)
        return False
    import aiohttp
    try:
        async with aiohttp.ClientSession() as client:
            async with client.post(url + endpoint, json=payload,
                headers={"X-Integration-Secret": secret},
                timeout=aiohttp.ClientTimeout(total=15)) as response:
                body = await response.text()
                if response.status != 200:
                    logging.error("빌지코인 연동 실패 (%s): %s %s", endpoint, response.status, body[:300])
                    return False
                return True
    except (aiohttp.ClientError, TimeoutError):
        logging.exception("빌지코인 연동 통신 실패: %s", endpoint)
        return False



# Persistent outbox: survives process restart and Railway redeploy when DB_PATH is on a Volume.
def _outbox_conn():
    conn = sqlite3.connect(config.DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute('''CREATE TABLE IF NOT EXISTS bilgecoin_outbox (
        event_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, payload TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT, next_retry INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)''')
    conn.commit()
    return conn

def queue_bilgecoin_event(endpoint, payload, event_id):
    ts = int(time.time())
    with _outbox_conn() as conn:
        conn.execute('''INSERT OR IGNORE INTO bilgecoin_outbox
            (event_id,endpoint,payload,created_at,updated_at) VALUES(?,?,?,?,?)''',
            (event_id,endpoint,json.dumps(payload,ensure_ascii=False),ts,ts))

def _outbox_update(event_id, ok, err=''):
    ts=int(time.time())
    with _outbox_conn() as conn:
        row=conn.execute('SELECT attempts FROM bilgecoin_outbox WHERE event_id=?',(event_id,)).fetchone()
        if row is None:return
        attempts=row['attempts']+1
        # Exponential backoff capped at 10 minutes; manual retries override this timer.
        delay=min(600, 15 * 2 ** min(attempts-1,6))
        conn.execute('''UPDATE bilgecoin_outbox SET status=?, attempts=?, last_error=?,
            next_retry=?, updated_at=? WHERE event_id=?''',
            ('done' if ok else 'pending',attempts,'' if ok else err[:500],0 if ok else ts+delay,ts,event_id))

async def send_queued_event(event_id):
    with _outbox_conn() as conn:
        row=conn.execute('SELECT * FROM bilgecoin_outbox WHERE event_id=?',(event_id,)).fetchone()
    if row is None:return False,'event not found'
    if row['status']=='done':return True,'already delivered'
    endpoint=row['endpoint'];payload=json.loads(row['payload'])
    url=os.getenv('BILGECOIN_URL','').strip().rstrip('/')
    secret=os.getenv('INTEGRATION_SECRET','').strip()
    if not url or not secret:
        error='BILGECOIN_URL / INTEGRATION_SECRET 미설정'
        _outbox_update(event_id,False,error)
        logging.error('빌지코인 연동 실패 %s: %s',event_id,error)
        return False,error
    import aiohttp
    try:
        timeout=aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.post(url+endpoint,json=payload,headers={'X-Integration-Secret':secret}) as response:
                body=await response.text()
                if response.status==200:
                    result=json.loads(body)
                    if result.get('ok') is True:
                        _outbox_update(event_id,True)
                        logging.info('빌지코인 지급 요청 성공 %s: %s',event_id,body[:300])
                        return True,body[:300]
                error=f'HTTP {response.status}: {body[:250]}'
    except Exception as exc:
        error=f'{type(exc).__name__}: {exc}'
    _outbox_update(event_id,False,error)
    logging.error('빌지코인 지급 요청 실패 %s: %s',event_id,error)
    return False,error

@tasks.loop(seconds=30)
async def retry_bilgecoin_outbox():
    with _outbox_conn() as conn:
        rows=conn.execute('''SELECT event_id FROM bilgecoin_outbox
            WHERE status='pending' AND next_retry<=? ORDER BY created_at LIMIT 20''',
            (int(time.time()),)).fetchall()
    for row in rows:
        await send_queued_event(row['event_id'])

@retry_bilgecoin_outbox.before_loop
async def before_retry_bilgecoin_outbox():
    await bot.wait_until_ready()

async def notify_match_reward(guild,channel,event_id,ok,detail):
    status='✅ 빌지코인 보상 요청 성공' if ok else '⚠️ 빌지코인 보상 전송 실패 · 자동 재시도 예약'
    msg=f'{status}\n경기 ID: `{event_id.split(":")[-1]}`\n{detail[:500]}'
    try:
        target_id=int(os.getenv('POINT_REWARD_STATUS_CHANNEL_ID','0') or 0)
        target=guild.get_channel(target_id) if target_id else channel
        if target is None:target=channel
        await target.send(msg,allowed_mentions=discord.AllowedMentions.none())
    except discord.HTTPException:
        logger.exception('빌지코인 보상 상태 알림 실패')

KST = ZoneInfo("Asia/Seoul")
FORMAT_NAME = {"bo3": "3판2선", "bo1": "단판"}
KIND_NAME = {"mobashi": "모바시", "reserve": "예약"}

GROUPS = {
    "mobashi": {"name": "모바시", "emoji": "🔥", "color": 0x3498DB,
                "channels": config.MOBASHI_CHANNELS + config.SINGLE_CHANNELS, "roles": ["내전"]},
    "low": {"name": "저티어", "emoji": "🌱", "color": 0x2ECC71,
            "channels": config.LOW_CHANNELS, "roles": config.LOW_TIER_ROLES},
    "high": {"name": "고티어", "emoji": "👑", "color": 0x9B59B6,
             "channels": config.HIGH_CHANNELS, "roles": config.HIGH_TIER_ROLES},
    "twenty": {"name": "20인", "emoji": "🏆", "color": 0xF1C40F,
               "channels": config.TWENTY_CHANNELS, "roles": ["내전"]},
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

TIER_RE = re.compile(r"^(GM|[IBSGPEDMC])(\d+)?[▼▲↓↑⬆⬇🔺🔻↗↘⇧⇩⏫⏬\uFE0F]*$", re.I)
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
        c.execute("CREATE TABLE IF NOT EXISTS sessions (channel_id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
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
            self.conn.execute("UPDATE sessions SET data=? WHERE channel_id=?",
                              (json.dumps(session, ensure_ascii=False), session["channel_id"]))

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
        self.conn.execute("INSERT OR REPLACE INTO sessions(channel_id, data) VALUES(?,?)",
                          (s["channel_id"], json.dumps(s, ensure_ascii=False)))
        self.conn.commit()

    def delete_session(self, cid: int):
        self.conn.execute("DELETE FROM sessions WHERE channel_id=?", (cid,))
        self.conn.commit()

    def load_sessions(self) -> dict[int, dict]:
        return {r["channel_id"]: json.loads(r["data"])
                for r in self.conn.execute("SELECT * FROM sessions")}

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
    sessions[s["channel_id"]] = s
    db.save_session(s)


def end_session(cid: int):
    sessions.pop(cid, None)
    db.delete_session(cid)


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
    graduation_id=f'graduation:{member.guild.id}:{member.id}'
    queue_bilgecoin_event('/events/newbie-graduated', {
        'guild_id':member.guild.id,'invitee_id':member.id,
        'completed_matches':wins+losses,'newbie_role_removed':True},graduation_id)
    await send_queued_event(graduation_id)
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
    return f"{g['emoji']} {s['label']} | {KIND_NAME[s['kind']]} {FORMAT_NAME[s['format']]} 내전 정보"


def gather_time(s: dict) -> str:
    t = datetime.strptime(s["reserve_time"], "%H:%M") - timedelta(minutes=10)
    return t.strftime("%H:%M")


def guide_text(s: dict) -> str:
    lines = []
    if s["kind"] == "reserve":
        lines.append(config.GUIDE_RESERVE.format(gather=gather_time(s)))
    lines.append(config.GUIDE_CANCEL.format(
        start=config.CANCEL_RECORD_START, end=config.CANCEL_RECORD_END,
        count=config.CANCEL_WARN_COUNT,
    ))
    lines.append(config.GUIDE_RATING_GAP)
    if s["group"] == "mobashi":
        lines.append(config.GUIDE_M700)
    lines.append(config.GUIDE_8)
    lines.append(config.GUIDE_LATE)
    return "\n".join(f"• {x}" for x in lines)


def entry_line(guild, i: int, e: dict) -> str:
    name = name_of(guild, e)
    tier = parse_nick(name)[1]
    return (f"`{i:>2}.` {tier_emoji(tier)} **{discord.utils.escape_markdown(name)}**"
            f" · {record_text(e['id'])} · ⏰ {e['time']}")


# ---------------------------------------------------------------------
#  1) 모집글
# ---------------------------------------------------------------------
def max_players(s):
    return 20 if s["group"] == "twenty" else config.MAX_PLAYERS

def is_twenty(s):
    return s["group"] == "twenty"

def recruit_embed(guild, s: dict) -> discord.Embed:
    p, w = s["participants"], s["waitlist"]
    n, mx = len(p), max_players(s)
    desc = [f"모집자 <@{s['host_id']}>"]
    if s["kind"] == "reserve":
        desc.append(f"시작 시간 : **{s['reserve_time']}** (집합 **{gather_time(s)}**)")
    desc.append(f"진행 방식 : **{FORMAT_NAME[s['format']]}**")
    desc.append(f"최대 **{mx}명** 참여 가능 · 대기 최대 **{config.MAX_WAITING}명**")
    desc.append(f"{'▰' * n}{'▱' * (mx - n)}  **{n}/{mx}** · {mx - n}자리 남음")

    emb = discord.Embed(title=title_of(s), description="\n".join(desc), color=GROUPS[s["group"]]["color"])
    player_lines = [entry_line(guild, i, e) for i, e in enumerate(p, 1)]
    emb.add_field(name=f"🟢 내전 참가 ({n}/{mx})",
                  value="\n".join(player_lines[:5]) or "아직 아무도 없습니다", inline=False)
    if len(player_lines) > 5:
        for offset in range(5, len(player_lines), 5):
            emb.add_field(name="참가 명단 계속", value="\n".join(player_lines[offset:offset+5])[:1024], inline=False)
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
        rows.append([riot.replace(chr(96), "ˋ"), tier or "-", lines.replace(chr(96), "ˋ") or "-",
                     f"{record_text(e['id'])}"])
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
    if s["format"] == "bo1" or is_twenty(s):
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
        btn(s, "calladmin", "관리자 호출", discord.ButtonStyle.primary, "🚨", 1) if s["group"] != "mobashi" else None,
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
                                    "모든 경기가 끝나면 최종 승리한 팀 버튼을 눌러주세요 · 관리자 전용",
                        color=0xF1C40F)
    emb.add_field(name="🔵 1팀", value=team_lines(guild, s, s["team1"]), inline=False)
    emb.add_field(name="🔴 2팀", value=team_lines(guild, s, s["team2"]), inline=False)
    if is_twenty(s):
        emb.description = "20인 내전 · 전적/포인트 미반영 · 팀장들이 현황판에서 경기 결과를 기록해 주세요."
        for n in (3, 4):
            emb.add_field(name=f"⚔️ {n}팀", value=team_lines(guild, s, s[f"team{n}"]), inline=False)
    return emb


def playing_view(s: dict) -> discord.ui.View:
    if is_twenty(s):
        return make_view(btn(s, "norecord", "기록 없이 종료", discord.ButtonStyle.secondary, "🔒"))
    return make_view(
        btn(s, "win1", "1팀 최종 승리", discord.ButtonStyle.primary, "🔵"),
        btn(s, "win2", "2팀 최종 승리", discord.ButtonStyle.danger, "🔴"),
        btn(s, "recordrequest", "내전 기록 요청", discord.ButtonStyle.secondary, "📝"),
        btn(s, "norecord", "기록 없이 종료", discord.ButtonStyle.secondary, "🔒"),
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


async def post_status(guild, s: dict):
    ch = guild.get_channel(config.STATUS_CHANNEL_ID)
    if not ch:
        return
    if s.get("status_message_id"):
        return
    if s["kind"] == "reserve":
        text = f"<#{s['channel_id']}> {s['reserve_time']} 예약 {FORMAT_NAME[s['format']]} 내전 인원 모집 중"
    else:
        text = f"<#{s['channel_id']}> 모바시 {FORMAT_NAME[s['format']]} 내전 인원 모집 중"
    try:
        msg = await ch.send(text)
        s["status_message_id"], s["status_text"] = msg.id, text
    except discord.HTTPException:
        pass


async def mark_status_done(guild, s: dict, label="✅ 모집 완료"):
    ch = guild.get_channel(config.STATUS_CHANNEL_ID)
    if not ch or not s.get("status_message_id"):
        return
    try:
        await ch.get_partial_message(s["status_message_id"]).edit(content=f"~~{s['status_text']}~~ {label}" if label else s["status_text"])
    except discord.HTTPException:
        pass


async def open_recruit(channel: discord.TextChannel, gkey: str, label: str, kind: str, fmt: str,
                       reserve_time: Optional[str], host_id: int, ping: bool = True):
    s = {
        "channel_id": channel.id, "guild_id": channel.guild.id, "group": gkey, "label": label,
        "kind": kind, "format": fmt, "reserve_time": reserve_time, "host_id": host_id,
        "stage": "recruit", "participants": [], "waitlist": [], "team1": [], "team2": [],
        "message_id": None, "status_message_id": None, "status_text": "", "last_call": 0,
        "cancels": [], "board_message_id": None,
    }
    content = "아래 버튼을 눌러 내전에 참여해주세요!"
    if ping and GROUPS[gkey]["roles"]:
        roles = [discord.utils.get(channel.guild.roles, name=n) for n in GROUPS[gkey]["roles"]]
        mentions = " ".join(r.mention for r in roles if r)
        if mentions:
            content = f"{mentions}\n{content}"
    msg = await channel.send(content=content, embed=recruit_embed(channel.guild, s),
                             view=recruit_view(s), allowed_mentions=ALLOW_ROLES)
    s["message_id"] = msg.id
    save(s)
    await post_status(channel.guild, s)
    save(s)
    return s


async def open_next(guild: discord.Guild, s: dict, host_id: int):
    """경기 시작 시 같은 그룹의 다음 빈 채널(A→E 순)에 동일한 모집글 자동 생성"""
    if s["group"] != "mobashi":
        return None
    channels = config.MOBASHI_CHANNELS
    index = next((i for i, conf in enumerate(channels) if conf["text_id"] == s["channel_id"]), None)
    if index is None:
        return None
    for conf in channels[index + 1:] + channels[:index]:
        cid = conf["text_id"]
        ch = guild.get_channel(cid)
        if not isinstance(ch, discord.TextChannel):
            continue
        async with locks[cid]:
            if cid not in sessions:
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
        stage = "full" if len(s["participants"]) == max_players(s) else "recruit"
        if s["stage"] != stage:
            await replace_post(channel, s, {**s, "stage": stage})
            await mark_status_done(channel.guild, sessions[channel.id],
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
    match_log_id = int(os.getenv("MATCH_LOG_CHANNEL_ID", "0") or 0)
    ch = guild.get_channel(match_log_id) if match_log_id else None
    if not ch:
        ch = guild.get_channel(config.CANCEL_LOG_CHANNEL_ID) if config.CANCEL_LOG_CHANNEL_ID else None
    if not ch and config.CANCEL_LOG_CHANNEL_NAME:
        ch = discord.utils.get(guild.text_channels, name=config.CANCEL_LOG_CHANNEL_NAME)
    return ch


async def log_roster_event(guild, channel, uid, action, name=None):
    log = log_channel(guild)
    if not log:
        return
    safe = discord.utils.escape_markdown(name or (guild.get_member(uid).display_name if guild.get_member(uid) else str(uid)))
    try:
        await log.send(f"📋 **내전 {action}** · <@{uid}> ({safe})\n"
                       f"{channel.mention} · {now().strftime('%m/%d %H:%M')}",
                       allowed_mentions=discord.AllowedMentions.none())
    except discord.HTTPException:
        logger.exception("내전 참가/취소 로그 전송 실패")


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
            await log_roster_event(guild, channel, uid, "참가 취소" if not forced else "참가 강제 제외", e["name"])
            if s["waitlist"]:
                promoted = s["waitlist"].pop(0)
                s["participants"].append(promoted)
                await log_roster_event(guild, channel, promoted["id"], "대기 → 참가 자동 승급", promoted["name"])
                try:
                    await channel.send(f"🔼 <@{promoted['id']}> 님이 대기열에서 **내전 참가**로 자동 이동했어요!",
                                       allowed_mentions=ALLOW_USERS, delete_after=60)
                except discord.HTTPException:
                    pass
            return "참가"
    for e in s["waitlist"]:
        if e["id"] == uid:
            s["waitlist"].remove(e)
            await log_roster_event(guild, channel, uid, "대기 취소" if not forced else "대기 강제 제외", e["name"])
            return "대기"
    return None


# =====================================================================
#  모달
# =====================================================================
class SummonModal(discord.ui.Modal, title="내전 참가자 소집"):
    def __init__(self, channel_id: int, message_id: int):
        super().__init__()
        self.channel_id = channel_id
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
            s = sessions.get(self.channel_id)
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
    # 티어 표의 행 전체를 복사해도 롤닉네임#태그로 참가자를 식별합니다.
    # Discord에서 긴 행이 줄바꿈되거나 여러 행이 한 줄로 붙는 경우도 처리합니다.
    names = defaultdict(list)
    for entry in roster:
        riot = parse_nick(name_of(guild, entry))[0]
        names[norm(riot)].append(entry["id"])

    # 표 복사 시 줄바꿈/공백이 달라질 수 있으므로 공백을 제거한 텍스트에서 검색합니다.
    compact = norm(text.replace("```", ""))
    found = []
    for riot, user_ids in names.items():
        if not riot:
            continue
        for match in re.finditer(re.escape(riot), compact):
            # 다른 닉네임의 일부를 잘못 인식하지 않도록 위치도 함께 보관합니다.
            found.append((match.start(), match.end(), user_ids))
    found.sort(key=lambda item: (item[0], -(item[1] - item[0])))

    ids, bad = [], []
    last_end = -1
    for start, end, matches in found:
        if start < last_end:
            continue
        last_end = end
        if len(matches) == 1:
            ids.append(matches[0])
        else:
            bad.append("동일한 롤 닉네임을 사용하는 참가자가 있어요")

    # 이전 형식의 ID 입력 및 단순 닉네임 입력도 유지합니다.
    if not ids and not bad:
        eligible = {entry["id"] for entry in roster}
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

    def __init__(self, channel_id: int, message_id: int):
        super().__init__()
        self.channel_id = channel_id
        self.message_id = message_id

    async def on_submit(self, inter: discord.Interaction):
        guild, channel = inter.guild, inter.channel
        if not is_staff(inter.user):
            return await eph(inter, "관리진만 경기를 시작할 수 있어요.")
        await inter.response.defer(ephemeral=True)
        async with locks[self.channel_id]:
            s = sessions.get(self.channel_id)
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
        await inter.followup.send(f"⚔️ 경기를 시작했어요!{extra}", ephemeral=True)


# 20인 내전: 4팀 입력 + 팀장 4명 (모달 최대 5개 입력란)
TWENTY_MATCHES = [(1, 2), (3, 4), (1, 3), (2, 4), (1, 4), (2, 3)]


class TwentyTeamModal(discord.ui.Modal, title="20인 내전 팀/팀장 입력"):
    def __init__(self, channel_id, message_id):
        super().__init__()
        self.channel_id, self.message_id = channel_id, message_id
        self.teams = []
        for n in range(1, 5):
            field = discord.ui.TextInput(label=f"{n}팀 (5명)", style=discord.TextStyle.paragraph,
                                         max_length=1800, placeholder="참가자 5명의 롤닉네임#태그 줄을 붙여넣으세요")
            self.add_item(field)
            self.teams.append(field)
        self.captains = discord.ui.TextInput(label="팀장 (1~4팀 순서, 한 줄에 한 명)",
                                              style=discord.TextStyle.paragraph, max_length=500,
                                              placeholder="각 팀 팀장의 롤닉네임#태그를 한 줄씩 입력")
        self.add_item(self.captains)

    async def on_submit(self, inter):
        if not is_staff(inter.user):
            return await eph(inter, "관리진만 팀을 입력할 수 있어요.")
        await inter.response.defer(ephemeral=True)
        async with locks[self.channel_id]:
            s = sessions.get(self.channel_id)
            if not s or not is_twenty(s) or s["stage"] != "full" or s["message_id"] != self.message_id:
                return await eph(inter, "현재 20인 내전 티어 정보 글에서 다시 입력해 주세요.")
            teams = []
            for field in self.teams:
                ids, bad = match_team(inter.guild, field.value, s["participants"])
                if bad or len(ids) != 5 or len(set(ids)) != 5:
                    return await eph(inter, f"{len(teams)+1}팀은 중복 없는 참가자 5명이어야 해요. 인식 실패: {bad[:3]}")
                teams.append(ids)
            all_ids = sum(teams, [])
            if len(set(all_ids)) != 20 or set(all_ids) != {e["id"] for e in s["participants"]}:
                return await eph(inter, "20명 전원이 정확히 한 팀에만 포함되어야 해요.")
            captain_lines = [line for line in self.captains.value.splitlines() if line.strip()]
            if len(captain_lines) != 4:
                return await eph(inter, "팀장은 1~4팀 순서로 한 줄에 한 명씩 입력해 주세요.")
            captain_ids = []
            for n, line in enumerate(captain_lines):
                ids, bad = match_team(inter.guild, line, s["participants"])
                if bad or len(ids) != 1 or ids[0] not in teams[n]:
                    return await eph(inter, f"{n+1}팀 팀장은 해당 팀 참가자 한 명이어야 해요.")
                captain_ids.append(ids[0])
            updated = {**s, **{f"team{i+1}": team for i, team in enumerate(teams)},
                       "captains": captain_ids, "twenty_results": {}, "stage": "playing"}
            await replace_post(inter.channel, s, updated)
            board = await inter.channel.send(embed=twenty_board_embed(updated), view=twenty_board_view(updated))
            updated["board_message_id"] = board.id
            save(updated)
            await eph(inter, "20인 내전 팀 명단과 현황판을 생성했어요. (전적/포인트 미반영)")


def twenty_board_embed(s):
    emb = discord.Embed(title="🏆 여 운 20인 내전 현황판",
                        description="주의사항 : 내전 불참자들은 음성 채널에서 마이크 OFF 필수",
                        color=0xF1C40F)
    results = s.get("twenty_results", {})
    for index, (a, b) in enumerate(TWENTY_MATCHES):
        winner = results.get(str(index))
        status = (f"승리 팀 : **{winner}팀**\n패배 팀 : **{b if winner == a else a}팀**"
                  if winner else "승리 팀 : 미정\n패배 팀 : 미정")
        emb.add_field(name=f"⚔️ {a}팀 VS {b}팀 (단판)", value=status, inline=False)
    return emb


def twenty_board_view(s):
    view = discord.ui.View(timeout=None)
    for index, (a, b) in enumerate(TWENTY_MATCHES):
        for team in (a, b):
            view.add_item(discord.ui.Button(label=f"{index+1}경기 {team}팀 승리", row=index//2,
                       style=discord.ButtonStyle.secondary,
                       custom_id=f"nj20:win:{s['channel_id']}:{index}:{team}"))
    view.add_item(discord.ui.Button(label="내전 종료", emoji="🏁", row=3,
                  style=discord.ButtonStyle.danger, custom_id=f"nj20:end:{s['channel_id']}:0:0"))
    return view


async def handle_twenty_button(inter):
    cid = (inter.data or {}).get("custom_id", "")
    match = re.fullmatch(r"nj20:(win|end):(\d+):(\d+):(\d+)", cid)
    if not match or inter.channel_id != int(match[2]):
        return
    action, channel_id, index, team = match[1], int(match[2]), int(match[3]), int(match[4])
    await inter.response.defer(ephemeral=True)
    async with locks[channel_id]:
        s = sessions.get(channel_id)
        if not s or not is_twenty(s) or s["stage"] != "playing" or s.get("board_message_id") != inter.message.id:
            return await eph(inter, "현재 진행 중인 20인 내전 현황판이 아니에요.")
        if action == "win":
            if index >= len(TWENTY_MATCHES) or team not in TWENTY_MATCHES[index]:
                return await eph(inter, "잘못된 경기 결과예요.")
            a, b = TWENTY_MATCHES[index]
            if str(index) in s.get("twenty_results", {}) and not is_staff(inter.user):
                return await eph(inter, "이미 확정된 경기예요. 정정은 관리진에게 요청해 주세요.")
            if not is_staff(inter.user) and inter.user.id not in (s["captains"][a-1], s["captains"][b-1]):
                return await eph(inter, "해당 경기의 팀장 또는 관리진만 결과를 기록할 수 있어요.")
            # 양 팀장의 일치된 결과가 있어야 확정 (관리진은 바로 확정)
            if not is_staff(inter.user):
                pending = s.setdefault("twenty_votes", {}).setdefault(str(index), {})
                pending[str(inter.user.id)] = team
                if pending.get(str(s["captains"][a-1])) != team or pending.get(str(s["captains"][b-1])) != team:
                    save(s)
                    return await eph(inter, "결과를 접수했어요. 상대 팀장의 동일한 결과 확인을 기다려요.")
            s.setdefault("twenty_results", {})[str(index)] = team
            save(s)
            await inter.message.edit(embed=twenty_board_embed(s), view=twenty_board_view(s))
            return await eph(inter, f"{index+1}경기 {team}팀 승리를 확정했어요.")
        if not is_staff(inter.user):
            return await eph(inter, "내전 종료는 관리진만 할 수 있어요.")
        if len(s.get("twenty_results", {})) != 6:
            return await eph(inter, "6경기 결과가 모두 확정되어야 내전을 종료할 수 있어요.")
        scores = {n: 0 for n in range(1, 5)}
        for winner in s["twenty_results"].values():
            scores[int(winner)] += 1
        top = max(scores.values())
        winners = [n for n, wins in scores.items() if wins == top]
        lines = []
        for n in winners:
            names = [discord.utils.escape_markdown(inter.guild.get_member(uid).display_name if inter.guild.get_member(uid)
                     else next((e["name"] for e in s["participants"] if e["id"] == uid), str(uid)))
                     for uid in s[f"team{n}"]]
            lines.append(f"🏆 **{'공동 우승' if len(winners)>1 else '우승'} ({n}팀)**: " + " / ".join(names))
        await inter.channel.send(f"# 🏆 {now().month}월 {now().day}일 - 여 운 20인 내전\n"
                  "## 👏 금일 내전 참여해 주신 20명의 여 운 유저 여러분 모두 고생 많으셨습니다! "
                  "치열한 승부 끝에 최종 순위를 발표합니다\n\n" + "\n".join(lines),
                  allowed_mentions=discord.AllowedMentions.none())
        await inter.message.edit(embed=twenty_board_embed(s), view=None)
        await inter.channel.get_partial_message(s["message_id"]).edit(view=None)
        end_session(channel_id)
        await eph(inter, "20인 내전을 종료했어요. 전적과 포인트는 변경하지 않았어요.")


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
    if cid.startswith("nj20:"):
        return await handle_twenty_button(inter)
    if not cid.startswith("nj:"):
        return
    match = re.fullmatch(r"nj:([a-z0-9]+):(\d+)", cid)
    if not match or int(match[2]) != inter.channel_id or inter.message is None:
        return
    action, ch_id = match[1], int(match[2])
    guild, channel, user = inter.guild, inter.channel, inter.user

    # 모달을 띄우는 버튼은 락 없이 바로 응답 (3초 제한)
    if action in ("summon", "start"):
        s = sessions.get(ch_id)
        if not s or s["message_id"] != inter.message.id:
            return await eph(inter, "현재 내전 게시글의 버튼을 사용해 주세요.")
        if not is_staff(user):
            return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
        if action == "summon":
            if s["kind"] != "mobashi":
                return await eph(inter, "참가자 소집은 모바시 내전에서만 사용할 수 있어요.")
            if not s["participants"]:
                return await eph(inter, "아직 참가자가 없어요.")
            return await inter.response.send_modal(SummonModal(ch_id, inter.message.id))
        if s["stage"] != "full":
            return await eph(inter, f"참가자 {max_players(s)}명이 모인 뒤에 경기를 시작할 수 있어요.")
        if is_twenty(s):
            return await inter.response.send_modal(TwentyTeamModal(ch_id, inter.message.id))
        return await inter.response.send_modal(TeamModal(ch_id, inter.message.id))

    if not isinstance(channel, discord.TextChannel) or guild is None:
        return
    await inter.response.defer(ephemeral=True, thinking=True)
    async with locks[ch_id]:
        await apply_button(inter, guild, channel, user, ch_id, action)


async def apply_button(inter, guild, channel, user, ch_id, action):
    s = sessions.get(ch_id)
    if not s or s["message_id"] != inter.message.id:
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
        if len(s["participants"]) >= max_players(s):
            return await eph(inter, "참가 정원이 찼어요. **대기열 등록**을 이용해 주세요.")
        if in_w:
            s["waitlist"] = [e for e in s["waitlist"] if e["id"] != uid]
        s["participants"].append(entry)
        await log_roster_event(guild, channel, uid, "참가 등록", entry["name"])
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
        await log_roster_event(guild, channel, uid, "대기 등록", entry["name"])
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
        if s["group"] == "mobashi":
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
        if s["stage"] not in ("full", "playing"):
            return await eph(inter, "티어 정보 또는 경기 진행 단계에서만 사용할 수 있어요.")
        await delete_msg(channel, s["message_id"])
        end_session(ch_id)
        if is_twenty(s) and s.get("board_message_id"):
            try:
                await channel.get_partial_message(s["board_message_id"]).edit(view=None)
            except discord.HTTPException:
                pass
        await channel.send(embed=discord.Embed(
            title="🔒 기록 없이 종료", color=0x95A5A6,
            description=f"{s['label']} 내전이 전적 반영 없이 종료되었어요. (처리: {user.mention})"))

        await eph(inter, "전적 반영 없이 종료했어요.")

    elif action == "recordrequest":
        if not is_staff(user):
            return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
        if s["stage"] != "playing":
            return await eph(inter, "경기 진행 중에만 사용할 수 있어요.")
        await channel.send("내전 종료 후 승리팀 기재 부탁드립니다 ex)1팀 승")
        await eph(inter, "내전 기록 요청 메시지를 보냈어요.")

    elif action in ("win1", "win2"):
        if not is_staff(user):
            return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
        if is_twenty(s):
            return await eph(inter, "20인 내전은 현황판에서 결과를 기록해 주세요.")
        if s["stage"] not in ("playing", "finished"):
            return await eph(inter, "진행 중인 경기가 아니에요.")
        if s["stage"] == "playing":
            s = {**s, "stage": "finished", "winner": 1 if action == "win1" else 2}
            db.finish_match(s, s["winner"])
            sessions[ch_id] = s
        # 이미 결과가 기록된 경기라도 재호출할 수 있으며 빌지코인에서 중복 지급을 차단합니다.
        event_id=f'match:{guild.id}:{s["message_id"]}'
        queue_bilgecoin_event('/events/match-completed', {
            'guild_id':guild.id,'match_id':str(s['message_id']),
            'participant_ids':s['team1']+s['team2'],
            'channel_id':channel.id},event_id)
        reward_ok,reward_detail=await send_queued_event(event_id)
        await notify_match_reward(guild,channel,event_id,reward_ok,reward_detail)
        failed = await clean_match_newbies(guild, s["team1"] + s["team2"])
        await inter.message.edit(embed=result_embed(guild, s, s["winner"]), view=None)
        end_session(ch_id)
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
    if found and found[2]["label"] == "단판" and gkey == "mobashi" and fmt != "bo1":
        return await eph(inter, "단판 내전 채널에서는 진행방식을 '단판'으로 선택해 주세요.")
    if not found or found[0] != gkey:
        names = ", ".join(c["label"] for c in GROUPS[gkey]["channels"])
        return await eph(inter, f"이 명령어는 {GROUPS[gkey]['name']} 내전 신청방({names})에서만 사용할 수 있어요.")
    if inter.channel_id in sessions:
        return await eph(inter, "이 채널에 이미 진행 중인 모집/경기가 있어요. (`/내전현황`, `/내전모집취소`)")
    reserve_time = None
    if kind == "reserve":
        if not when:
            return await eph(inter, "예약 내전은 **모집시간**을 입력해 주세요. 예) 22:00, 오후10시")
        reserve_time = parse_time(when)
        if not reserve_time:
            return await eph(inter, "시간 형식을 알아보지 못했어요. 예) 22:00, 22시30분, 오후10시")
    await inter.response.defer(ephemeral=True)
    async with locks[inter.channel_id]:
        if inter.channel_id in sessions:
            return await eph(inter, "이 채널에 이미 진행 중인 모집/경기가 있어요.")
        await open_recruit(inter.channel, gkey, found[2]["label"], kind, fmt, reserve_time, inter.user.id)
    await eph(inter, "모집글을 생성했어요.")


@bot.tree.command(name="모바시내전모집", description="[관리진] 모바시 내전 참가자 모집 시작")
@app_commands.describe(진행방식="3판2선 또는 단판")
@app_commands.choices(진행방식=FORMAT_CHOICES)
async def cmd_mobashi(inter: discord.Interaction, 진행방식: app_commands.Choice[str]):
    await start_recruit(inter, "mobashi", "mobashi", 진행방식.value, None)


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


@bot.tree.command(name="20인내전모집", description="[관리진] 20인 내전 모집 시작 (전적/포인트 미반영)")
async def cmd_twenty(inter: discord.Interaction):
    if not has_role(inter.user, ["관리자", "보조관리자", "위장티어 담당", "내전관리자"]):
        return await eph(inter, "관리자·보조관리자·위장티어 담당·내전관리자만 사용할 수 있어요.")
    if not any(ch["text_id"] and ch["text_id"] == inter.channel_id for ch in config.TWENTY_CHANNELS):
        return await eph(inter, "20인 내전 신청방에서만 사용할 수 있어요. TWENTY_MATCH_CHANNEL_ID를 설정해 주세요.")
    if inter.channel_id in sessions:
        return await eph(inter, "이 채널에서 이미 내전이 진행 중이에요.")
    await inter.response.defer(ephemeral=True)
    async with locks[inter.channel_id]:
        if inter.channel_id in sessions:
            return await eph(inter, "이 채널에서 이미 내전이 진행 중이에요.")
        await open_recruit(inter.channel, "twenty", "20인", "mobashi", "bo1", None, inter.user.id)
    await eph(inter, "20인 내전 모집글을 생성했어요.")


@bot.tree.command(name="강제취소", description="잠수/탈퇴 유저를 강제로 명단에서 제외 (모집자 또는 관리진)")
async def cmd_force(inter: discord.Interaction, 유저: Optional[discord.Member] = None,
                    유저아이디: Optional[str] = None):
    if (유저 is None) == (유저아이디 is None):
        return await eph(inter, "유저 또는 유저아이디 중 하나를 입력해 주세요.")
    if 유저아이디 is not None and not re.fullmatch(r"[0-9]{15,20}", 유저아이디.strip()):
        return await eph(inter, "디스코드 사용자 ID를 숫자로 입력해 주세요.")
    uid = 유저.id if 유저 else int(유저아이디)
    await inter.response.defer(ephemeral=True)
    async with locks[inter.channel_id]:
        s = sessions.get(inter.channel_id)
        if not s or s["stage"] not in ("recruit", "full"):
            return await eph(inter, "이 채널에 모집 중인 내전이 없어요.")
        if not (is_staff(inter.user) or inter.user.id == s["host_id"]):
            return await eph(inter, "모집자 또는 관리진만 사용할 수 있어요.")
        where = await remove_player(inter.guild, inter.channel, s, uid, forced=True)
        if not where:
            return await eph(inter, "해당 유저는 명단에 없어요.")
        await refresh(inter.channel, s)
        await eph(inter, f"<@{uid}> 님을 {where} 명단에서 제외했어요.")


@bot.tree.command(name="내전현황", description="현재 모집 명단을 채팅창 아래에 새로 띄우기")
async def cmd_status(inter: discord.Interaction):
    async with locks[inter.channel_id]:
        s = sessions.get(inter.channel_id)
        if not s:
            # Recheck the persistent database before declaring a session missing.
            # Do not reconstruct participants from an embed: its displayed names
            # cannot reliably identify Discord users.
            row = db.conn.execute("SELECT data FROM sessions WHERE channel_id=?", (inter.channel_id,)).fetchone()
            if row:
                try:
                    recovered = json.loads(row["data"])
                    if recovered.get("guild_id") == inter.guild_id and recovered.get("stage") in ("recruit", "full", "playing", "finished"):
                        sessions[inter.channel_id] = recovered
                        s = recovered
                        logger.warning("내전현황: DB에서 세션 재복구 guild=%s channel=%s", inter.guild_id, inter.channel_id)
                except (ValueError, TypeError, KeyError):
                    logger.exception("내전현황: DB 세션 복구 실패 channel=%s", inter.channel_id)
        if not s:
            logger.warning("내전현황: 활성 세션 없음 guild=%s channel=%s db_path=%s", inter.guild_id, inter.channel_id, config.DB_PATH)
            return await eph(inter, "⚠️ 이 채널의 활성 내전 데이터가 확인되지 않아요. 기존 모집 임베드가 남아 있어도 DB에서 복구할 수 없는 상태일 수 있어요. 관리진은 `/내전데이터진단`을 실행해 주세요. 기존 모집글은 삭제하지 마세요.")
        await inter.response.defer(ephemeral=True)
        if s["stage"] in ("recruit", "full"):
            await refresh(inter.channel, s)
        current = sessions[inter.channel_id]
        await replace_post(inter.channel, current, dict(current))
        await eph(inter, "명단을 다시 띄웠어요.")
        if s["stage"] == "finished":
            end_session(inter.channel_id)


@bot.tree.command(name="내전데이터진단", description="[관리진] 현재 채널의 모집 DB 저장·복원 상태 확인")
async def cmd_session_diagnostic(inter: discord.Interaction):
    if not is_staff(inter.user):
        return await eph(inter, "관리진 전용 명령어입니다.")
    try:
        row = db.conn.execute("SELECT data FROM sessions WHERE channel_id=?", (inter.channel_id,)).fetchone()
        count = db.conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        mem = sessions.get(inter.channel_id)
        saved = json.loads(row["data"]) if row else None
        db_file = os.path.abspath(str(config.DB_PATH))
        volume = os.path.abspath(str(config.volume)) if config.volume else "미설정"
        lines = ["🔎 **내전 데이터 진단**",
                 f"채널: <#{inter.channel_id}>",
                 f"메모리 세션: {'있음' if mem else '없음'}",
                 f"DB 세션: {'있음' if saved else '없음'} (DB 전체 {count}건)",
                 f"DB 경로: `{db_file}`", f"Volume 설정: `{volume}`"]
        if saved:
            lines.extend([f"저장된 단계: `{saved.get('stage')}`",
                          f"저장된 참가자: {len(saved.get('participants', []))}명",
                          f"저장된 메시지 ID: `{saved.get('message_id')}`"])
        else:
            lines.append("⚠️ 이 DB에는 해당 채널 모집 기록이 없습니다. 기존 임베드만으로 참가자 ID를 안전하게 복구할 수 없습니다.")
        await inter.response.send_message("\n".join(lines), ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
    except (sqlite3.Error, ValueError, TypeError) as exc:
        logger.exception("내전 데이터 진단 실패")
        await eph(inter, f"진단 실패: `{type(exc).__name__}` (Railway 로그 확인)")


@bot.tree.command(name="기존모집글정리", description="[관리진] DB에서 사라진 기존 내전 모집글만 안전하게 삭제")
@app_commands.describe(메시지아이디="삭제할 기존 모집글의 Discord 메시지 ID", 확인="정말 삭제하려면 참을 선택")
async def cmd_cleanup_orphan(inter: discord.Interaction, 메시지아이디: str, 확인: bool = False):
    if not is_staff(inter.user):
        return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
    if not re.fullmatch(r"[0-9]{15,20}", 메시지아이디.strip()):
        return await eph(inter, "모집글의 메시지 ID를 숫자로 입력해 주세요.")
    if not isinstance(inter.channel, discord.TextChannel):
        return await eph(inter, "텍스트 채널에서 실행해 주세요.")
    async with locks[inter.channel_id]:
        if sessions.get(inter.channel_id) is not None:
            return await eph(inter, "이 채널에 활성 모집이 있습니다. `/내전모집취소`를 사용해 주세요.")
        row = db.conn.execute("SELECT 1 FROM sessions WHERE channel_id=?", (inter.channel_id,)).fetchone()
        if row:
            return await eph(inter, "DB에 모집 기록이 있습니다. `/내전현황`으로 복구한 후 정상 취소해 주세요.")
        try:
            msg = await inter.channel.fetch_message(int(메시지아이디))
        except discord.NotFound:
            return await eph(inter, "해당 메시지를 찾지 못했어요. 현재 채널의 메시지 ID인지 확인해 주세요.")
        except discord.Forbidden:
            return await eph(inter, "메시지 기록 보기 권한이 없어요.")
        except discord.HTTPException:
            logger.exception("고아 모집글 조회 실패")
            return await eph(inter, "메시지 조회에 실패했어요. Railway 로그를 확인해 주세요.")
        if not bot.user or msg.author.id != bot.user.id:
            return await eph(inter, "별빛지기가 작성한 메시지만 정리할 수 있어요.")
        titles = [str(e.title or "") for e in msg.embeds]
        if not any("내전" in title and ("모집" in title or "정보" in title) for title in titles):
            return await eph(inter, "내전 모집 임베드인지 확인할 수 없어 삭제하지 않았어요.")
        if not 확인:
            return await eph(inter, f"대상 확인: `{메시지아이디}` / 임베드: {titles[0] if titles else '-'}\n삭제하려면 같은 명령어에 `확인: 참`을 선택해 주세요. **참가자 기록은 복구되지 않습니다.**")
        try:
            await msg.delete()
        except discord.HTTPException:
            logger.exception("고아 모집글 삭제 실패")
            return await eph(inter, "메시지 삭제에 실패했어요. 메시지 관리 권한을 확인해 주세요.")
        logger.warning("고아 모집글 정리 guild=%s channel=%s message=%s admin=%s", inter.guild_id, inter.channel_id, 메시지아이디, inter.user.id)
        await eph(inter, f"🗑️ 기존 모집글 `{메시지아이디}`를 정리했어요. DB 기록은 변경하지 않았습니다.")


@bot.tree.command(name="내전모집취소", description="[관리진] 이 채널의 내전 모집/경기를 기록 없이 닫기")
async def cmd_close(inter: discord.Interaction):
    if not is_staff(inter.user):
        return await eph(inter, "관리자·보조관리자·매니저만 사용할 수 있어요.")
    async with locks[inter.channel_id]:
        s = sessions.get(inter.channel_id)
        if not s:
            return await eph(inter, "이 채널에 진행 중인 내전이 없어요.")
        if s["stage"] == "finished":
            return await eph(inter, "전적이 이미 반영됐어요. /내전현황으로 결과를 다시 표시할 수 있어요.")
        await inter.response.defer()
        await delete_msg(inter.channel, s["message_id"])
        await mark_status_done(inter.guild, s, "❌ 모집 취소")
        end_session(inter.channel_id)
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


def ranking_members(guild):
    rows = db.conn.execute("SELECT user_id,wins,losses FROM records WHERE wins+losses>0").fetchall()
    entries = [(guild.get_member(r["user_id"]), r["wins"], r["losses"]) for r in rows]
    entries = [(m, w, l) for m, w, l in entries if m is not None]
    entries.sort(key=lambda e: (0 if e[1]+e[2]>=50 else 1,
                   -(e[1]/(e[1]+e[2]) if e[1]+e[2]>=50 else e[1]+e[2]), -e[1]))
    return entries


class RankSearchModal(discord.ui.Modal, title="내전 랭킹 검색"):
    query = discord.ui.TextInput(label="닉네임 또는 Discord ID", max_length=100)
    async def on_submit(self, inter):
        rows = ranking_members(inter.guild)
        q = self.query.value.strip().lower()
        found = [(i, m, w, l) for i, (m, w, l) in enumerate(rows, 1)
                 if q in m.display_name.lower() or q == str(m.id)]
        if not found:
            return await eph(inter, "해당 유저의 랭킹을 찾지 못했어요.")
        i, m, w, l = found[0]
        await eph(inter, f"🔎 {m.display_name} · {i}위 / {len(rows)}명 · {w}승 {l}패 ({w/(w+l)*100:.1f}%)")


class RankView(discord.ui.View):
    def __init__(self, guild, owner_id, page=0):
        super().__init__(timeout=600)
        self.guild, self.owner_id, self.page = guild, owner_id, page
        self.update_buttons()

    def update_buttons(self):
        count = len(ranking_members(self.guild))
        self.previous.disabled = self.page == 0
        self.next_page.disabled = (self.page+1)*10 >= count
        self.counter.label = f"{self.page+1} / {max(1, (count+9)//10)}"

    def embed(self):
        rows = ranking_members(self.guild)
        lines = []
        for i, (m, w, l) in enumerate(rows[self.page*10:(self.page+1)*10], self.page*10+1):
            medal = ["🥇", "🥈", "🥉"][i-1] if i <= 3 else f"{i}위"
            lines.append(f"{medal} **{discord.utils.escape_markdown(m.display_name)}**\n"
                         f"└ 🟢 **{w}승 {l}패** ({w+l}전) · 승률 **{w/(w+l)*100:.1f}%**")
        return discord.Embed(title="🏆 롤이터 내전 명예의 전당",
             description=f"50판 이상은 승률 순, 50판 미만은 판수 순 · 전체 랭커 {len(rows)}명\n\n"
                         + ("\n\n".join(lines) if lines else "랭킹 기록이 없어요."), color=0xF1C40F)

    @discord.ui.button(label="이전", emoji="◀️", style=discord.ButtonStyle.secondary, row=0)
    async def previous(self, inter, button):
        self.page = max(0, self.page-1)
        self.update_buttons()
        await inter.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="1 / 1", style=discord.ButtonStyle.secondary, disabled=True, row=0)
    async def counter(self, inter, button):
        pass

    @discord.ui.button(label="다음", emoji="▶️", style=discord.ButtonStyle.secondary, row=0)
    async def next_page(self, inter, button):
        self.page += 1
        self.update_buttons()
        await inter.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="내 순위", emoji="📍", style=discord.ButtonStyle.primary, row=1)
    async def my_rank(self, inter, button):
        rows = ranking_members(self.guild)
        for i, (m, w, l) in enumerate(rows, 1):
            if m.id == inter.user.id:
                return await eph(inter, f"📍 내 순위: **{i}위 / {len(rows)}명** · {w}승 {l}패 ({w/(w+l)*100:.1f}%)")
        await eph(inter, "현재 서버 랭킹에 등록된 전적이 없어요.")

    @discord.ui.button(label="검색", emoji="🔍", style=discord.ButtonStyle.secondary, row=1)
    async def search(self, inter, button):
        await inter.response.send_modal(RankSearchModal())


@bot.tree.command(name="순위", description="🏆 서버 내전 명예의 전당 전체 랭킹")
async def cmd_rank(inter: discord.Interaction):
    view = RankView(inter.guild, inter.user.id)
    await inter.response.send_message(embed=view.embed(), view=view)


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


@bot.tree.command(name='포인트연동상태',description='[관리자] 빌지코인 자동 지급 대기 및 실패 상태 조회')
async def point_integration_status(inter:discord.Interaction, 재시도:bool=False):
    if not is_staff(inter.user):
        return await eph(inter,'관리진 전용 명령어입니다.')
    await inter.response.defer(ephemeral=True)
    with _outbox_conn() as conn:
        rows=conn.execute('''SELECT event_id,status,attempts,last_error,updated_at
            FROM bilgecoin_outbox ORDER BY updated_at DESC LIMIT 15''').fetchall()
        pending=conn.execute("SELECT COUNT(*) FROM bilgecoin_outbox WHERE status='pending'").fetchone()[0]
    lines=[f'연동 URL 설정: {"예" if os.getenv("BILGECOIN_URL") else "아니오"}',
           f'비밀키 설정: {"예" if os.getenv("INTEGRATION_SECRET") else "아니오"}',
           f'미전송/재시도 대기: {pending}건']
    for row in rows:
        if 재시도 and row['status']=='pending':
            ok,detail=await send_queued_event(row['event_id'])
            lines.append(f'{row["event_id"]}: {"성공" if ok else "실패"} {detail[:90]}')
        else:
            lines.append(f'{row["event_id"]}: {row["status"]} · 시도 {row["attempts"]}회 · {(row["last_error"] or "정상")[:90]}')
    await inter.followup.send('\n'.join(lines)[:1900],ephemeral=True)

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
    if not retry_bilgecoin_outbox.is_running():
        retry_bilgecoin_outbox.start()


@bot.event
async def on_member_remove(member: discord.Member):
    # 모집/대기 명단에서 서버를 나간 이용자만 제거합니다. 진행 중인 경기 기록은 유지합니다.
    for cid in list(sessions):
        async with locks[cid]:
            s = sessions.get(cid)
            if not s or s["guild_id"] != member.guild.id or s["stage"] not in ("recruit", "full"):
                continue
            in_participants = any(e["id"] == member.id for e in s["participants"])
            in_waitlist = any(e["id"] == member.id for e in s["waitlist"])
            if not (in_participants or in_waitlist):
                continue
            channel = member.guild.get_channel(cid)
            if isinstance(channel, discord.TextChannel):
                await log_roster_event(member.guild, channel, member.id, "서버 퇴장으로 명단 제외", member.display_name)
            s["participants"] = [e for e in s["participants"] if e["id"] != member.id]
            s["waitlist"] = [e for e in s["waitlist"] if e["id"] != member.id]
            if in_participants and s["waitlist"]:
                promoted = s["waitlist"].pop(0)
                s["participants"].append(promoted)
                if isinstance(channel, discord.TextChannel):
                    await log_roster_event(member.guild, channel, promoted["id"], "대기 → 참가 자동 승급", promoted["name"])
            channel = member.guild.get_channel(cid)
            if isinstance(channel, discord.TextChannel):
                try:
                    await refresh(channel, s)
                except discord.HTTPException:
                    logger.exception("퇴장한 참가자 명단 갱신 실패 (channel=%s, user=%s)", cid, member.id)
            else:
                save(s)


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
