# =====================================================================
#  내전 모집 봇 설정 파일
#  - 채널 ID: 디스코드 설정 > 고급 > 개발자 모드 ON → 채널 우클릭 > "채널 ID 복사"
#  - 값이 0 인 항목은 "사용 안 함"으로 처리됩니다.
# =====================================================================

import os
from pathlib import Path


def _load_env(path=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")):
    """같은 폴더의 .env 파일을 읽어 환경변수로 등록 (GitHub에 토큰이 올라가지 않도록)"""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

# 봇 토큰 / 서버 ID 는 .env 파일에 적어주세요 (.env.example 참고)
TOKEN = os.getenv("DISCORD_TOKEN", "")
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)

# ---------------------------------------------------------------------
#  역할 설정 (역할 "이름" 기준)
# ---------------------------------------------------------------------
# 모집 명령어 / 소집 / 팀 입력 / 승리 기록 / 관리 명령어를 쓸 수 있는 역할
STAFF_ROLE_NAMES = ["관리자", "보조관리자", "매니저"]

# '관리자 호출' 버튼을 누르면 언급되는 역할
CALL_ADMIN_ROLE_NAMES = ["관리자", "보조관리자"]

# /뉴비정리 로 회수할 역할 이름과 기준 판수
NEWBIE_ROLE_NAME = "뉴비"
NEWBIE_GAMES = 3

# 저티어 / 고티어 모집글 생성 시 자동 언급할 역할
LOW_TIER_ROLES = ["브론즈", "실버", "골드", "플레티넘", "에메랄드"]
HIGH_TIER_ROLES = ["마스터", "그랜드마스터", "챌린저"]

# ---------------------------------------------------------------------
#  채널 설정
#  text_id  : 내전-신청방(텍스트 채널) ID  ← 모집글이 올라가는 곳
#  lobby_id : 대기방(음성 채널) ID        ← 소집 메시지에 링크로 표시 (없으면 0)
#  리스트 순서 = 자동 모집글 생성 순서 (A → E)
# ---------------------------------------------------------------------
MOBASHI_CHANNELS = [
    {"label": "A", "text_id": 0, "lobby_id": 0},
    {"label": "B", "text_id": 0, "lobby_id": 0},
    {"label": "C", "text_id": 0, "lobby_id": 0},
    {"label": "D", "text_id": 0, "lobby_id": 0},
    {"label": "E", "text_id": 0, "lobby_id": 0},
]

SINGLE_CHANNELS = [{
    "label": "단판",
    "text_id": int(os.getenv("SINGLE_MATCH_CHANNEL_ID") or 0),
    "lobby_id": int(os.getenv("SINGLE_MATCH_LOBBY_ID") or 0),
}]

LOW_CHANNELS = [
    {"label": "저티어", "text_id": 0, "lobby_id": 0},   # 저티어내전 | 신청방
    {"label": "저티어B", "text_id": 0, "lobby_id": 0},  # 저티어내전b | 신청방
]

HIGH_CHANNELS = [
    {"label": "고티어", "text_id": 0, "lobby_id": 0},   # 고티어내전 | 신청방
    {"label": "고티어B", "text_id": 0, "lobby_id": 0},  # 고티어내전b | 신청방
]

# 내전-모집-현황 채널 ID ("#a | 내전-신청방 모바시 3판2선 내전 인원 모집 중" 메시지가 올라감)
STATUS_CHANNEL_ID = 0

# 취소 명단이 자동으로 올라갈 관리자 채널
#  - ID를 넣으면 ID 우선, ID가 0이면 아래 채널 "이름"으로 찾습니다.
#  - 취소 기록은 모집글의 '📝 취소 기록' 칸에도 함께 표시됩니다.
CANCEL_LOG_CHANNEL_ID = 0
CANCEL_LOG_CHANNEL_NAME = "취소-명단"

# ---------------------------------------------------------------------
#  모집 규칙
# ---------------------------------------------------------------------
MAX_PLAYERS = 10
MAX_WAITING = 3

# 취소 기록 시간대 (CANCEL_WARN_COUNT 조건도 동시에 충족해야 기록)
CANCEL_RECORD_START = "20:00"
CANCEL_RECORD_END = "06:00"

# 취소 기록 시간대에 이 인원 이상 모인 상태에서 참가를 취소하면 기록
CANCEL_WARN_COUNT = 8

# '내전 참가자 소집' 입력창에 기본으로 채워질 시간 (현재 시각 + N분)
SUMMON_DEFAULT_MINUTES = 10

# '관리자 호출' 버튼 재사용 대기시간(초) - 도배 방지
CALL_ADMIN_COOLDOWN = 60

# /순위 승률 정렬 시 최소 판수
RANK_MIN_GAMES = 5

# ---------------------------------------------------------------------
#  안내 문구 (자유롭게 수정 가능)
#  {start}/{end} = 취소 기록 시간대, {count} = 기록 기준 인원, {gather} = 예약 집합 시각
# ---------------------------------------------------------------------

WINNER_REPORT_PROMPT = "🏁 내전이 끝났어요! 이 채널에 승리한 팀을 적어주세요. 예) **1팀 승리**"

volume = os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
DB_PATH = os.getenv("DB_PATH") or str(Path(volume or Path(__file__).parent) / "naejeon.db")


def _set_channel_ids(prefix, channels):
    for channel in channels:
        key = f"{prefix}_{channel['label']}" if prefix == "MOBASHI" else prefix
        if channel["label"].endswith("B") and prefix != "MOBASHI":
            key += "_B"
        for field in ("text_id", "lobby_id"):
            channel[field] = int(os.getenv(f"{key}_{field.upper()}") or channel[field])


_set_channel_ids("MOBASHI", MOBASHI_CHANNELS)
_set_channel_ids("LOW", LOW_CHANNELS)
_set_channel_ids("HIGH", HIGH_CHANNELS)
STATUS_CHANNEL_ID = int(os.getenv("STATUS_CHANNEL_ID") or STATUS_CHANNEL_ID)
PLATINUM_ROLE_ID = int(os.getenv("PLATINUM_ROLE_ID") or 0)
CANCEL_LOG_CHANNEL_ID = int(os.getenv("CANCEL_LOG_CHANNEL_ID") or CANCEL_LOG_CHANNEL_ID)


# 주간 승률 공개 스레드를 만들 텍스트 채널 ID (0이면 자동 게시 끔)
WEEKLY_WINRATE_CHANNEL_ID = int(os.getenv("WEEKLY_WINRATE_CHANNEL_ID") or 0)
