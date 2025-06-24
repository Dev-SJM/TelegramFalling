from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    # 텔레그램 봇 토큰 (BotFather에서 발급받은 토큰)
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    
    # Google Sheets 설정
    JSON_KEY_PATH = "thinking-mesh-463900-v6-14ecf2a1fe53.json"
    SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/126-_OsJE5I0flNe0l0miwz3IAvXT60LAgY-BtbdWQKA/edit?gid=1438756663#gid=1438756663"
    WORKSHEET_NAME = "유입 결과"
    
    # 데이터 필터링 설정
    TARGET_VALUES = ['', '부재중/재티엠', '티엠 예약', '장기']
    EXCLUDED_INFLOW_VALUES = ['', 'J']
    
    # CSV 파일 설정
    SELECTED_COLUMNS = ['이름', '유입', '티엠 결과']