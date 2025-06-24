import asyncio
import io
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from data_processor import DataProcessor
from config import Config
from datetime import datetime

class TelegramBot:
    def __init__(self):
        self.config = Config()
        self.data_processor = DataProcessor()
        self.app = Application.builder().token(self.config.TELEGRAM_BOT_TOKEN).build()
        self._setup_handlers()
    
    def _setup_handlers(self):
        """명령어 핸들러 설정"""
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("data", self.get_data_command))
        self.app.add_handler(CommandHandler("stats", self.get_stats_command))
        self.app.add_handler(CommandHandler("csv", self.get_csv_command))
        self.app.add_handler(CommandHandler("tm", self.get_inflow_data_command))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시작 명령어"""
        welcome_message = """
🤖 **유입 결과 분석 봇에 오신 것을 환영합니다!**

사용 가능한 명령어:
• /help - 도움말 보기
• /data - 데이터 요약 정보 보기
• /stats - 상세 통계 보기
• /csv - CSV 파일 다운로드
• /tm [구역명] - 특정 구역의 데이터만 보기

시작하려면 /data 명령어를 사용해보세요!
        """
        await update.message.reply_text(welcome_message, parse_mode=ParseMode.MARKDOWN)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """도움말 명령어"""
        help_message = """
📖 **명령어 도움말**

🔹 **/start** - 봇 시작 및 환영 메시지
🔹 **/help** - 이 도움말 보기
🔹 **/data** - 기본 데이터 정보 및 요약
🔹 **/stats** - 유입별, 티엠 결과별 상세 통계
🔹 **/csv** - 분석된 데이터를 CSV 파일로 다운로드
🔹 **/tm [구역명]** - 특정 구역의 데이터만 조회

📝 **사용 예시:**
• `/tm 1` - 유입이 '1구역'인 데이터만 조회
• `/tm 2` - 유입이 '2구역'인 데이터만 조회

ℹ️ **참고사항:**
• 데이터는 Google Sheets에서 실시간으로 가져옵니다
• 유입이 빈 값이거나 'J'인 데이터는 제외됩니다
• 티엠 결과는 '신규', '부재중/재티엠', '티엠 예약', '장기'로 분류됩니다
        """
        await update.message.reply_text(help_message, parse_mode=ParseMode.MARKDOWN)
    
    async def get_data_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """데이터 요약 정보 명령어"""
        try:
            await update.message.reply_text("📊 데이터를 가져오는 중...")
            
            # 데이터 가져오기 및 처리
            raw_df = self.data_processor.fetch_data()
            processed_df = self.data_processor.process_data(raw_df)
            
            # 요약 통계 생성
            summary = self.data_processor.get_summary_stats(processed_df)
            
            await update.message.reply_text(summary, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            error_message = f"❌ 데이터를 가져오는 중 오류가 발생했습니다:\n{str(e)}"
            await update.message.reply_text(error_message)
    
    async def get_stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """상세 통계 명령어"""
        try:
            await update.message.reply_text("📈 상세 통계를 생성하는 중...")
            
            # 데이터 가져오기 및 처리
            raw_df = self.data_processor.fetch_data()
            processed_df = self.data_processor.process_data(raw_df)
            
            if '유입' not in processed_df.columns or '티엠 결과' not in processed_df.columns:
                await update.message.reply_text("❌ 필요한 컬럼을 찾을 수 없습니다.")
                return
            
            # 유입별 상세 통계
            stats_message = "📊 **상세 통계**\n\n"
            
            inflow_groups = processed_df.groupby('유입')
            
            for inflow_type, inflow_group in inflow_groups:
                stats_message += f"**【{inflow_type}구역】** ({len(inflow_group)}명)\n"
                
                tm_result_groups = inflow_group.groupby('티엠 결과')
                for tm_result, tm_group in tm_result_groups:
                    display_result = "신규" if tm_result == '' else tm_result
                    names = [name for name in tm_group['이름'].values if name.strip()]
                    
                    stats_message += f"  ▪️ {display_result}: {len(tm_group)}명\n"
                    
                    if names:
                        # 이름이 너무 많으면 일부만 표시
                        if len(names) > 10:
                            displayed_names = names[:10]
                            stats_message += f"    👤 {', '.join(displayed_names)} 외 {len(names)-10}명\n"
                        else:
                            stats_message += f"    👤 {', '.join(names)}\n"
                
                stats_message += "\n"
            
            # 메시지 길이 제한 (텔레그램 4096자 제한)
            if len(stats_message) > 4000:
                stats_message = stats_message[:4000] + "\n... (메시지가 너무 길어 생략됨)"
            
            await update.message.reply_text(stats_message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            error_message = f"❌ 통계를 생성하는 중 오류가 발생했습니다:\n{str(e)}"
            await update.message.reply_text(error_message)
    
    async def get_csv_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """CSV 파일 다운로드 명령어"""
        try:
            await update.message.reply_text("📄 CSV 파일을 생성하는 중...")
            
            # 데이터 가져오기 및 처리
            raw_df = self.data_processor.fetch_data()
            processed_df = self.data_processor.process_data(raw_df)
            analysis_df = self.data_processor.create_analysis_data(processed_df)
            
            # CSV 버퍼 생성
            csv_data = self.data_processor.create_csv_buffer(analysis_df)
            
            # 파일명 생성
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f"유입결과_분석_{current_date}.csv"
            
            # CSV 파일 전송
            csv_file = io.BytesIO(csv_data)
            csv_file.name = filename
            
            await update.message.reply_document(
                document=csv_file,
                filename=filename,
                caption=f"📊 유입 결과 분석 데이터\n📅 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            
        except Exception as e:
            error_message = f"❌ CSV 파일을 생성하는 중 오류가 발생했습니다:\n{str(e)}"
            await update.message.reply_text(error_message)
    
    async def get_inflow_data_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """특정 유입 데이터 조회 명령어"""
        try:
            # 명령어 인자 확인
            if not context.args:
                await update.message.reply_text(
                    "❌ 유입명을 입력해주세요.\n\n"
                    "사용법: `/TM [유입명]`\n"
                    "예시: `/TM 1구역` 또는 `/TM 인스타그램`",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # 유입명 추출 (공백으로 구분된 여러 단어 합치기)
            inflow_name = " ".join(context.args)
            
            await update.message.reply_text(f"🔍 '{inflow_name}구역' 유입 데이터를 조회하는 중...")
            
            # 데이터 가져오기 및 처리
            raw_df = self.data_processor.fetch_data()
            processed_df = self.data_processor.process_data(raw_df)
            
            if '유입' not in processed_df.columns:
                await update.message.reply_text("❌ '유입' 컬럼을 찾을 수 없습니다.")
                return
            
            # 특정 유입만 필터링
            inflow_df = processed_df[processed_df['유입'] == inflow_name]
            
            if inflow_df.empty:
                # 사용 가능한 유입 목록 제공
                available_inflows = processed_df['유입'].unique().tolist()
                available_list = "\n".join([f"• {inflow}" for inflow in available_inflows])
                
                await update.message.reply_text(
                    f"❌ '{inflow_name}구역' 유입을 찾을 수 없습니다.\n\n"
                    f"📋 **사용 가능한 유입 목록:**\n{available_list}",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            # 결과 메시지 생성
            result_message = f"📊 **【{inflow_name}구역】 유입 데이터**\n\n"
            result_message += f"총 인원: {len(inflow_df)}명\n\n"
            
            if '티엠 결과' in inflow_df.columns:
                # 모든 가능한 티엠 결과 상태 정의
                all_tm_results = ['', '부재중/재티엠', '티엠 예약', '장기']
                
                # 티엠 결과별 그룹화
                tm_result_groups = inflow_df.groupby('티엠 결과')
                
                # 모든 상태에 대해 처리 (없는 상태는 0명으로 표시)
                for tm_result in all_tm_results:
                    display_result = "신규" if tm_result == '' else tm_result
                    
                    if tm_result in tm_result_groups.groups:
                        # 해당 상태의 데이터가 있는 경우
                        tm_group = tm_result_groups.get_group(tm_result)
                        result_message += f"**▶ {display_result}** ({len(tm_group)}명)\n"
                        
                        # 이름 목록
                        if '이름' in tm_group.columns:
                            names = [name.strip() for name in tm_group['이름'].values if name.strip()]
                            
                            if names:
                                # 이름이 많으면 줄바꿈해서 표시
                                if len(names) > 8:
                                    # 8명씩 줄바꿈
                                    name_lines = []
                                    for i in range(0, len(names), 8):
                                        batch = names[i:i+8]
                                        name_lines.append(", ".join(batch))
                                    result_message += f"👤 {chr(10).join(name_lines)}\n\n"
                                else:
                                    result_message += f"👤 {', '.join(names)}\n\n"
                            else:
                                result_message += "👤 (이름 정보 없음)\n\n"
                        else:
                            result_message += f"👤 {len(tm_group)}명 (이름 컬럼 없음)\n\n"
                    else:
                        # 해당 상태의 데이터가 없는 경우 (0명)
                        result_message += f"**▶ {display_result}** (0명)\n\n"
            else:
                result_message += "⚠️ '티엠 결과' 정보를 찾을 수 없습니다.\n"
            
            # 메시지 길이 제한
            if len(result_message) > 4000:
                result_message = result_message[:4000] + "\n... (메시지가 너무 길어 생략됨)"
            
            await update.message.reply_text(result_message, parse_mode=ParseMode.MARKDOWN)
                
        except Exception as e:
            error_message = f"❌ '{inflow_name if 'inflow_name' in locals() else '알 수 없음'}' 유입 데이터를 조회하는 중 오류가 발생했습니다:\n{str(e)}"
            await update.message.reply_text(error_message)
    
    def run(self):
        """봇 실행"""
        print("🤖 텔레그램 봇을 시작합니다...")
        self.app.run_polling()