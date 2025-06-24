from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from datetime import datetime
import io
from config import Config

class DataProcessor:
    def __init__(self):
        self.config = Config()
        self.gc = None
        self._authorize()
    
    def _authorize(self):
        """Google Sheets API 인증"""
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        credential = ServiceAccountCredentials.from_json_keyfile_name(
            self.config.JSON_KEY_PATH, scope
        )
        self.gc = gspread.authorize(credential)
    
    def fetch_data(self):
        """Google Sheets에서 데이터 가져오기"""
        try:
            doc = self.gc.open_by_url(self.config.SPREADSHEET_URL)
            sheet = doc.worksheet(self.config.WORKSHEET_NAME)
            raw_data = sheet.get_all_values()
            
            # 데이터프레임 생성
            df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
            
            # 빈 열 제거
            df_cleaned = df.loc[:, (df != '').any(axis=0)]
            
            return df_cleaned
        except Exception as e:
            raise Exception(f"데이터 가져오기 실패: {str(e)}")
    
    def process_data(self, df):
        """데이터 필터링 및 처리"""
        try:
            # '티엠 결과' 열 확인 및 필터링
            if '티엠 결과' not in df.columns:
                raise Exception("'티엠 결과' 열을 찾을 수 없습니다.")
            
            # 특정 값들만 필터링
            filtered_df = df[df['티엠 결과'].isin(self.config.TARGET_VALUES)]
            
            # 필요한 컬럼만 선택
            available_columns = [col for col in self.config.SELECTED_COLUMNS 
                               if col in filtered_df.columns]
            
            if not available_columns:
                raise Exception("필요한 컬럼을 찾을 수 없습니다.")
            
            final_df = filtered_df[available_columns]
            
            # 유입 필터링 (빈 값, 'J' 제외)
            if '유입' in final_df.columns:
                final_df = final_df[~final_df['유입'].isin(self.config.EXCLUDED_INFLOW_VALUES)]
            
            return final_df
        except Exception as e:
            raise Exception(f"데이터 처리 실패: {str(e)}")
    
    def create_analysis_data(self, df):
        """분석 데이터 생성"""
        try:
            if '유입' not in df.columns or '티엠 결과' not in df.columns:
                return df
            
            # 유입별로 그룹화하여 분석 데이터 생성
            analysis_data = []
            inflow_groups = df.groupby('유입')
            
            for inflow_type, inflow_group in inflow_groups:
                tm_result_groups = inflow_group.groupby('티엠 결과')
                for tm_result, tm_group in tm_result_groups:
                    for idx, person in tm_group.iterrows():
                        analysis_data.append({
                            '유입': inflow_type,
                            '티엠결과': tm_result if tm_result != '' else '신규',
                            '이름': person.get('이름', '')
                        })
            
            return pd.DataFrame(analysis_data)
        except Exception as e:
            raise Exception(f"분석 데이터 생성 실패: {str(e)}")
    
    def create_csv_buffer(self, df):
        """CSV 파일을 메모리 버퍼로 생성"""
        try:
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, encoding='utf-8')
            buffer.seek(0)
            return buffer.getvalue().encode('utf-8-sig')
        except Exception as e:
            raise Exception(f"CSV 생성 실패: {str(e)}")
    
    def get_summary_stats(self, df):
        """요약 통계 생성"""
        try:
            if '유입' not in df.columns or '티엠 결과' not in df.columns:
                return "통계를 생성할 수 없습니다."
            
            total_count = len(df)
            inflow_counts = df['유입'].value_counts()
            
            summary = f"📊 **데이터 요약**\n"
            summary += f"총 인원: {total_count}명\n\n"
            summary += "**구역별 현황:**\n"
            
            for inflow_type, count in inflow_counts.items():
                percentage = (count / total_count) * 100
                summary += f"• {inflow_type}구역: {count}명 ({percentage:.1f}%)\n"
            
            # 티엠 결과별 현황
            tm_result_counts = df['티엠 결과'].value_counts()
            summary += "\n**티엠 결과별 현황:**\n"
            
            for tm_result, count in tm_result_counts.items():
                display_result = "신규" if tm_result == '' else tm_result
                percentage = (count / total_count) * 100
                summary += f"• {display_result}: {count}명 ({percentage:.1f}%)\n"
            
            return summary
        except Exception as e:
            return f"통계 생성 실패: {str(e)}"