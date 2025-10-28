import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# --- アプリケーションの基本設定 ---
st.set_page_config(
    page_title="Citibike Data Analysis",
    layout="wide"
)

# --- Snowparkセッションの取得 ---
try:
    session = get_active_session()
except Exception:
    st.error("Could not get Snowflake session. This app is designed to run in Streamlit in Snowflake.")
    st.stop()

# --- データ読み込み関数（キャッシュ付き） ---
@st.cache_data(ttl=600) # 10分間キャッシュ
def load_hourly_data():
    """時間ごとの走行回数と平均所要時間をロードする"""
    query = """
    SELECT
        DATE_TRUNC('hour', STARTTIME) AS "HOUR",
        COUNT(*) AS "NUM_TRIPS",
        AVG(TRIPDURATION)/60 AS "AVG_DURATION_MINS"
    FROM CITIBIKE.PUBLIC.TRIPS
    GROUP BY 1
    ORDER BY 1;
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=600)
def load_weather_data():
    """天候別の走行回数をロードする"""
    query = """
    SELECT
        WEATHER_CONDITIONS AS CONDITIONS,
        COUNT(*) AS NUM_TRIPS
    FROM CITIBIKE.PUBLIC.TRIPS
    LEFT OUTER JOIN WEATHER.PUBLIC.JSON_WEATHER_DATA_VIEW
        ON DATE_TRUNC('hour', OBSERVATION_TIME) = DATE_TRUNC('hour', STARTTIME)
    WHERE CONDITIONS IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC;
    """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=600)
def load_station_data():
    """出発ステーションごとの走行回数と地理情報をロードする"""
    query = """
    SELECT
        START_STATION_NAME,
        COUNT(*) AS NUM_TRIPS,
        AVG(START_STATION_LATITUDE) AS "LAT",
        AVG(START_STATION_LONGITUDE) AS "LON"
    FROM CITIBIKE.PUBLIC.TRIPS
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 100;
    """
    return session.sql(query).to_pandas()

# --- Cortex統合関数 ---
def get_data_insights(user_prompt: str, data_df: pd.DataFrame) -> str:
    """
    Snowflake Cortex Completeを使用して、データに関するユーザーの質問に回答する。
    """
    completion_model = "claude-3-7-sonnet"
    
    # プロンプトのテンプレート
    prompt_template = f"""
    あなたは、自転車共有サービス「Citibike」の熟練データアナリストです。
    あなたのタスクは、以下に提供される時間ごとの走行概要データセットに基づいて、ユーザーの質問に答えることです。
    回答は、簡潔で、分かりやすく整形され、質問に直接答えるものでなければなりません。

    ユーザーの質問: "{user_prompt}"

    対象データ:
    """
    
    data_string = data_df.to_string(index=False)

    # プロンプトが長くなりすぎるのを防ぐ
    if len(data_string) > 15000:
        data_string = data_df.head(100).to_string(index=False) + "\n... (data truncated)"
    
    # 最終的なプロンプトを作成
    full_prompt = prompt_template + data_string

    # プロンプト内のシングルクォートを、SQLで安全な '' にエスケープする
    sanitized_prompt = full_prompt.replace("'", "''")

    # サニタイズしたプロンプトを、SQLの標準的なシングルクォートで囲む
    prompt_sql = f"""
    SELECT snowflake.cortex.complete(
        '{completion_model}',
        '{sanitized_prompt}'
    ) as response
    """

    try:
        # クエリを実行し、結果をPandas DataFrameに変換
        result_df = session.sql(prompt_sql).to_pandas()
        
        # DataFrameからレスポンス文字列を抽出
        if not result_df.empty:
            response = result_df['RESPONSE'][0]
            return response
        else:
            return "Cortexから応答がありませんでした。"
            
    except Exception as e:
        # エラーデバッグ用に、生成されたSQLを表示してみる
        st.error(f"An error occurred while querying Cortex: {e}")
        st.code(prompt_sql, language="sql")
        return "Cortexへの問い合わせ中にエラーが発生しました。"

# --- メインアプリケーションUI ---
st.title('🚲 Citibike Data Analysis Dashboard')

st.sidebar.title("Navigation")
analysis_options = ["📈 Hourly Trip Analysis",
                    "🌦️ Analysis by Weather",
                    "🗺️ Station Popularity Map",
                    "💬 Natural Language Query (Cortex)"]
selected_option = st.sidebar.selectbox(
    "Choose an analysis view:",
    analysis_options
)

# --- 表示コンテンツの切り替え ---

if selected_option == "📈 Hourly Trip Analysis":
    st.header("Hourly Trip Analysis")
    st.write("This chart shows the total number of bike trips for each hour.")
    with st.spinner("Loading hourly data..."):
        hourly_df = load_hourly_data()
        if not hourly_df.empty:
            st.line_chart(hourly_df.set_index('HOUR'), y="NUM_TRIPS")
            with st.expander("View Raw Data"):
                st.dataframe(hourly_df)
        else:
            st.warning("No hourly data available.")

elif selected_option == "🌦️ Analysis by Weather":
    st.header("Trip Count by Weather Conditions")
    st.write("This chart displays how different weather conditions affect the number of bike trips.")
    with st.spinner("Loading weather data..."):
        weather_df = load_weather_data()
        if not weather_df.empty:
            fig = px.bar(
                weather_df,
                x='CONDITIONS',
                y='NUM_TRIPS',
                color='CONDITIONS',
                title='Number of Trips for Each Weather Condition'
            )
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("View Raw Data"):
                st.dataframe(weather_df)
        else:
            st.warning("No weather data available.")

elif selected_option == "🗺️ Station Popularity Map":
    st.header("Top 100 Most Popular Start Stations")
    st.write("This map shows the locations of the 100 most frequently used start stations.")
    with st.spinner("Loading station data..."):
        station_df = load_station_data()
        if not station_df.empty:
            # NaN値を削除
            station_df.dropna(subset=['LAT', 'LON'], inplace=True)
            st.map(station_df)
            with st.expander("View Raw Data"):
                st.dataframe(station_df)
        else:
            st.warning("No station data available.")

elif selected_option == "💬 Natural Language Query (Cortex)":
    st.header("Ask a Question About Your Data")
    st.info("This feature uses Snowflake Cortex AI to answer questions about the hourly trip data. Note that responses are AI-generated and may require verification.")

    # Cortexに渡すためのデータをロード
    summary_df = load_hourly_data()

    user_question = st.text_area("Your question:", height=100, placeholder="e.g., Which hours are the busiest for bike trips? What is the average trip duration overall?")

    if st.button("Get Insights"):
        if user_question:
            with st.spinner("Analyzing data and generating insights..."):
                insights = get_data_insights(user_question, summary_df)
                st.subheader("AI-Generated Insights:")
                st.markdown(insights)
        else:
            st.warning("Please enter a question.")