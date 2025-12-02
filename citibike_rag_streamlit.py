# streamlit_app.py  (Streamlit in Snowflake - Dark Theme Friendly)
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root

# =========================================
# 設定（必要に応じて変更）
# =========================================
SERVICE_DB = "CITIBIKE"
SERVICE_SCHEMA = "PUBLIC"
SERVICE_NAME = "CITIBIKE_TERMS_SEARCH"   # 既存の Cortex Search Service 名
TOP_K = 4                                # 取得するコンテキスト数の上限（調整可）

DEFAULT_SYSTEM_PROMPT = """あなたは自転車シェアリングサービスCiti BikeのカスタマーサポートAIアシスタントです。
「お客様からの質問」はお客様が実際に投げかけた質問です。
「利用規約抜粋」はCiti Bike利用規約から抽出された抜粋です。
「お客様からの質問」に対して、「利用規約抜粋」の情報に基づいて、回答を生成してください。
なお、回答にあたっては以下の「ルール」を守ること。

「ルール」
・必ず最初に、質問頂いたことに対する御礼を述べること。
・必ず「利用規約抜粋」の根拠に基づいて簡潔かつ正確に回答してください。
・根拠が十分でない場合は、推測せず「手元の情報では断定できません」と述べてください。
・必要に応じて注意事項や手順を番号付きで示してください。
・出力は日本語で返してください。
"""

EXAMPLE_QUESTIONS = [
    "どのような方法で課金がされますか？",
    "ヘルメットの着用は義務ですか？",
    "自転車に不備があった場合、どうすればよいですか？",
    "返却時にロック施錠を忘れた場合の対応は？",
    "利用中に事故が発生した際の流れを知りたいです。",
    "今日の晩御飯には何がオススメですか。"
]

# 利用モデル候補
MODEL_CANDIDATES = [
    "claude-4-sonnet",
    "claude-3-7-sonnet",
    "mistral-large2",
    "openai-gpt-4.1",
    "snowflake-arctic"
]

# =========================================
# ユーティリティ
# =========================================
def get_session_and_service():
    session = get_active_session()
    root = Root(session)
    service = (
        root
        .databases[SERVICE_DB]
        .schemas[SERVICE_SCHEMA]
        .cortex_search_services[SERVICE_NAME]
    )
    return session, service

def retrieve_context(service, user_query: str, limit: int = TOP_K):
    # Cortex Search からコンテキスト抽出
    resp = service.search(
        query=user_query,
        columns=["CHUNK_TEXT"],
        limit=limit
    )
    results = resp.to_dict().get("results", [])
    chunks = []
    for r in results:
        chunk = r.get("CHUNK_TEXT", "")
        page = r.get("PAGE_INDEX", None)
        kw = r.get("EXTRACTED_WORD", None)
        header = []
        if page is not None:
            header.append(f"page_index={page}")
        if kw:
            header.append(f"keyword={kw}")
        meta = " | ".join(header) if header else ""
        chunks.append(f"[{meta}]\n{chunk}" if meta else chunk)
    context_text = "\n\n---\n\n".join(chunks)
    return context_text, results

def call_ai_complete(session, model: str, final_prompt: str) -> str:
    df = session.sql(
        "SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(?, ?) AS RESPONSE",
        params=[model, final_prompt]
    )
    row = df.collect()[0]
    return row["RESPONSE"]

def build_final_prompt(system_prompt: str, user_query: str, context_text: str) -> str:
    final_prompt = f"""

{system_prompt}

[お客様からの質問]
{user_query}

[利用規約抜粋]
{context_text if context_text.strip() else "(該当するコンテキストが見つかりませんでした)"} 

"""
    return final_prompt

def normalize_for_display(s: str) -> str:
    """AI_COMPLETE応答やテキストに含まれる \\n 等の表示用エスケープを実文字に正規化"""
    if not isinstance(s, str):
        return "" if s is None else str(s)
    s = s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\t", "\t")
    return s

def init_state():
    if "rag" not in st.session_state:
        st.session_state["rag"] = {
            "ready": False,
            "query": "",
            "context": "",
            "answer": "",
            "final_prompt": "",
            "model": MODEL_CANDIDATES[0],
            "system_prompt": DEFAULT_SYSTEM_PROMPT,
            "satisfaction": None
        }

# =========================================
# Dark背景向けの落ち着いた配色（CSS）
# =========================================
DARK_CSS = """
<style>
:root {
  /* background & surfaces */
  --bg-1: #0b1220;     /* 最背面 */
  --bg-2: #0f172a;     /* セクション背景 */
  --surface: #111827;  /* カード */
  --surface-2: #0f1b2d;
  --border: #1f2937;

  /* text */
  --text: #e5e7eb;
  --muted: #9ca3af;
  --accent: #93c5fd;   /* sky-300 */
  --accent-2: #60a5fa; /* sky-400 */

  /* chips */
  --chip: #1f2937;
  --chip-hover: #334155;

  /* answer box */
  --answer-bg: #0b1220;
  --answer-border: #334155;
}

/* 全体の背景調整（黒系に馴染ませつつ読みやすいコントラスト） */
.block-container {
  padding-top: 1rem;
  padding-bottom: 1.6rem;
}

/* 背景色補助（Streamlitの背景が黒想定のためカードとテキストのコントラストを強める） */
header, footer, .main, .block-container {
  color: var(--text);
}

/* ヘッダー（落ち着いたグラデでほんのり） */
.header-soft {
  background: radial-gradient(circle at 20% 10%, rgba(96,165,250,0.12), transparent 55%),
              radial-gradient(circle at 85% 0%, rgba(147,197,253,0.10), transparent 45%);
  border: 1px solid var(--border);
  border-radius: 14px;
  padding: 0.9rem 1.0rem;
}
.header-title { font-weight: 800; font-size: 1.2rem; color: var(--accent-2); }
.header-sub   { color: var(--muted); font-size: 0.95rem; margin-top: 0.2rem; }

/* カード */
.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 1.0rem 1.0rem;
  margin: 0.7rem 0;
  box-shadow: 0 6px 14px rgba(0,0,0,0.25);
}

/* セクション見出し・補助 */
.section-title { font-weight: 700; font-size: 1.02rem; margin-bottom: 0.5rem; color: var(--accent); }
.small { color: var(--muted); font-size: 0.9rem; }

/* チップ風ボタン */
.chip-btn > button {
  background: var(--chip) !important;
  color: var(--text) !important;
  border: 1px solid var(--border) !important;
  border-radius: 999px !important;
  padding: 0.35rem 0.8rem !important;
  font-size: 0.92rem !important;
}
.chip-btn > button:hover {
  background: var(--chip-hover) !important;
}

/* 回答表示 */
.answer-box {
  background: var(--answer-bg);
  border: 1px dashed var(--answer-border);
  border-radius: 12px;
  padding: 0.85rem 0.95rem;
  line-height: 1.7;
  color: var(--text);
}

/* expander 内テキストも見やすく */
.streamlit-expanderHeader { color: var(--text) !important; }
.streamlit-expanderContent { color: var(--text) !important; }

/* フッター */
.footer-note {
  text-align: center; color: var(--muted);
  font-size: 0.85rem; margin-top: 1.2rem;
}
</style>
"""

# =========================================
# UI 構成
# =========================================
st.set_page_config(page_title="Citi Bike サポートチャット（RAG）", page_icon="🚲", layout="centered")
init_state()
st.markdown(DARK_CSS, unsafe_allow_html=True)

# ヘッダー
st.markdown(
    """
<div class="header-soft">
  <div class="header-title">🚲 Citi Bike サポートチャット</div>
  <div class="header-sub">RAG検索チャットボット</div>
</div>
""",
    unsafe_allow_html=True
)

# モデル選択
with st.container():
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🤖 回答AIモデルの選択</div>', unsafe_allow_html=True)
    st.session_state["rag"]["model"] = st.selectbox(
        "CortexAIの対応するモデルの一部からリストを作成しています",
        options=MODEL_CANDIDATES,
        index=MODEL_CANDIDATES.index(st.session_state["rag"]["model"]) if st.session_state["rag"]["model"] in MODEL_CANDIDATES else 0
    )
    st.markdown('</div>', unsafe_allow_html=True)

# SYSTEM PROMPT（編集）
with st.expander("🛠️ SYSTEM PROMPT（編集可）", expanded=False):
    st.session_state["rag"]["system_prompt"] = st.text_area(
        "SYSTEM PROMPT",
        value=st.session_state["rag"]["system_prompt"],
        height=160
    )

# よくある質問（チップ）
with st.container():
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">💡 よくあるご質問（タップで挿入）</div>', unsafe_allow_html=True)
    cols = st.columns(2)
    for i, q in enumerate(EXAMPLE_QUESTIONS):
        with cols[i % 2]:
            st.markdown('<div class="chip-btn">', unsafe_allow_html=True)
            if st.button("❓ " + q, key=f"ex_q_{i}"):
                st.session_state["user_input"] = q
            st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# 質問入力
with st.container():
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📝 ご質問</div>', unsafe_allow_html=True)
    user_query = st.text_area(
        label="",
        value=st.session_state.get("user_input", ""),
        placeholder="例）返却手続きが完了しない場合の対処を教えてください",
        height=120
    )
    left, right = st.columns([1, 4])
    with left:
        submit = st.button("📨 送信する", type="primary", use_container_width=True)
    with right:
        st.markdown('<div class="small">Cortex Searchで規約の該当箇所をベクトル探索し、システムプロンプト、ご質問内容とともにLLMモデルへインプットします。</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# =========================================
# RAG 実行
# =========================================
if submit:
    if not user_query.strip():
        st.warning("ご質問を入力してください。")
        st.stop()

    with st.spinner("🔍 規約を検索し、回答を作成しています…"):
        try:
            session, service = get_session_and_service()
            # コンテキスト抽出
            context_text, raw_results = retrieve_context(service, user_query, limit=TOP_K)

            # 最終プロンプト作成
            final_prompt = build_final_prompt(
                st.session_state["rag"]["system_prompt"],
                user_query,
                context_text
            )

            # 生成
            answer = call_ai_complete(session, st.session_state["rag"]["model"], final_prompt)

            # 結果を状態に格納
            st.session_state["rag"]["ready"] = True
            st.session_state["rag"]["query"] = user_query
            st.session_state["rag"]["context"] = context_text
            st.session_state["rag"]["answer"] = answer
            st.session_state["rag"]["final_prompt"] = final_prompt
            st.session_state["rag"]["satisfaction"] = None

        except Exception as e:
            st.error(f"エラーが発生しました: {e}")
            st.stop()

# =========================================
# 結果表示（ダーク前提配色 + 折りたたみ）
# =========================================
if st.session_state["rag"]["ready"]:
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">💬 回答</div>', unsafe_allow_html=True)
        answer_disp = normalize_for_display(st.session_state["rag"]["answer"])
        answer_md = answer_disp.replace("\n", "  \n")
        # f-stringの式にバックスラッシュを含めないよう、別変数に整形
        st.markdown(f'<div class="answer-box">{answer_md}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # 参照コンテキスト（畳める）
    with st.expander("📚 参照した規約の抜粋（クリックで展開/収納）", expanded=False):
        ctx_disp = normalize_for_display(st.session_state["rag"]["context"])
        st.code(ctx_disp, language="markdown")

    # 最終プロンプト（畳める）
    with st.expander("🧪 最終プロンプト（デバッグ）", expanded=False):
        fp_disp = normalize_for_display(st.session_state["rag"].get("final_prompt", ""))
        st.code(fp_disp, language="markdown")

    # フィードバック
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🧷 フィードバック</div>', unsafe_allow_html=True)
        sat = st.radio(
            label="この回答に満足いただけましたか？",
            options=["はい 🙂", "いいえ 🙁"],
            horizontal=True,
            key="satisfaction_radio"
        )
        st.session_state["rag"]["satisfaction"] = sat
        if st.session_state["rag"]["satisfaction"] == "いいえ 🙁":
            st.warning("担当者へお繋ぎします。少々お待ちください。")
        else:
            st.info("ありがとうございます。他にも気になる点があれば、いつでもどうぞ。")
        st.markdown('</div>', unsafe_allow_html=True)

# フッター
st.markdown(
    '<div class="footer-note">© Citi Bike Support RAG • Dark-friendly • Powered by Snowflake Cortex</div>',
    unsafe_allow_html=True
)
