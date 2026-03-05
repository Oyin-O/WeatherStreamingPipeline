try:
    # ✅ Streamlit Cloud — reads from secrets
    import streamlit as st
    DB_URL = st.secrets["DB_URL"]
except Exception:
    # ✅ Local — reads from .env
    import os
    from dotenv import load_dotenv
    load_dotenv()
    DB_URL = os.getenv("DB_URL")
