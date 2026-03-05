import smtplib
import logging
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone
from dotenv import load_dotenv
import streamlit as st

load_dotenv()

logger = logging.getLogger(__name__)

try:
    DB_URL = st.secrets["DB_URL"]
    GMAIL_SENDER = st.secrets["GMAIL_SENDER"]
    GMAIL_PASSWORD = st.secrets["GMAIL_PASSWORD"]
    GMAIL_RECIPIENT = st.secrets["GMAIL_RECIPIENT"]
except Exception:
    load_dotenv()
    GMAIL_SENDER = os.getenv("GMAIL_SENDER")
    GMAIL_PASSWORD = os.getenv("GMAIL_PASSWORD")
    GMAIL_RECIPIENT = os.getenv("GMAIL_RECIPIENT")


def _send_email(subject: str, html_body: str):
    """Core email sending function."""
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = GMAIL_SENDER
        msg["To"] = GMAIL_RECIPIENT
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_SENDER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_SENDER, GMAIL_RECIPIENT, msg.as_string())

        logger.info(f"Email sent: {subject}")

    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def _base_template(title: str, color: str, content: str) -> str:
    """Shared HTML email template."""
    return f"""
    <html>
    <body style="margin:0;padding:0;background:#0a0e1a;font-family:'Helvetica Neue',sans-serif;">
        <div style="max-width:600px;margin:0 auto;padding:24px;">

            <!-- header -->
            <div style="
                background:linear-gradient(135deg,#111827,#1a2235);
                border:1px solid #1e2d45;
                border-left:4px solid {color};
                border-radius:12px;
                padding:24px;
                margin-bottom:20px;
            ">
                <div style="font-size:.7rem;letter-spacing:.15em;color:#64748b;text-transform:uppercase;">
                    WEATHER STREAM
                </div>
                <h1 style="margin:8px 0 0;font-size:1.4rem;color:#e2e8f0;">{title}</h1>
                <div style="font-size:.8rem;color:#64748b;margin-top:4px;">
                    {datetime.now(timezone.utc).strftime('%A, %d %B %Y·%H:%M:%S UTC')}
                </div>
            </div>

            <!-- content -->
            {content}

            <!-- footer -->
            <div style="text-align:center;padding:16px;font-size:.7rem;color:#64748b;">
                Weather Streaming Pipeline · Powered by Kafka + Spark + Supabase
            </div>
        </div>
    </body>
    </html>
    """


def send_weather_alert(city: str, country: str, alert_level: str, alert_message: str,
                       temperature_c: float, wind_speed_kmh: float):
    """Send email for severe or warning weather alerts."""
    color = "#ef4444" if alert_level == "severe" else "#f59e0b"
    icon = "🚨" if alert_level == "severe" else "⚠️"

    temp_str = f"{temperature_c:.1f}°C" if temperature_c is not None else "—"
    wind_str = f"{wind_speed_kmh:.1f} km/h" if wind_speed_kmh is not None else "—"

    content = f"""
    <div style="
        background:#111827;border:1px solid {color}44;
        border-left:4px solid {color};border-radius:10px;padding:20px;margin-bottom:16px;
    ">
        <div style="font-size:1.5rem;font-weight:700;color:#e2e8f0;margin-bottom:8px;">{icon} {city}, {country}</div>
        <div style="
            display:inline-block;padding:4px 12px;border-radius:20px;
            background:{color}22;border:1px solid {color}55;
            font-size:.75rem;color:{color};text-transform:uppercase;
            letter-spacing:.08em;margin-bottom:16px;
        ">{alert_level}</div>
        <p style="color:#e2e8f0;margin:0 0 16px;">{alert_message}</p>
        <table style="width:100%;border-collapse:collapse;">
            <tr>
                <td style="padding:8px;color:#64748b;font-size:.85rem;">🌡️ Temperature</td>
                <td style="padding:8px;color:#e2e8f0;font-size:.85rem;text-align:right;"><b>{temp_str}</b></td>
            </tr>
            <tr style="background:#1a2235;">
                <td style="padding:8px;color:#64748b;font-size:.85rem;">💨 Wind Speed</td>
                <td style="padding:8px;color:#e2e8f0;font-size:.85rem;text-align:right;"><b>{wind_str}</b></td>
            </tr>
        </table>
    </div>
    """

    subject = f"{icon} [{alert_level.upper()}] Weather Alert — {city}"
    _send_email(subject, _base_template(f"Weather Alert: {city}", color, content))


def send_daily_summary(records: list[dict]):
    """Send a daily summary email of all city conditions."""
    rows_html = ""
    for r in sorted(records, key=lambda x: x.get("city", "")):
        level = r.get("alert_level", "normal")
        color = {
            "normal": "#10b981", "advisory": "#00d4ff",
            "warning": "#f59e0b", "severe": "#ef4444", "extreme": "#ef4444"
        }.get(level, "#10b981")
        temp = r.get("temperature_c")
        wind = r.get("wind_speed_kmh")
        temp_str = f"{temp:.1f}°C" if temp is not None else "—"
        wind_str = f"{wind:.1f}" if wind is not None else "—"

        rows_html += f"""
        <tr>
            <td style="padding:10px;color:#e2e8f0;">{r.get('city', '—')}</td>
            <td style="padding:10px;color:#e2e8f0;">{r.get('country', '—')}</td>
            <td style="padding:10px;color:#00d4ff;font-family:monospace;">{temp_str}</td>
            <td style="padding:10px;color:#e2e8f0;font-family:monospace;">{wind_str} km/h</td>
            <td style="padding:10px;">
                <span style="
                    padding:3px 10px;border-radius:20px;
                    background:{color}22;border:1px solid {color}55;
                    font-size:.7rem;color:{color};text-transform:uppercase;
                ">{level}</span>
            </td>
        </tr>
        """

    content = f"""
    <div style="background:#111827;border:1px solid #1e2d45;border-radius:10px;overflow:hidden;">
        <table style="width:100%;border-collapse:collapse;">
            <thead>
                <tr style="background:#1a2235;">
                    <th style="padding:12px;color:#64748b;font-size:.75rem;text-align:left;text-transform:uppercase;">City</th>
                    <th style="padding:12px;color:#64748b;font-size:.75rem;text-align:left;text-transform:uppercase;">Country</th>
                    <th style="padding:12px;color:#64748b;font-size:.75rem;text-align:left;text-transform:uppercase;">Temp</th>
                    <th style="padding:12px;color:#64748b;font-size:.75rem;text-align:left;text-transform:uppercase;">Wind</th>
                    <th style="padding:12px;color:#64748b;font-size:.75rem;text-align:left;text-transform:uppercase;">Status</th>
                </tr>
            </thead>
            <tbody>{rows_html}</tbody>
        </table>
    </div>
    """

    subject = f"🌦 Daily Weather Summary — {datetime.now().strftime('%d %B %Y')}"
    _send_email(subject, _base_template("Daily Weather Summary", "#00d4ff", content))


def send_pipeline_error(error_message: str, batch_id: int = None):
    """Send email when the pipeline encounters an error."""
    batch_str = f"Batch {batch_id}" if batch_id is not None else "Unknown batch"

    content = f"""
    <div style="
        background:#111827;border:1px solid #ef444444;
        border-left:4px solid #ef4444;border-radius:10px;padding:20px;
    ">
        <div style="font-size:1.2rem;color:#ef4444;margin-bottom:12px;">❌ Pipeline Error</div>
        <div style="color:#64748b;font-size:.8rem;margin-bottom:8px;">{batch_str}</div>
        <pre style="
            background:#0a0e1a;border:1px solid #1e2d45;border-radius:8px;
            padding:16px;color:#e2e8f0;font-size:.8rem;
            overflow-x:auto;white-space:pre-wrap;
        ">{error_message}</pre>
        <p style="color:#64748b;font-size:.8rem;margin-top:12px;">
            Please check your Spark consumer logs for more details.
        </p>
    </div>
    """

    subject = "❌ Weather Pipeline Error"
    _send_email(subject, _base_template("Pipeline Error Detected", "#ef4444", content))
