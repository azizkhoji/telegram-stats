#!/usr/bin/env python3
"""
Testbor — Daily Telegram Report
Запуск: python3 daily_report.py
Диагностика стадий: python3 daily_report.py --check-stages
"""

import requests
import json
import sys
from datetime import datetime, timedelta, timezone

# ============================================================
# КОНФИГ — заполните свои данные
# ============================================================

TELEGRAM_TOKEN = "8684063397:AAGSwstBqwH1JaTWpGNzmaaeLFDx9BV202g"
TELEGRAM_CHATS = [
    734178941,       # Личный чат (Aziz)
    -5108650906,     # Группа "Abdulaziz & Kunlik Hisobot"
    -1003542065185,  # Новая группа
]

META_TOKEN      = "EAAgbtvLR8uoBQxZCHCxCGVwzwySGAGwhuiZCZCXMdeZBUb9MAdtuRpMhZChCPLEEJRZC2QsjTzYDYwIKy0GUnJMqyKZAdva2f1BKBBmeEpRSNGy2Xnrw3vhM6EdNSTa94z6AlyJ2UZBnOZBeiXVB3h0kpskOHdZAeZADtqnU3z1bBvUvpr31iUuxQZCqMQOoi2XLDQZDZD"
META_AD_ACCOUNT = "act_9988347027917215"

BITRIX_WEBHOOK  = "https://testbor.bitrix24.kz/rest/1/1d00g5asqv7uz9dw/"

USD_RATE = 12800  # UZS за 1 USD

# Стадии Bitrix24
# Запустите --check-stages чтобы проверить ID у вас
UNPROCESSED_STAGE = "C10:NEW"   # "Нерозобранные" — первая стадия
WON_STAGE         = "C10:WON"  # Успешная продажа

# Поля квалификации и канала
QUAL_FIELD    = "UF_CRM_1761032832539"
SIFATLI_VALUE = "448"
CHANNEL_FIELD = "UF_CRM_68F73263913D0"

# Ключевые слова в названии кампании → название воронки
# Добавляйте/меняйте по мере необходимости
FUNNEL_KEYWORDS = {
    "tilda":    "Tilda",
    "telegram": "Telegram",
    "sms":      "SMS",
    "traffic":  "Traffic",
    "vsl":      "VSL",
    "youtube":  "YouTube",
    "bio":      "Bio",
    "bloger":   "Bloger",
    "blogger":  "Bloger",
    "progrev":  "Progrev",
    "direct":   "Direct",
}

# ============================================================
# УТИЛИТЫ
# ============================================================

def get_yesterday():
    tz = timezone(timedelta(hours=5))  # UTC+5 Tashkent
    return (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")


def detect_channel(deal):
    ch = str(deal.get(CHANNEL_FIELD) or "").strip()
    if ch:
        return ch
    src = str(deal.get("UTM_SOURCE") or "").lower()
    med = str(deal.get("UTM_MEDIUM") or "").lower()
    trm = str(deal.get("UTM_TERM")   or "").lower()
    if "tilda"    in src:                          return "Tilda"
    if src in ("ig", "instagram") or "social" in med: return "Bio"
    if "telegram" in src or "tg" in src:           return "Telegram"
    if "bio"      in med:                          return "Bio"
    if "progrev"  in med or "warm" in med:         return "Progrev"
    if "direct"   in med or "direct" in trm:       return "Direct"
    if "bloger"   in trm or "blogger" in trm:      return "Bloger"
    return "Boshqa"


# ============================================================
# META ADS
# ============================================================

def get_meta_data(date):
    """Spend и leads по кампаниям за дату, сгруппированные по воронкам"""
    url = f"https://graph.facebook.com/v20.0/{META_AD_ACCOUNT}/insights"
    params = {
        "access_token": META_TOKEN,
        "level": "campaign",
        "fields": "campaign_name,spend,actions",
        "time_range": json.dumps({"since": date, "until": date}),
        "limit": 500,
    }
    r = requests.get(url, params=params, timeout=30)
    campaigns = r.json().get("data", [])

    funnels = {}
    total_spend = 0.0

    for c in campaigns:
        name  = c.get("campaign_name", "").lower()
        spend = float(c.get("spend", 0))
        leads = sum(
            int(a.get("value", 0))
            for a in c.get("actions", [])
            if a.get("action_type") == "lead"
        )
        total_spend += spend

        funnel = "Boshqa"
        for keyword, fname in FUNNEL_KEYWORDS.items():
            if keyword in name:
                funnel = fname
                break

        if funnel not in funnels:
            funnels[funnel] = {"spend": 0.0, "leads": 0}
        funnels[funnel]["spend"] += spend
        funnels[funnel]["leads"] += leads

    return total_spend, funnels


# ============================================================
# BITRIX24
# ============================================================

def bitrix_get_deals(filters, select_fields):
    """Универсальная пагинация по crm.deal.list"""
    all_deals = []
    start = 0
    while True:
        params = {"select[]": select_fields, "start": start}
        params.update(filters)
        r = requests.get(
            f"{BITRIX_WEBHOOK}crm.deal.list",
            params=params, timeout=30
        )
        result = r.json()
        batch = result.get("result", [])
        all_deals.extend(batch)
        if len(batch) < 50:
            break
        start += 50
    return all_deals


def get_new_deals(date):
    """Все сделки, созданные в указанный день"""
    date_from = f"{date}T00:00:00+05:00"
    date_to   = f"{date}T23:59:59+05:00"
    return bitrix_get_deals(
        filters={
            "filter[>=DATE_CREATE]": date_from,
            "filter[<=DATE_CREATE]": date_to,
        },
        select_fields=[
            "ID", "STAGE_ID", "OPPORTUNITY", "DATE_CREATE",
            "UTM_SOURCE", "UTM_MEDIUM", "UTM_TERM", "UTM_CONTENT",
            QUAL_FIELD, CHANNEL_FIELD,
        ]
    )


def get_won_deals(date):
    """Все WON сделки по дате закрытия (CLOSEDATE = дата, STAGE_ID = WON)"""
    return bitrix_get_deals(
        filters={
            "filter[STAGE_ID]": WON_STAGE,
            "filter[>=CLOSEDATE]": date,
            "filter[<=CLOSEDATE]": date,
        },
        select_fields=[
            "ID", "OPPORTUNITY", "CLOSEDATE", "DATE_CREATE",
            "UTM_SOURCE", "UTM_MEDIUM", "UTM_TERM",
            CHANNEL_FIELD,
        ]
    )


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(text):
    for chat_id in TELEGRAM_CHATS:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
        status = "OK" if r.status_code == 200 else f"ERROR {r.status_code}: {r.text}"
        print(f"  Telegram [{chat_id}]: {status}")


# ============================================================
# ГЛАВНАЯ ЛОГИКА
# ============================================================

def main():
    date = get_yesterday()
    print(f"Pulling data for {date}...")

    # --- META ---
    print("  Meta Ads...")
    total_spend_usd, meta_funnels = get_meta_data(date)
    total_spend_uzs = total_spend_usd * USD_RATE

    # --- BITRIX: новые лиды за вчера ---
    print("  Bitrix new deals...")
    new_deals = get_new_deals(date)
    total_new = len(new_deals)

    # Лиды по каналам (без Boshqa)
    channel_leads = {}
    for d in new_deals:
        ch = detect_channel(d)
        channel_leads[ch] = channel_leads.get(ch, 0) + 1

    boshqa_count = channel_leads.pop("Boshqa", 0)  # убираем Boshqa из показа и счётчика
    total_new_real = total_new - boshqa_count       # реальный итог без Boshqa

    # Обработанные / необработанные (только из реальных лидов, без Boshqa)
    real_deals    = [d for d in new_deals if detect_channel(d) != "Boshqa"]
    unprocessed   = [d for d in real_deals if d.get("STAGE_ID") == UNPROCESSED_STAGE]
    processed     = [d for d in real_deals if d.get("STAGE_ID") != UNPROCESSED_STAGE]
    unprocessed_count = len(unprocessed)
    processed_count   = len(processed)
    processing_rate   = (processed_count / total_new_real * 100) if total_new_real > 0 else 0

    # NonQual из обработанных (поле заполнено И не sifatli)
    nonqual_count = sum(
        1 for d in processed
        if str(d.get(QUAL_FIELD) or "").strip() not in ("", SIFATLI_VALUE)
    )

    # --- BITRIX: все WON за вчера ---
    print("  Bitrix won deals...")
    all_won = get_won_deals(date)
    total_sales_count = len(all_won)
    total_sales_sum   = sum(float(d.get("OPPORTUNITY") or 0) for d in all_won)

    # Продажи по воронкам
    won_by_channel = {}
    for d in all_won:
        ch = detect_channel(d)
        if ch == "Boshqa":
            ch = "Sarafan"
        if ch not in won_by_channel:
            won_by_channel[ch] = {"count": 0, "sum": 0.0}
        won_by_channel[ch]["count"] += 1
        won_by_channel[ch]["sum"]   += float(d.get("OPPORTUNITY") or 0)

    # WON из новых реальных лидов вчера
    new_won       = [d for d in real_deals if d.get("STAGE_ID") == WON_STAGE]
    new_won_count = len(new_won)
    new_won_sum   = sum(float(d.get("OPPORTUNITY") or 0) for d in new_won)

    # Конверсия продаж (все WON / реальные новые лиды)
    sales_conv = (total_sales_count / total_new_real * 100) if total_new_real > 0 else 0

    # ============================================================
    # ФОРМАТ СООБЩЕНИЯ
    # ============================================================

    # Продажи по воронкам — фиксированный порядок
    FUNNEL_ORDER = ["Tilda", "Telegram", "Bio", "Bloger", "YouTube", "Direct", "Progrev", "Sarafan"]
    won_channel_lines = ""
    for ch in FUNNEL_ORDER:
        if ch in won_by_channel:
            v = won_by_channel[ch]
            won_channel_lines += f"  • {ch}: {v['count']} ta — {v['sum']:,.0f} so'm\n"
    # Остальные каналы (если вдруг есть неизвестные)
    for ch, v in won_by_channel.items():
        if ch not in FUNNEL_ORDER:
            won_channel_lines += f"  • {ch}: {v['count']} ta — {v['sum']:,.0f} so'm\n"

    # Воронки — Meta spend (без Boshqa, с десятичными)
    meta_lines = ""
    for funnel, vals in sorted(meta_funnels.items(), key=lambda x: -x[1]["spend"]):
        if vals["spend"] > 0 and funnel != "Boshqa":
            uzs_m = vals["spend"] * USD_RATE / 1_000_000
            meta_lines += f"  • {funnel}: ${vals['spend']:.2f} ({uzs_m:.1f}M)\n"

    # Воронки — Bitrix лиды (Boshqa уже убран из channel_leads)
    bitrix_lines = ""
    for ch, cnt in sorted(channel_leads.items(), key=lambda x: -x[1]):
        bitrix_lines += f"  • {ch}: {cnt}\n"

    msg = (
        f"📊 <b>Testbor — Kunlik hisobot</b>\n"
        f"📅 <b>{date}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"📥 <b>YANGI LIDLAR</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Jami: <b>{total_new_real}</b>\n"
        f"\n"
        f"Voronkalar bo'yicha:\n"
        f"{bitrix_lines.rstrip()}\n"
        f"\n"
        f"✅ Ishlangan: <b>{processed_count}</b>\n"
        f"❌ Ishlanmagan (nerazobranniye): <b>{unprocessed_count}</b>\n"
        f"📈 Ishlash konversiyasi: <b>{processing_rate:.1f}%</b>\n"
        f"🚫 NonQual (ishlanganlardan): <b>{nonqual_count}</b>\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>REKLAMA XARAJATI</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Jami: <b>${total_spend_usd:.2f}</b> ({total_spend_uzs/1_000_000:.1f}M UZS)\n"
        f"\n"
        f"{meta_lines.rstrip()}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🏆 <b>SOTUVLAR</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Jami sotuvlar (kun): <b>{total_sales_count}</b>\n"
        f"Jami summa: <b>{total_sales_sum:,.0f} UZS</b>\n"
        f"\n"
        f"Yangi lidlardan sotuvlar: <b>{new_won_count}</b>\n"
        f"Yangi lidlardan summa: <b>{new_won_sum:,.0f} UZS</b>\n"
        f"\n"
        f"Sotuv konversiyasi: <b>{sales_conv:.1f}%</b>\n"
        f"\n"
        f"Voronkalar bo'yicha sotuvlar:\n"
        f"{won_channel_lines.rstrip()}"
    )

    print("\n--- MESSAGE PREVIEW ---")
    print(msg)
    print("-----------------------\n")

    send_telegram(msg)
    print("Done.")


# ============================================================
# ДИАГНОСТИКА СТАДИЙ
# ============================================================

def check_stages():
    """Вывести все стадии воронки — чтобы найти ID 'Нерозобранные'"""
    print("Checking pipeline stages...")
    r = requests.get(
        f"{BITRIX_WEBHOOK}crm.dealcategory.stages",
        params={"id": 10}, timeout=30
    )
    stages = r.json().get("result", [])
    if not stages:
        # Попробовать через status list
        r2 = requests.get(
            f"{BITRIX_WEBHOOK}crm.status.list",
            params={"filter[ENTITY_ID]": "DEAL_STAGE_10"}, timeout=30
        )
        stages = r2.json().get("result", [])
    print(f"{'STATUS_ID':<20} {'NAME'}")
    print("-" * 50)
    for s in stages:
        print(f"{s.get('STATUS_ID',''):<20} {s.get('NAME','')}")


# ============================================================

if __name__ == "__main__":
    if "--check-stages" in sys.argv:
        check_stages()
    else:
        main()
