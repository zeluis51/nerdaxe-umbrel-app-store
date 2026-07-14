import os
import time
import asyncio
import requests

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

NERDAXE_URL = os.getenv("NERDAXE_URL")
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "10"))

TAPO_ENABLED = os.getenv("TAPO_ENABLED", "false").lower() == "true"
TAPO_IP = os.getenv("TAPO_IP")
TAPO_EMAIL = os.getenv("TAPO_EMAIL")
TAPO_PASSWORD = os.getenv("TAPO_PASSWORD")
ENERGY_PRICE_BRL_KWH = float(os.getenv("ENERGY_PRICE_BRL_KWH", "1.00"))

client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)

write_api = client.write_api(write_options=SYNCHRONOUS)


def get_any(data, keys, default=None):
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return default


def safe_float(value, default=None):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default=None):
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def add_field(point, name, value):
    if value is not None:
        point = point.field(name, value)
    return point


def expected_hashrate_gh(frequency_mhz, asic_count=1):
    if frequency_mhz is None:
        return None
    return float(frequency_mhz) * 2.04 * int(asic_count or 1)


async def read_tapo_async():
    if not TAPO_ENABLED:
        return {}

    from tapo import ApiClient

    client_tapo = ApiClient(TAPO_EMAIL, TAPO_PASSWORD)
    device = await client_tapo.p110(TAPO_IP)

    info = await device.get_device_info()
    usage = await device.get_energy_usage()

    current_power_w = safe_float(getattr(usage, "current_power", None))
    if current_power_w is not None:
        current_power_w = current_power_w / 1000

    today_energy_kwh = safe_float(getattr(usage, "today_energy", None))
    if today_energy_kwh is not None:
        today_energy_kwh = today_energy_kwh / 1000

    month_energy_kwh = safe_float(getattr(usage, "month_energy", None))
    if month_energy_kwh is not None:
        month_energy_kwh = month_energy_kwh / 1000

    return {
        "plug_power_w": current_power_w,
        "plug_today_energy_kwh": today_energy_kwh,
        "plug_month_energy_kwh": month_energy_kwh,
        "plug_today_runtime_min": safe_float(getattr(usage, "today_runtime", None)),
        "plug_month_runtime_min": safe_float(getattr(usage, "month_runtime", None)),
        "plug_rssi": safe_float(getattr(info, "rssi", None)),
        "plug_signal_level": safe_float(getattr(info, "signal_level", None)),
        "plug_on_time_seconds": safe_float(getattr(info, "on_time", None)),
        "plug_device_on": bool(getattr(info, "device_on", False)),
    }


def read_tapo():
    if not TAPO_ENABLED:
        return {}

    try:
        return asyncio.run(read_tapo_async())
    except Exception as e:
        print(f"TAPO ERROR: {e}")
        return {}


def collect_once():
    r = requests.get(NERDAXE_URL, timeout=5)
    r.raise_for_status()
    data = r.json()

    tapo = read_tapo()

    pools = data.get("stratum", {}).get("pools", [])
    pool = pools[0] if pools else {}

    hostname = str(get_any(data, ["hostname", "hostName"], "nerdaxe"))
    model = str(get_any(data, ["deviceModel", "model"], "unknown"))
    asic_model = str(get_any(data, ["ASICModel", "asicModel", "asic"], "unknown"))
    firmware = str(get_any(data, ["version", "firmware", "fwVersion"], "unknown"))

    frequency_mhz = safe_float(get_any(data, ["frequency", "currentFrequency"]))
    asic_count = safe_int(get_any(data, ["asicCount", "ASICCount"]), 1)

    core_voltage_mv = safe_float(get_any(data, ["coreVoltage", "voltageASIC", "asicVoltage"]))
    core_voltage_actual_mv = safe_float(get_any(data, ["coreVoltageActual", "actualCoreVoltage"]))

    oc_profile = (
        f"{int(frequency_mhz)}_{int(core_voltage_mv)}"
        if frequency_mhz is not None and core_voltage_mv is not None
        else "unknown"
    )
    oc_session_id = oc_profile
    oc_frequency_mhz = frequency_mhz
    oc_voltage_mv = core_voltage_mv

    hashrate = safe_float(get_any(data, ["hashRate", "hashrate"]))
    hashrate_1m = safe_float(get_any(data, ["hashRate_1m", "hashrate_1m"]))
    hashrate_10m = safe_float(get_any(data, ["hashRate_10m", "hashrate_10m"]))
    hashrate_1h = safe_float(get_any(data, ["hashRate_1h", "hashrate_1h"]))
    hashrate_1d = safe_float(get_any(data, ["hashRate_1d", "hashrate_1d"]))

    base_hashrate = hashrate_10m or hashrate_1h or hashrate

    asic_temp = safe_float(get_any(data, ["temp", "asicTemp", "asic_temp"]))
    vr_temp = safe_float(get_any(data, ["vrTemp", "vr_temp", "voltageRegulatorTemp"]))
    vr_temp_int = safe_float(get_any(data, ["vrTempInt", "vr_temp_int", "voltageRegulatorTempInt"]))

    voltage_mv = safe_float(get_any(data, ["voltage", "inputVoltageMv"]))
    voltage_v_direct = safe_float(get_any(data, ["inputVoltage", "voltageV"]))
    voltage_v = voltage_v_direct if voltage_v_direct is not None else (
        voltage_mv / 1000 if voltage_mv is not None and voltage_mv > 100 else voltage_mv
    )

    current_a = safe_float(get_any(data, ["currentA", "inputCurrent", "current"]))
    current_ma = current_a * 1000 if current_a is not None and current_a < 100 else current_a
    if current_a is not None and current_a > 100:
        current_a = current_a / 1000

    power_w = safe_float(get_any(data, ["power", "powerW", "powerUsage"]))

    expected_hashrate = expected_hashrate_gh(frequency_mhz, asic_count)

    hashrate_percent = (
        (base_hashrate / expected_hashrate) * 100
        if base_hashrate is not None and expected_hashrate and expected_hashrate > 0
        else None
    )

    efficiency_fw_j_th = (
        power_w / (base_hashrate / 1000)
        if power_w is not None and base_hashrate is not None and base_hashrate > 0
        else None
    )

    plug_power_w = tapo.get("plug_power_w")

    efficiency_real_j_th = (
        plug_power_w / (base_hashrate / 1000)
        if plug_power_w is not None and base_hashrate is not None and base_hashrate > 0
        else None
    )

    power_loss_w = (
        plug_power_w - power_w
        if plug_power_w is not None and power_w is not None
        else None
    )

    power_loss_percent = (
        (power_loss_w / plug_power_w) * 100
        if power_loss_w is not None and plug_power_w and plug_power_w > 0
        else None
    )

    psu_efficiency_percent = (
        (power_w / plug_power_w) * 100
        if power_w is not None and plug_power_w and plug_power_w > 0
        else None
    )

    estimated_cost_day_brl = (
        (plug_power_w / 1000) * 24 * ENERGY_PRICE_BRL_KWH
        if plug_power_w is not None
        else None
    )

    estimated_cost_month_brl = (
        estimated_cost_day_brl * 30
        if estimated_cost_day_brl is not None
        else None
    )

    shares_accepted = safe_int(get_any(data, ["sharesAccepted", "acceptedShares", "shares"]), 0)
    shares_rejected = safe_int(get_any(data, ["sharesRejected", "rejectedShares"]), 0)
    shares_total = shares_accepted + shares_rejected

    reject_rate_percent = (
        (shares_rejected / shares_total) * 100
        if shares_total > 0
        else 0.0
    )

    pool_accepted = safe_int(get_any(pool, ["accepted", "sharesAccepted"]), 0)
    pool_rejected = safe_int(get_any(pool, ["rejected", "sharesRejected"]), 0)
    pool_total = pool_accepted + pool_rejected

    pool_reject_rate_percent = (
        (pool_rejected / pool_total) * 100
        if pool_total > 0
        else 0.0
    )

    point = (
        Point("nerdaxe")
        .tag("device", hostname)
        .tag("model", model)
        .tag("asic", asic_model)
        .tag("firmware", firmware)
        .tag("oc_profile", oc_profile)
        .tag("oc_session_id", oc_session_id)
    )

    fields = {
        "hashrate": hashrate,
        "hashrate_1m": hashrate_1m,
        "hashrate_10m": hashrate_10m,
        "hashrate_1h": hashrate_1h,
        "hashrate_1d": hashrate_1d,
        "expected_hashrate": expected_hashrate,
        "hashrate_percent": hashrate_percent,

        "asic_temp": asic_temp,
        "vr_temp": vr_temp,
        "vr_temp_int": vr_temp_int,
        "temp_delta_vr_asic": (
            vr_temp - asic_temp
            if vr_temp is not None and asic_temp is not None
            else None
        ),

        "voltage_mv": voltage_mv,
        "voltage_v": voltage_v,
        "current_ma": current_ma,
        "current_a": current_a,
        "power_w": power_w,
        "efficiency_fw_j_th": efficiency_fw_j_th,

        "plug_power_w": plug_power_w,
        "plug_today_energy_kwh": tapo.get("plug_today_energy_kwh"),
        "plug_month_energy_kwh": tapo.get("plug_month_energy_kwh"),
        "plug_today_runtime_min": tapo.get("plug_today_runtime_min"),
        "plug_month_runtime_min": tapo.get("plug_month_runtime_min"),
        "plug_rssi": tapo.get("plug_rssi"),
        "plug_signal_level": tapo.get("plug_signal_level"),
        "plug_on_time_seconds": tapo.get("plug_on_time_seconds"),
        "plug_device_on": tapo.get("plug_device_on"),

        "efficiency_real_j_th": efficiency_real_j_th,
        "power_loss_w": power_loss_w,
        "power_loss_percent": power_loss_percent,
        "psu_efficiency_percent": psu_efficiency_percent,
        "estimated_cost_day_brl": estimated_cost_day_brl,
        "estimated_cost_month_brl": estimated_cost_month_brl,

        "core_voltage_mv": core_voltage_mv,
        "core_voltage_actual_mv": core_voltage_actual_mv,
        "core_voltage_delta_mv": (
            core_voltage_mv - core_voltage_actual_mv
            if core_voltage_mv is not None and core_voltage_actual_mv is not None
            else None
        ),
        "frequency_mhz": frequency_mhz,
        "oc_frequency_mhz": oc_frequency_mhz,
        "oc_voltage_mv": oc_voltage_mv,

        "wifi_rssi": safe_float(get_any(data, ["wifiRSSI", "wifiRssi", "rssi"])),
        "ping_ms": safe_float(get_any(data, ["lastpingrtt", "lastPingRtt", "pingRtt"])),
        "ping_loss": safe_float(get_any(data, ["recentpingloss", "recentPingLoss", "pingLoss"])),

        "shares_accepted": shares_accepted,
        "shares_rejected": shares_rejected,
        "shares_total": shares_total,
        "reject_rate_percent": reject_rate_percent,

        "best_diff": safe_float(get_any(data, ["bestDiff", "bestDifficulty"])),
        "best_session_diff": safe_float(get_any(data, ["bestSessionDiff"])),
        "pool_difficulty": safe_float(get_any(data, ["poolDifficulty", "difficulty"])),
        "network_difficulty": safe_float(get_any(data, ["networkDifficulty"])),

        "pool_connected": bool(get_any(pool, ["connected"], False)),
        "pool_accepted": pool_accepted,
        "pool_rejected": pool_rejected,
        "pool_total": pool_total,
        "pool_reject_rate_percent": pool_reject_rate_percent,
        "pool_ping_ms": safe_float(get_any(pool, ["pingRtt", "ping", "lastPingRtt"]), 0),
        "pool_ping_loss": safe_float(get_any(pool, ["pingLoss"], 0)),
        "pool_best_diff": safe_float(get_any(pool, ["bestDiff", "bestDifficulty"], 0)),

        "uptime_seconds": safe_int(get_any(data, ["uptimeSeconds", "uptime"])),
        "free_heap": safe_int(get_any(data, ["freeHeap"])),
        "free_heap_internal": safe_int(get_any(data, ["freeHeapInt", "freeHeapInternal"])),
        "fanspeed": safe_float(get_any(data, ["fanspeed", "fanSpeed"])),
        "fanrpm": safe_float(get_any(data, ["fanrpm", "fanRpm", "fanRPM"])),
    }

    for key, value in fields.items():
        point = add_field(point, key, value)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

    print(
        f"OK | {hostname} | OC {oc_profile} | "
        f"{base_hashrate:.2f} GH/s | "
        f"{hashrate_percent:.1f}% expected | "
        f"ASIC {asic_temp}C | "
        f"VRM {vr_temp}C | "
        f"FW {power_w:.2f}W | "
        f"Plug {plug_power_w:.2f}W | "
        f"Real {efficiency_real_j_th:.2f} J/TH | "
        f"Loss {power_loss_w:.2f}W | "
        f"rej {reject_rate_percent:.2f}%"
    )


def main():
    print("NerdAxe collector V3 started")
    print(f"URL: {NERDAXE_URL}")
    print(f"Influx: {INFLUX_URL} / org={INFLUX_ORG} bucket={INFLUX_BUCKET}")
    print(f"Tapo enabled: {TAPO_ENABLED} / IP={TAPO_IP}")

    while True:
        try:
            collect_once()
        except Exception as e:
            print(f"ERROR: {e}")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
